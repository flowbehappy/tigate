package dynstream

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flowbehappy/tigate/utils/deque"
	"github.com/flowbehappy/tigate/utils/ringbuffer"
)

const maxInflight = 16

type pathStat struct {
	path      Path
	totalTime time.Duration
	waitLen   int64
}

type streamStat struct {
	id uint64

	// Calculate the time for the past n events.
	handleTimeOnNum  time.Duration
	handleCountonNum int64

	// Calculate the time for the past report interval.
	handleTimeInPeriod  time.Duration
	handleCountInPeriod int64

	queueLen int64 // The length of the wait queue.

	futureWaitOnNum    time.Duration // The longgest estimated wait time of all waiting events.
	futureWaitInPeriod time.Duration // The longgest estimated wait time of all waiting events.

	historyWait time.Duration // The max wait time of the latest handled events.

	pathStats []pathStat // Sorted by the total time in descending order.
}

type waitRunning[T Event, D any] struct {
	path     Path
	waitChan <-chan *EventWrap[T, D]
}

type stream[T Event, D any] struct {
	id    uint64
	zeroT T

	expectedLatency time.Duration
	reportInterval  time.Duration

	worker Handler[T, D]

	// The statistics of the paths.
	// Keep in mind that the pathInfo instance are shared in the pathMap here, the pathMap in DynamicStream,
	// and the pathInfo in the EventWrap. They are all reference to the same instances.
	// It is designed to avoid the frequent mapping by path in different places.
	pathMap map[Path]*pathInfo[T, D]

	incame uint64 // How many events are put into the stream.

	inChan     chan *EventWrap[T, D]          // The buffer channel to receive the events.
	waitQueue  *deque.Deque[*EventWrap[T, D]] // The queue to store the waiting events.
	handleChan chan *EventWrap[T, D]          // The buffer channel to send the events to the worker.
	doneChan   chan *EventWrap[T, D]          // A buffer channel to receive the done events.

	inflight      *ringbuffer.RingBuffer[*EventWrap[T, D]] // The batches that are in the handleChan but not likely to be handled.
	latestHandled *ringbuffer.RingBuffer[*EventWrap[T, D]] // The latest handled batches.

	handleTime  time.Duration // The total time to handle the events in the report period.
	handleCount int64         // The number of handled events in the report period.
	reportChan  chan *streamStat

	hasClosed atomic.Bool

	waitRunning *waitRunning[T, D] // The wait object from the former stream.

	handleDone     sync.WaitGroup
	backgroundDone sync.WaitGroup
}

func newStream[T Event, D any](
	id uint64,
	expectedLatency time.Duration,
	reportInterval time.Duration, // 200 milliseconds?
	acceptedPaths []*pathInfo[T, D],
	worker Handler[T, D],
	reportChan chan *streamStat,
) *stream[T, D] {
	s := &stream[T, D]{
		id:              id,
		expectedLatency: expectedLatency,
		reportInterval:  reportInterval,
		worker:          worker,
		pathMap:         make(map[Path]*pathInfo[T, D], len(acceptedPaths)),
		inChan:          make(chan *EventWrap[T, D], 64),
		waitQueue:       deque.NewDequeDefault[*EventWrap[T, D]](),
		handleChan:      make(chan *EventWrap[T, D], maxInflight),
		doneChan:        make(chan *EventWrap[T, D], maxInflight),
		inflight:        ringbuffer.NewRingBuffer[*EventWrap[T, D]](maxInflight + 1),
		latestHandled:   ringbuffer.NewRingBuffer[*EventWrap[T, D]](16),
		reportChan:      reportChan,
	}

	for _, p := range acceptedPaths {
		s.pathMap[p.path] = p
	}

	return s
}

func (s *stream[T, D]) start(formerStreams ...*stream[T, D]) {

	// Start worker to handle events.
	s.handleDone.Add(1)
	go s.handleEventLoop()

	// Start manaing events and statistics.
	s.backgroundDone.Add(1)
	go s.backgroundLoop(formerStreams)
}

func (s *stream[T, D]) in() chan *EventWrap[T, D] {
	return s.inChan
}

// Close the stream and return the running event.
// Not all of the new streams need to wait for the former stream's handle goroutine to finish.
// Only the streams that are interested in the path of the running event need to wait.
func (s *stream[T, D]) close(interested ...map[Path]*pathInfo[T, D]) *waitRunning[T, D] {
	if s.hasClosed.CompareAndSwap(false, true) {
		close(s.inChan)
	}
	s.backgroundDone.Wait()

	if len(interested) != 0 {
		if s.waitRunning != nil {
			if _, ok := interested[0][s.waitRunning.path]; ok {
				return s.waitRunning
			}
			return nil
		}

		if e, ok := s.inflight.Front(); ok {
			if _, ok := interested[0][e.Path()]; ok {
				waitChan := make(chan *EventWrap[T, D], 1)
				go func() {
					s.handleDone.Wait()
					waitChan <- e
					close(waitChan)
				}()
				return &waitRunning[T, D]{path: e.Path(), waitChan: waitChan}
			}
		}
	}
	return nil
}

func (s *stream[T, D]) handleEventLoop() {
	defer func() {
		close(s.doneChan)
		s.handleDone.Done()
	}()

	for {
		e, ok := <-s.handleChan
		if !ok {
			// The stream is closing.
			return
		}

		e.startTime.Store(time.Now())
		s.worker.Handle(e)
		e.doneTime.Store(time.Now())

		s.doneChan <- e
	}
}

func (s *stream[T, D]) backgroundLoop(formerStreams []*stream[T, D]) {
	defer func() {
		close(s.handleChan)

		s.recordAndDrainDoneChan(nil)

		// Move all events in the inChan & handleChan to the waitQueue.
		now := time.Now()
		for e := range s.inChan {
			e.inQueueTime = now
			s.waitQueue.PushBack(e)
		}

		if ic := s.inflight.Length(); ic > 1 {
			for {
				e, _ := s.inflight.PopBack()
				s.waitQueue.PushFront(e)
				ic -= 1
				if ic == 1 {
					break
				}
			}
		}

		if s.inflight.Length() != 0 {
			// There is a running event.
		}

		s.backgroundDone.Done()
	}()

	// Move the remaining events in the former streams to this stream.

	for _, stream := range formerStreams {
		s.waitRunning = stream.close(s.pathMap)

		itr := stream.waitQueue.ForwardIterator()
		for e, ok := itr.Next(); ok; e, ok = itr.Next() {
			if _, ok := s.pathMap[e.Path()]; ok {
				s.waitQueue.PushBack(e)
			}
		}

	}

	pushToWaitQueue := func(e *EventWrap[T, D]) {
		e.inQueueTime = time.Now()
		e.pathInfo.waitLen++

		s.waitQueue.PushBack(e)
		s.incame++
	}

	nextReport := time.Now().Add(s.reportInterval)

	if s.waitRunning != nil {
		// The waitStream is not nil. We need to wait for the running event from the former stream.
		// And in the same time, we need to listen to the new events and report the statistics.
	Loop:
		for {
			select {
			case <-time.After(nextReport.Sub(time.Now())):
				s.reportStat()
				nextReport = time.Now().Add(s.reportInterval)
			case e, ok := <-s.inChan:
				if !ok {
					return
				}
				pushToWaitQueue(e)
			case e := <-s.waitRunning.waitChan:
				if e != nil && e.doneTime.Load() == nil {
					s.waitQueue.PushFront(e)
				}

				s.waitRunning = nil
				break Loop
			}
		}
	}

	for {
		nextEvent, ok := s.waitQueue.Front()

		if ok {
			select {
			case <-time.After(nextReport.Sub(time.Now())):
				s.reportStat()
				nextReport = time.Now().Add(s.reportInterval)
			case e, ok := <-s.inChan: // Listen to the new events and put them into the wait queue.
				if !ok {
					return
				}
				pushToWaitQueue(e)
			case done := <-s.doneChan: // Receive the done batch and update the statistics.
				s.recordAndDrainDoneChan(done)
			case s.handleChan <- nextEvent: // Send the batch to the worker.
				s.waitQueue.PopFront()
				s.inflight.PushBack(nextEvent)
			}
		} else {
			select {
			case <-time.After(nextReport.Sub(time.Now())):
				s.reportStat()
				nextReport = time.Now().Add(s.reportInterval)
			case e, ok := <-s.inChan:
				if !ok {
					return
				}
				pushToWaitQueue(e)
			case done := <-s.doneChan:
				s.recordAndDrainDoneChan(done)
			}
		}
	}
}

func (s *stream[T, D]) recordAndDrainDoneChan(first *EventWrap[T, D]) {
	afterDone := func(e *EventWrap[T, D]) {
		start := e.startTime.Load().(time.Time)
		done := e.doneTime.Load().(time.Time)
		handleTime := done.Sub(start)

		// The batch is done, remove it from the inflight.
		if head, ok := s.inflight.PopFront(); !ok || head != e {
			panic("The batch is not the head of the inflight.")
		}

		// Store the timing statistics.
		s.handleTime += handleTime
		s.handleCount++

		e.pathInfo.totalTime += handleTime
		e.pathInfo.waitLen-- // It is not correct. But we will fix it when report the statistics.

		// Release memory
		e.event = s.zeroT
		e.pathInfo = nil

		s.latestHandled.PushBack(e)
	}

	if first != nil {
		afterDone(first)
	}
	for done := range s.doneChan {
		afterDone(done)
	}
}

func (s *stream[T, D]) runningEvent() (*EventWrap[T, D], time.Duration, time.Duration, bool) {
	if head, ok := s.inflight.Front(); ok {
		start := head.startTime.Load()
		done := head.doneTime.Load()
		if start != nil && done != nil {
			startTime := start.(time.Time)
			doneTime := done.(time.Time)
			return head, doneTime.Sub(startTime), startTime.Sub(head.inQueueTime), true
		} else if start != nil {
			startTime := start.(time.Time)
			return head, time.Now().Sub(startTime), startTime.Sub(head.inQueueTime), true
		} else {
			return head, time.Duration(0), time.Now().Sub(head.inQueueTime), true
		}
	}
	return nil, time.Duration(0), time.Duration(0), false
}

func (s *stream[T, D]) nextEvent() (*EventWrap[T, D], bool) {
	if s.inflight.Length() < 2 {
		return nil, false
	}
	// The second event in the inflight is the next event.
	itr := s.inflight.ForwardIterator()
	itr.Next()
	e, _ := itr.Next()
	return e, true
}

func (s *stream[T, D]) lastEvent() (*EventWrap[T, D], bool) {
	if e, ok := s.waitQueue.Back(); ok {
		return e, true
	}
	return nil, false
}

func (s *stream[T, D]) reportStat() {
	// Let's drain the doneChan to make the report as accurate as possible.
	s.recordAndDrainDoneChan(nil)

	now := time.Now()
	var handleTimeOnNum time.Duration
	var handleCountOnNum int64
	handleTimeInPeriod := s.handleTime
	handleCountInPeriod := s.handleCount

	var historyWait time.Duration

	// Count the running event.
	if _, rt, wt, running := s.runningEvent(); running {
		handleTimeOnNum += rt
		handleCountOnNum++

		handleTimeInPeriod += rt
		handleCountInPeriod++

		historyWait = max(historyWait, wt)
	}

	// Count the finished events.
	itr := s.latestHandled.ForwardIterator()
	for e, ok := itr.Next(); ok; e, ok = itr.Next() {
		start := e.startTime.Load().(time.Time)
		done := e.doneTime.Load().(time.Time)
		handleTimeOnNum += done.Sub(start)
		handleCountOnNum++

		historyWait = max(historyWait, start.Sub(e.inQueueTime))
	}

	if e, ok := s.nextEvent(); ok {
		historyWait = max(historyWait, now.Sub(e.inQueueTime))
	}

	queueLen := int64(s.waitQueue.Length())
	if il := s.inflight.Length(); il > 0 {
		// The head of the inflight is the running event. Don't count it.
		queueLen += int64(il - 1)
	}

	avgOnNum := time.Duration(0)
	if handleCountOnNum != 0 {
		avgOnNum = handleTimeOnNum / time.Duration(handleCountOnNum)
	}
	avgInPeriod := time.Duration(0)
	if handleCountInPeriod != 0 {
		avgInPeriod = handleTimeInPeriod / time.Duration(handleCountInPeriod)
	}

	futureWaitOnNum := avgOnNum
	futureWaitInPeriod := avgInPeriod
	if e, ok := s.lastEvent(); ok {
		futureWaitOnNum = max(futureWaitOnNum, now.Sub(e.inQueueTime)+avgOnNum*time.Duration(queueLen))
		futureWaitInPeriod = max(futureWaitInPeriod, now.Sub(e.inQueueTime)+avgInPeriod*time.Duration(queueLen))
	}

	stat := &streamStat{
		handleTimeOnNum:  handleTimeOnNum,
		handleCountonNum: handleCountOnNum,

		handleTimeInPeriod:  handleTimeInPeriod,
		handleCountInPeriod: handleCountInPeriod,

		queueLen:           queueLen,
		futureWaitOnNum:    futureWaitOnNum,
		futureWaitInPeriod: futureWaitInPeriod,
		historyWait:        historyWait,
	}
	if min(futureWaitOnNum, futureWaitInPeriod, historyWait) >= s.expectedLatency*2 {
		// The stream is not in a good state.
		// We need to report the statistics of each path to the scheduler.
		stat.pathStats = s.resetPathStat()
	}
	select {
	case s.reportChan <- stat:
	default:
	}

	// Reset the statistics.
	s.handleTime = 0
	s.handleCount = 0
}

func (s *stream[T, D]) resetPathStat() []pathStat {
	pathStats := make([]pathStat, len(s.pathMap))
	// We need to add the time of the running event to the total time.
	// To make the scheduler to notice the long running events.
	re, rt, _, running := s.runningEvent()
	for _, stat := range s.pathMap {
		ps := pathStat{
			path:      stat.path,
			totalTime: stat.totalTime,
			waitLen:   stat.waitLen,
		}
		if running && re.Path() == stat.path {
			ps.totalTime += rt
			// One event of this path is running. But we only decrease the waitLen after the event is done.
			// To make the report more accurate, we decrease the waitLen here.
			ps.waitLen--
		}
		pathStats = append(pathStats)

		// Reset the statistics.
		stat.totalTime = 0
	}
	// Sort the path stats by the total time in descending order.
	sort.Slice(pathStats, func(i, j int) bool {
		return pathStats[i].totalTime > pathStats[j].totalTime
	})
	return pathStats
}
