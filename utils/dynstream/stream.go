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

type eventWrap[T Event, D any] struct {
	event T

	pathInfo *pathInfo[T, D]

	inQueueTime time.Time
	// Those two fields are accessed by the worker and background goroutines in the stream.
	// So they need synchronization.
	startTime atomic.Value
	doneTime  atomic.Value
}

func (ew *eventWrap[T, D]) path() Path { return ew.pathInfo.path }
func (ew *eventWrap[T, D]) dest() D    { return ew.pathInfo.dest }

type streamStat struct {
	id int64

	// Based on the past n events.
	handleTimeOnNum  time.Duration
	handleCountOnNum int64

	// Based on the current report interval.
	handleTimeInPeriod  time.Duration
	handleCountInPeriod int64

	queueLen int64 // The length of the wait queue.

	futureWaitOnNum    time.Duration // The longgest estimated wait time of all waiting events.
	futureWaitInPeriod time.Duration // The longgest estimated wait time of all waiting events.

	historyWait time.Duration // The max wait time of the latest handled events.

	pathStats []pathStat // Sorted by the total time in descending order.
}

type runningTask[T Event, D any] struct {
	event      *eventWrap[T, D]
	startTrack time.Time
	waitChan   <-chan *eventWrap[T, D]
}

type stream[T Event, D any] struct {
	id    int64
	zeroT T

	expectedLatency time.Duration
	reportInterval  time.Duration

	handler Handler[T, D]

	// The statistics of the paths.
	// Keep in mind that the pathInfo instance are shared in the pathMap here, the pathMap in DynamicStream,
	// and the pathInfo in the eventWrap. They are all reference to the same instances.
	// It is designed to avoid the frequent mapping by path in different places.
	pathMap map[Path]*pathInfo[T, D]

	incame uint64 // How many events are put into the stream.

	inChan     chan *eventWrap[T, D]          // The buffer channel to receive the events.
	waitQueue  *deque.Deque[*eventWrap[T, D]] // The queue to store the waiting events.
	handleChan chan *eventWrap[T, D]          // The buffer channel to send the events to the worker.
	doneChan   chan *eventWrap[T, D]          // A buffer channel to receive the done events.

	inflight      *ringbuffer.RingBuffer[*eventWrap[T, D]] // The batches that are in the handleChan but not likely to be handled.
	latestHandled *ringbuffer.RingBuffer[*eventWrap[T, D]] // The latest handled batches.

	handleTime  time.Duration // The total time to handle the events in the report period.
	handleCount int64         // The number of handled events in the report period.
	reportChan  chan *streamStat

	hasClosed atomic.Bool

	runningTasks []*runningTask[T, D] // The running tasks from the former stream.

	prepareDone    sync.WaitGroup // For testing.
	handleDone     sync.WaitGroup
	backgroundDone sync.WaitGroup
}

func newStream[T Event, D any](
	id int64,
	expectedLatency time.Duration,
	reportInterval time.Duration, // 200 milliseconds?
	handler Handler[T, D],
	reportChan chan *streamStat,
) *stream[T, D] {
	s := &stream[T, D]{
		id:              id,
		expectedLatency: expectedLatency,
		reportInterval:  reportInterval,
		handler:         handler,
		inChan:          make(chan *eventWrap[T, D], 64),
		waitQueue:       deque.NewDequeDefault[*eventWrap[T, D]](),
		handleChan:      make(chan *eventWrap[T, D], maxInflight),
		doneChan:        make(chan *eventWrap[T, D], maxInflight),
		// The cap of inflight should be at least cap of handleChan + doneChan + 1 (the running event).
		inflight:      ringbuffer.NewRingBuffer[*eventWrap[T, D]](maxInflight*2 + 1),
		latestHandled: ringbuffer.NewRingBuffer[*eventWrap[T, D]](16),
		reportChan:    reportChan,
	}

	s.prepareDone.Add(1)

	return s
}

func (s *stream[T, D]) getId() int64 { return s.id }

func (s *stream[T, D]) start(acceptedPaths []*pathInfo[T, D], formerStreams ...*stream[T, D]) {
	if s.hasClosed.Load() {
		panic("The stream has been closed.")
	}

	// Start worker to handle events.
	s.handleDone.Add(1)
	go s.handleEventLoop()

	// Start manaing events and statistics.
	s.backgroundDone.Add(1)
	go s.backgroundLoop(acceptedPaths, formerStreams)
}

func (s *stream[T, D]) in() chan *eventWrap[T, D] {
	return s.inChan
}

// Close the stream and return the running event.
// Not all of the new streams need to wait for the former stream's handle goroutine to finish.
// Only the streams that are interested in the path of the running event need to wait.
func (s *stream[T, D]) close(interested ...map[Path]*pathInfo[T, D]) []*runningTask[T, D] {
	if s.hasClosed.CompareAndSwap(false, true) {
		close(s.inChan)
	}
	s.backgroundDone.Wait()

	if len(interested) != 0 {
		now := time.Now()
		if len(s.runningTasks) != 0 {
			// By now this stream is still waiting for the running events from the former stream.
			rts := make([]*runningTask[T, D], 0)
			for _, task := range s.runningTasks {
				if _, ok := interested[0][task.event.path()]; ok {
					t := &runningTask[T, D]{event: task.event, startTrack: task.startTrack, waitChan: task.waitChan}
					rts = append(rts, t)
				}
			}
			return rts
		}

		if e, ok := s.inflight.Front(); ok {
			// This stream is handling events before exit.
			if _, ok := interested[0][e.path()]; ok {
				waitChan := make(chan *eventWrap[T, D], 1)
				go func(s *stream[T, D], e *eventWrap[T, D]) {
					s.handleDone.Wait()
					waitChan <- e
					close(waitChan)
				}(s, e)
				return []*runningTask[T, D]{{event: e, startTrack: now, waitChan: waitChan}}
			}
		}
	}
	return []*runningTask[T, D]{}
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
		s.handler.Handle(e.event, e.dest())
		e.doneTime.Store(time.Now())

		s.doneChan <- e
	}
}

func (s *stream[T, D]) backgroundLoop(acceptedPaths []*pathInfo[T, D], formerStreams []*stream[T, D]) {
	defer func() {
		close(s.handleChan)

		s.recordAndDrainDoneChan(nil)

		// Gether all remaing events to the waitQueue,
		// to make it easier to later streams to take over the work.
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

		s.backgroundDone.Done()
	}()

	// We initialize the pathMap here to avoid blocking the main goroutine.
	// As there could be many paths, and the initialization could be time-consuming.
	s.pathMap = make(map[Path]*pathInfo[T, D], len(acceptedPaths))
	for _, p := range acceptedPaths {
		s.pathMap[p.path] = p
	}

	waitQueueLock := sync.Mutex{}
	pushToWaitQueue := func(e *eventWrap[T, D], useLock bool) {
		e.inQueueTime = time.Now()
		e.pathInfo.waitLen++

		if useLock {
			waitQueueLock.Lock()
			defer waitQueueLock.Unlock()

			s.waitQueue.PushBack(e)
			s.incame++
		} else {
			s.waitQueue.PushBack(e)
			s.incame++
		}
	}

	nextReport := time.NewTimer(0) // Make the first report as soon as possible.

	if len(formerStreams) != 0 {
		// Start a new goroutine (eary fetch) to handle the events in the inChan during
		// the background goroutine waiting for the running events.
		// To avoid blocking the inChan.
		earlyFetchStop := make(chan struct{})
		waitRunningStop := make(chan struct{})
		earlyFetchDone := sync.WaitGroup{}
		earlyFetchDone.Add(1)
		go func() {
			defer earlyFetchDone.Done()
			for {
				select {
				case <-nextReport.C:
					s.reportStat()
					nextReport.Reset(s.reportInterval)
				case e, ok := <-s.inChan:
					if e != nil {
						pushToWaitQueue(e, true)
					}
					if !ok {
						// Because the inChan is occupied by the early fetching goroutine,
						// we need another signal to stop the waiting for the running events.
						close(waitRunningStop)
						return
					}
				case <-earlyFetchStop:
					return
				}
			}
		}()

		// Move the remaining events in the former streams to this stream.
		for _, stream := range formerStreams {
			s.runningTasks = append(s.runningTasks, stream.close(s.pathMap)...)

			// We push to the front of waitQueue here. Because the eary events are being pushed to the back.
			// We don't want to mix them.
			itr := stream.waitQueue.BackwardIterator()
			for e, ok := itr.Next(); ok; e, ok = itr.Next() {
				if _, ok := s.pathMap[e.path()]; ok {
					// Don't call pushToWaitQueue. Because we don't want to update the inQueueTime of the events (They are set by former streams already).
					waitQueueLock.Lock()
					defer waitQueueLock.Unlock()

					s.waitQueue.PushBack(e)
					s.incame++
				}
			}
		}

		s.prepareDone.Done()

		// The waitStream is not nil. We need to wait for the running events from the former streams.
		// And in the same time, we need to listen to the new events and report the statistics.
	Loop:
		for len(s.runningTasks) != 0 {
			select {
			case e := <-s.runningTasks[len(s.runningTasks)-1].waitChan:
				if e != nil && e.doneTime.Load() == nil {
					s.waitQueue.PushFront(e)
				}
				s.runningTasks = s.runningTasks[:len(s.runningTasks)-1]
				break Loop
			case <-waitRunningStop:
				break Loop
			}
		}

		// Make the eary fetching goroutine to stop.
		// This goroutine (background goroutine) will take over the work.
		close(earlyFetchStop)
		earlyFetchDone.Wait()
	}

	for {
		nextEvent, ok := s.waitQueue.Front()

		if ok {
			select {
			case <-nextReport.C:
				s.reportStat()
				nextReport.Reset(s.reportInterval)
			case e, ok := <-s.inChan: // Listen to the new events and put them into the wait queue.
				if e != nil {
					pushToWaitQueue(e, false)
				}
				if !ok {
					return
				}
			case done := <-s.doneChan: // Receive the done batch and update the statistics.
				s.recordAndDrainDoneChan(done)
			case s.handleChan <- nextEvent: // Send the batch to the worker.
				s.waitQueue.PopFront()
				s.inflight.PushBack(nextEvent)
			}
		} else {
			select {
			case <-nextReport.C:
				s.reportStat()
				nextReport.Reset(s.reportInterval)
			case e, ok := <-s.inChan:
				if e != nil {
					pushToWaitQueue(e, false)
				}
				if !ok {
					return
				}
			case done := <-s.doneChan:
				s.recordAndDrainDoneChan(done)
			}
		}
	}
}

func (s *stream[T, D]) recordAndDrainDoneChan(first *eventWrap[T, D]) {
	afterDone := func(e *eventWrap[T, D]) {
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
	for {
		select {
		case done := <-s.doneChan:
			if done == nil {
				return
			}
			afterDone(done)
		default:
			return
		}
	}
}

// Return (event, already running time, actual waiting time, hasRunningEvent)
func (s *stream[T, D]) runningEvent() (*eventWrap[T, D], time.Duration, time.Duration, bool) {
	if len(s.runningTasks) != 0 {
		longestRunning := s.runningTasks[0]
		for _, task := range s.runningTasks {
			if task.startTrack.Before(longestRunning.startTrack) {
				longestRunning = task
			}
		}
		return longestRunning.event, time.Since(longestRunning.startTrack), time.Duration(0), true
	}
	if head, ok := s.inflight.Front(); ok {
		start := head.startTime.Load()
		done := head.doneTime.Load()
		if start != nil && done != nil {
			startTime := start.(time.Time)
			doneTime := done.(time.Time)
			return head, doneTime.Sub(startTime), startTime.Sub(head.inQueueTime), true
		} else if start != nil {
			startTime := start.(time.Time)
			return head, time.Since(startTime), startTime.Sub(head.inQueueTime), true
		} else {
			return head, time.Duration(0), time.Since(head.inQueueTime), true
		}
	}
	return nil, time.Duration(0), time.Duration(0), false
}

func (s *stream[T, D]) nextEvent() (*eventWrap[T, D], bool) {
	if s.inflight.Length() < 2 {
		return nil, false
	}
	// The second event in the inflight is the next event.
	itr := s.inflight.ForwardIterator()
	itr.Next()
	e, _ := itr.Next()
	return e, true
}

func (s *stream[T, D]) lastEvent() (*eventWrap[T, D], bool) {
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
		id: s.id,

		handleTimeOnNum:  handleTimeOnNum,
		handleCountOnNum: handleCountOnNum,

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
		var a int
		a++
	default:
	}

	// Reset the statistics.
	s.handleTime = 0
	s.handleCount = 0
}

func (s *stream[T, D]) resetPathStat() []pathStat {
	pathStats := make([]pathStat, 0, len(s.pathMap))
	// We need to add the time of the running event to the total time.
	// To make the scheduler to notice the long running events.
	re, rt, _, running := s.runningEvent()
	for _, stat := range s.pathMap {
		ps := pathStat{
			path:      stat.path,
			totalTime: stat.totalTime,
			waitLen:   stat.waitLen,
		}
		if running && re.path() == stat.path {
			ps.totalTime += rt
			// One event of this path is running. But we only decrease the waitLen after the event is done.
			// To make the report more accurate, we decrease the waitLen here.
			ps.waitLen--
		}
		pathStats = append(pathStats, ps)

		// Reset the statistics.
		stat.totalTime = 0
	}
	// Sort the path stats by the total time in descending order.
	sort.Slice(pathStats, func(i, j int) bool {
		return pathStats[i].totalTime > pathStats[j].totalTime
	})
	return pathStats
}
