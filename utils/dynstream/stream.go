package dynstream

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/flowbehappy/tigate/utils/deque"
	"github.com/flowbehappy/tigate/utils/heap"
)

var nextReportRound = atomic.Int64{}

// Only contains one kind of event:
// 1. event + pathInfo
// 2. wake = true
type eventWrap[P Path, T Event[P], D any] struct {
	event    T
	pathInfo *pathInfo[P, T, D]

	wake bool
}

func (e eventWrap[P, T, D]) isZero() bool {
	return e.pathInfo == nil && !e.wake
}

type eventSignal[P Path, T Event[P], D any] struct {
	pathInfo   *pathInfo[P, T, D]
	eventCount int
}

type doneInfo[P Path, T Event[P], D any] struct {
	pathInfo   *pathInfo[P, T, D]
	handleTime time.Duration
	pendingLen int
}

type stream[P Path, T Event[P], D any] struct {
	id int

	handler Handler[P, T, D]

	inChan      chan eventWrap[P, T, D]            // The buffer channel to receive the events.
	signalQueue *deque.Deque[eventSignal[P, T, D]] // The queue to store the event signals.
	donChan     chan *doneInfo[P, T, D]            // The channel to receive the done events.

	reportNow chan struct{} // For test, make the reportStatLoop to report immediately.

	pendingLen int // The total pending event count of all paths

	reportChan     chan *streamStat[P, T, D]
	reportInterval time.Duration
	trackTopPaths  int

	hasClosed atomic.Bool

	handleDone sync.WaitGroup
	reportDone sync.WaitGroup
}

func newStream[P Path, T Event[P], D any](
	id int,
	handler Handler[P, T, D],
	reportChan chan *streamStat[P, T, D],
	reportInterval time.Duration, // 200 milliseconds?
	trackTopPaths int,
) *stream[P, T, D] {
	s := &stream[P, T, D]{
		id:             id,
		handler:        handler,
		inChan:         make(chan eventWrap[P, T, D], 64),
		signalQueue:    deque.NewDeque[eventSignal[P, T, D]](128, 0),
		donChan:        make(chan *doneInfo[P, T, D], 64),
		reportNow:      make(chan struct{}, 1),
		reportChan:     reportChan,
		reportInterval: reportInterval,
		trackTopPaths:  trackTopPaths,
	}

	return s
}

func (s *stream[P, T, D]) in() chan eventWrap[P, T, D] {
	return s.inChan
}

func (s *stream[P, T, D]) start(acceptedPaths []*pathInfo[P, T, D], formerStreams ...*stream[P, T, D]) {
	if s.hasClosed.Load() {
		panic("The stream has been closed.")
	}

	s.handleDone.Add(1)
	go s.handleLoop(acceptedPaths, formerStreams)

	s.reportDone.Add(1)
	go s.reportStatLoop()
}

// Close the stream and return the running event.
// Not all of the new streams need to wait for the former stream's handle goroutine to finish.
// Only the streams that are interested in the path of the running event need to wait.
func (s *stream[P, T, D]) close() {
	if s.hasClosed.CompareAndSwap(false, true) {
		close(s.inChan)
	}
	s.handleDone.Wait()
}

func (s *stream[P, T, D]) addPaths(newPaths []*pathInfo[P, T, D]) {
	for _, p := range newPaths {
		len := p.pendingQueue.Length()
		if len > 0 {
			s.signalQueue.PushBack(eventSignal[P, T, D]{pathInfo: p, eventCount: len})
			s.pendingLen += len
		}
	}
}

func (s *stream[P, T, D]) handleLoop(acceptedPaths []*pathInfo[P, T, D], formerStreams []*stream[P, T, D]) {
	pushToPendingQueue := func(e eventWrap[P, T, D]) {
		if e.wake {
			// It is a wake event, we set the path to be non-blocking, and generate a signal for all pending events.
			e.pathInfo.blocking = false
			s.signalQueue.PushBack(eventSignal[P, T, D]{pathInfo: e.pathInfo, eventCount: e.pathInfo.pendingQueue.Length()})
		} else {
			// It is a normal event

			// Push to pendingQueue
			e.pathInfo.pendingQueue.PushBack(e.event)
			s.pendingLen++
			// Send a signal
			sg, ok := s.signalQueue.BackRef()
			if ok && sg.pathInfo == e.pathInfo {
				sg.eventCount++
			} else {
				s.signalQueue.PushBack(eventSignal[P, T, D]{pathInfo: e.pathInfo, eventCount: 1})
			}
		}
	}

	defer func() {
		close(s.donChan)

		// Move remaing events in the inChan to pendingQueue.
		for e := range s.inChan {
			pushToPendingQueue(e)
		}

		s.reportDone.Wait()
		s.handleDone.Done()
	}()

	// Close and wait for the former streams.
	for _, stream := range formerStreams {
		stream.close()
	}

	// We initialize the pathMap here to avoid blocking the main goroutine.
	// As there could be many paths, and the initialization could be time-consuming.
	s.addPaths(acceptedPaths)

	drainPending := false

Loop:
	for {
		if drainPending {
			select {
			case e, ok := <-s.inChan:
				if !e.isZero() {
					pushToPendingQueue(e)
					drainPending = false
				}
				if !ok {
					return
				}
			}
		} else {
			select {
			case e, ok := <-s.inChan:
				if !e.isZero() {
					pushToPendingQueue(e)
					drainPending = false
				}
				if !ok {
					return
				}
			default:
				now := time.Now()

				signal, ok := s.signalQueue.Front()
				if !ok {
					drainPending = true
					continue Loop
				}
				if signal.eventCount == 0 {
					panic("signal event count is zero")
				}
				if signal.pathInfo.blocking {
					// The path is blocking, we should drop the signal.
					s.signalQueue.PopFront()
					continue Loop
				}

				signal.eventCount--
				e, ok := signal.pathInfo.pendingQueue.Front()
				if !ok {
					// The pendingQueue is empty, we should remove the signal.
					s.signalQueue.PopFront()
					continue Loop
				}

				s.handler.Handle(e, signal.pathInfo.dest)

				signal.pathInfo.pendingQueue.PopFront()
				s.pendingLen--
				if s.pendingLen < 0 {
					panic("pendingLen is less than zero")
				}

				if signal.eventCount == 0 {
					s.signalQueue.PopFront()
				}

				// We want the handle time to be as long as possible
				s.donChan <- &doneInfo[P, T, D]{pathInfo: signal.pathInfo, handleTime: time.Since(now), pendingLen: s.pendingLen}
			}
		}
	}
}

func (s *stream[P, T, D]) reportStatLoop() {
	defer s.reportDone.Done()

	lastReportTime := time.Now()
	nextReportTime := lastReportTime.Add(s.reportInterval)
	reportWait := time.After(time.Until(nextReportTime))

	reportRound := nextReportRound.Add(1)

	handleCount := 0
	totalTime := time.Duration(0)
	mostBusyPaths := heap.NewHeap[*pathStat[P, T, D]]()

	recordStat := func(doneInfo *doneInfo[P, T, D]) {
		handleCount++
		totalTime += doneInfo.handleTime

		if doneInfo.pathInfo.reportRound != reportRound {
			doneInfo.pathInfo.resetStat()
			doneInfo.pathInfo.reportRound = reportRound
		}

		doneInfo.pathInfo.pathStat.totalTime += doneInfo.handleTime
		doneInfo.pathInfo.pathStat.count++
		doneInfo.pathInfo.pathStat.pendingLen = doneInfo.pendingLen

		tryAddPathToBusyHeap(mostBusyPaths, doneInfo.pathInfo.pathStat, s.trackTopPaths)
	}

	reportStat := func() {
		select {
		case <-time.After(10 * time.Millisecond):
			// If the reportChan is full, we just drop the report.
			// It could happen when the scheduler is closing or too busy.
		case s.reportChan <- &streamStat[P, T, D]{
			id:           s.id,
			period:       time.Since(lastReportTime),
			totalTime:    totalTime,
			count:        handleCount,
			pendingLen:   s.pendingLen,
			mostBusyPath: mostBusyPaths,
		}:
		}
		reportRound = nextReportRound.Add(1)
		handleCount = 0
		totalTime = time.Duration(0)
		mostBusyPaths = heap.NewHeap[*pathStat[P, T, D]]()

		lastReportTime = time.Now()
		nextReportTime = lastReportTime.Add(s.reportInterval)
		reportWait = time.After(time.Until(nextReportTime))
	}

	for {
		select {
		case <-reportWait:
			reportStat()
		case <-s.reportNow:
			reportStat()
		case doneInfo, ok := <-s.donChan:
			if doneInfo != nil {
				recordStat(doneInfo)
			}
			if !ok {
				// Drain the donChan and report before return. Mainly used for the test.
				for doneInfo := range s.donChan {
					recordStat(doneInfo)
				}
				reportStat()
				return
			}
		}
	}
}
