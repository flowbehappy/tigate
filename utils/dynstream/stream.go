package dynstream

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/flowbehappy/tigate/utils/deque"
	"github.com/flowbehappy/tigate/utils/heap"
)

var nextReportRound = atomic.Int64{}

// ====== internal types ======

type pathStat[A Area, P Path, T Event, D Dest] struct {
	pathInfo  *pathInfo[A, P, T, D]
	totalTime time.Duration
	count     int
	heapIndex int
}

func (p *pathStat[A, P, T, D]) busyRatio(period time.Duration) float64 {
	if period == 0 {
		return 0
	} else {
		return float64(p.totalTime) / float64(period)
	}
}

// Implement heap.Item interface
func (p *pathStat[A, P, T, D]) SetHeapIndex(index int) { p.heapIndex = index }
func (p *pathStat[A, P, T, D]) GetHeapIndex() int      { return p.heapIndex }
func (p *pathStat[A, P, T, D]) LessThan(o *pathStat[A, P, T, D]) bool {
	return p.totalTime < o.totalTime
} // It is safe on a 64-bit machine.

type pathInfo[A Area, P Path, T Event, D Dest] struct {
	// Note that although this struct is used by multiple goroutines, it doesn't need synchronization because
	// different fields are either immutable or accessed by different goroutines.
	// We use one struct to store them together to avoid mapping by path in different places in many times.
	area A
	path P
	dest D

	// Stream level area info.
	areaInfo *areaInfo[A, P, T, D]
	stream   *stream[A, P, T, D]
	// This field is used to mark the path as removed, so that the handle goroutine can ignore it.
	// Note that we should not need to use a atomic.Bool here, because this field is set by the RemovePaths method,
	// and we use sync.WaitGroup to wait for finish. So if RemovePaths is called in the handle goroutine, it should be
	// guaranteed to see the memory change of this field.
	removed bool

	pendingQueue *deque.Deque[eventWrap[A, P, T, D]]
	blocking     bool

	reportRound int64
	pathStat    *pathStat[A, P, T, D]

	// The indexes in the heap.
	tsHeapIndex int
	qtHeapIndex int
}

func newPathInfo[A Area, P Path, T Event, D Dest](area A, path P, dest D) *pathInfo[A, P, T, D] {
	pi := &pathInfo[A, P, T, D]{
		area:         area,
		path:         path,
		dest:         dest,
		pendingQueue: deque.NewDeque[eventWrap[A, P, T, D]](32, 0),
		pathStat:     &pathStat[A, P, T, D]{},
	}
	return pi
}

func (pi *pathInfo[A, P, T, D]) resetStat() {
	// Don't create a new pathStat on the heap, just reset the fields.
	(*pi.pathStat) = pathStat[A, P, T, D]{pathInfo: pi}
}

func (pi *pathInfo[A, P, T, D]) appendEvent(e eventWrap[A, P, T, D], option *OptionEnhanced[A, P, T, D]) bool {
	if option.MaxPendingLength > 0 && pi.pendingQueue.Length() >= option.MaxPendingLength {
		switch option.DropPolicy {
		case DropLate:
			if option.DropListener != nil {
				option.DropListener.OnDrop(e.pathInfo.dest, e.event)
			}
		case DropEarly:
			dropped, _ := pi.pendingQueue.PopFront()
			if option.DropListener != nil {
				option.DropListener.OnDrop(e.pathInfo.dest, dropped.event)
			}
			e.pathInfo.pendingQueue.PushBack(e)
		}
		return false
	} else {
		e.pathInfo.pendingQueue.PushBack(e)
		return true
	}
}

type streamStat[A Area, P Path, T Event, D Dest] struct {
	id int

	period    time.Duration
	totalTime time.Duration
	count     int

	pendingLen int

	mostBusyPath heap.Heap[*pathStat[A, P, T, D]]
}

func (s streamStat[A, P, T, D]) getMostBusyPaths() []*pathStat[A, P, T, D] {
	if s.mostBusyPath == nil {
		return nil
	}
	return s.mostBusyPath.All()
}

func tryAddPathToBusyHeap[A Area, P Path, T Event, D Dest](heap heap.Heap[*pathStat[A, P, T, D]], pi *pathStat[A, P, T, D], trackTop int) {
	if heap.Len() < trackTop {
		heap.AddOrUpdate(pi)
	} else if top, _ := heap.PeekTop(); top.LessThan(pi) {
		heap.PopTop()
		heap.AddOrUpdate(pi)
	}
}

// Only contains one kind of event:
// 1. event
// 2. wake = true
type eventWrap[A Area, P Path, T Event, D Dest] struct {
	event T
	wake  bool

	pathInfo *pathInfo[A, P, T, D]

	eType     Type
	timestamp Timestamp
	queueTime time.Time
}

func (e eventWrap[A, P, T, D]) isZero() bool {
	return e.pathInfo == nil
}

type eventSignal[A Area, P Path, T Event, D Dest] struct {
	pathInfo   *pathInfo[A, P, T, D]
	eventCount int
}

type doneInfo[A Area, P Path, T Event, D Dest] struct {
	pathInfo   *pathInfo[A, P, T, D]
	handleTime time.Duration
}

func (d doneInfo[A, P, T, D]) isZero() bool {
	return d.pathInfo == nil
}

// A stream uses two goroutines
// 1. handleLoop: to handle the events.
// 2. reportStatLoop: to report the statistics.
type stream[A Area, P Path, T Event, D Dest] struct {
	id int

	handler Handler[P, T, D]

	inChan       chan eventWrap[A, P, T, D] // The buffer channel to receive the events.
	pendingQueue pendingQueue[A, P, T, D]   // The queue to store the pending events.
	// signalQueue *deque.Deque[eventSignal[A, P, T, D]] // The queue to store the event signals.
	donChan chan doneInfo[A, P, T, D] // The channel to receive the done events.

	reportNow chan struct{} // For test, make the reportStatLoop to report immediately.

	// pendingLen int // The total pending event count of all paths

	reportChan    chan streamStat[A, P, T, D]
	trackTopPaths int
	option        OptionEnhanced[A, P, T, D]

	hasClosed atomic.Bool

	handleDone sync.WaitGroup
	reportDone sync.WaitGroup
}

func newStream[A Area, P Path, T Event, D Dest](
	id int,
	handler Handler[P, T, D],
	reportChan chan streamStat[A, P, T, D],
	trackTopPaths int,
	option OptionEnhanced[A, P, T, D],
) *stream[A, P, T, D] {
	s := &stream[A, P, T, D]{
		id:            id,
		handler:       handler,
		inChan:        make(chan eventWrap[A, P, T, D], 64),
		pendingQueue:  newPendingQueue[A, P, T, D](option),
		donChan:       make(chan doneInfo[A, P, T, D], 64),
		reportNow:     make(chan struct{}, 1),
		reportChan:    reportChan,
		trackTopPaths: trackTopPaths,
		option:        option,
	}
	return s
}

func (s *stream[A, P, T, D]) addPath(pi *pathInfo[A, P, T, D]) {
	pi.stream = s
	s.pendingQueue.addPath(pi)
}

func (s *stream[A, P, T, D]) removePath(pi *pathInfo[A, P, T, D]) {
	pi.stream = nil
	s.pendingQueue.removePath(pi)
}

func (s *stream[A, P, T, D]) in() chan eventWrap[A, P, T, D] {
	return s.inChan
}

// Close the former streams and start the handleLoop and reportStatLoop.
func (s *stream[A, P, T, D]) start(acceptedPaths []*pathInfo[A, P, T, D], formerStreams ...*stream[A, P, T, D]) {
	if s.hasClosed.Load() {
		panic("The stream has been closed.")
	}

	s.handleDone.Add(1)
	go s.handleLoop(acceptedPaths, formerStreams)

	s.reportDone.Add(1)
	go s.reportStatLoop()
}

// Close the stream and wait for all goroutines to exit.
// wait is by default true, which means to wait for the goroutines to exit.
func (s *stream[A, P, T, D]) close(wait ...bool) {
	if s.hasClosed.CompareAndSwap(false, true) {
		close(s.inChan)
	}
	if len(wait) == 0 || wait[0] {
		s.handleDone.Wait()
	}
}

func (s *stream[A, P, T, D]) handleLoop(acceptedPaths []*pathInfo[A, P, T, D], formerStreams []*stream[A, P, T, D]) {
	pushToPendingQueue := func(e eventWrap[A, P, T, D]) {
		if e.wake {
			e.pathInfo.blocking = false
			s.pendingQueue.wakePath(e.pathInfo)
		} else {
			s.pendingQueue.appendEvent(e)
		}

		// if e.wake {
		// 	// It is a wake event, we set the path to be non-blocking, and generate a signal for all pending events.
		// 	e.pathInfo.blocking = false
		// 	count := e.pathInfo.pendingQueue.Length()
		// 	if count > 0 {
		// 		s.signalQueue.PushFront(eventSignal[A, P, T, D]{pathInfo: e.pathInfo, eventCount: count})
		// 	}
		// } else {
		// 	// It is a normal event

		// 	// Push to pendingQueue
		// 	if s.option.MaxPendingLength > 0 && e.pathInfo.pendingQueue.Length() >= s.option.MaxPendingLength {
		// 		switch s.option.DropPolicy {
		// 		case DropLate:
		// 			if s.dropListener != nil {
		// 				s.dropListener.OnDrop(e.pathInfo.dest, e.event)
		// 			}
		// 			return
		// 		case DropEarly:
		// 			dropped, _ := e.pathInfo.pendingQueue.PopFront()
		// 			if s.dropListener != nil {
		// 				s.dropListener.OnDrop(e.pathInfo.dest, dropped)
		// 			}

		// 			e.pathInfo.pendingQueue.PushBack(e.event)
		// 			return
		// 		}
		// 	} else {
		// 		e.pathInfo.pendingQueue.PushBack(e.event)
		// 		s.pendingLen++
		// 		// Send a signal
		// 		sg, ok := s.signalQueue.BackRef()
		// 		if ok && sg.pathInfo == e.pathInfo {
		// 			sg.eventCount++
		// 		} else {
		// 			s.signalQueue.PushBack(eventSignal[A, P, T, D]{pathInfo: e.pathInfo, eventCount: 1})
		// 		}
		// 	}
		// }
	}

	defer func() {
		if r := recover(); r != nil {
			log.Panic("handleLoop panic",
				zap.Any("recover", r),
				zap.Stack("stack"))
		}
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
		// We don't need to explicitly remove the paths from the pendingQueue.
		// Because the stream is closed already, and the data structure is not used anymore.
	}

	// We initialize the pathMap here to avoid blocking the main goroutine.
	// As there could be many paths, and the initialization could be time-consuming.
	for _, p := range acceptedPaths {
		s.addPath(p)
	}

	drainPending := false
	eventBuf := make([]T, 0, s.option.BatchSize)
	var zeroT T

	// For testing. Don't handle events until this wait group is done.
	if s.option.handleWait != nil {
		s.option.handleWait.Wait()
	}

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
				// signal, ok := s.signalQueue.FrontRef() // We are going to update the signal directly, so we need the reference.
				// if !ok {
				// 	drainPending = true
				// 	continue Loop
				// }
				// if signal.eventCount == 0 {
				// 	panic("signal event count is zero")
				// }
				// if signal.pathInfo.blocking || signal.pathInfo.removed {
				// 	// The path is blocking or removed, we should ignore the signal completely.
				// 	s.signalQueue.PopFront()
				// 	continue Loop
				// }

				// handleCount := min(signal.eventCount, s.option.BatchSize)

				// for i := 0; i < handleCount; i++ {
				// 	e, ok := signal.pathInfo.pendingQueue.PopFront()
				// 	if !ok {
				// 		// The signal could contain more events than the pendingQueue,
				// 		// which is possible when the path is removed or recovered from blocked.
				// 		break
				// 	}
				// 	eventBuf = append(eventBuf, e)
				// }

				// actualCount := len(eventBuf)

				// if actualCount != 0 {
				// 	now := time.Now()
				// 	signal.pathInfo.blocking = s.handler.Handle(signal.pathInfo.dest, eventBuf...)
				// 	s.donChan <- doneInfo[A, P, T, D]{pathInfo: signal.pathInfo, handleTime: time.Since(now)}
				// }

				// s.pendingLen -= actualCount
				// if s.pendingLen < 0 {
				// 	panic("pendingLen is less than zero")
				// }

				// signal.eventCount -= handleCount
				// if signal.eventCount == 0 || actualCount < handleCount {
				// 	// signal.eventCount == 0 means the signal is handled completely.
				// 	// actualCount < handleCount means the pendingQueue is drained.
				// 	s.signalQueue.PopFront()
				// }

				eventBuf, path := s.pendingQueue.popEvents(eventBuf)
				if len(eventBuf) == 0 {
					drainPending = true
					continue Loop
				}

				now := time.Now()
				path.blocking = s.handler.Handle(path.dest, eventBuf...)
				s.donChan <- doneInfo[A, P, T, D]{pathInfo: path, handleTime: time.Since(now)}

				if path.blocking {
					s.pendingQueue.blockPath(path)
				}

				// clean up the eventBuf
				for i, _ := range eventBuf {
					eventBuf[i] = zeroT
				}
				eventBuf = eventBuf[:0]
			}
		}
	}
}

func (s *stream[A, P, T, D]) reportStatLoop() {
	defer s.reportDone.Done()

	lastReportTime := time.Now()
	nextReportTime := lastReportTime.Add(s.option.ReportInterval)
	reportWait := time.After(time.Until(nextReportTime))

	reportRound := nextReportRound.Add(1)

	handleCount := 0
	totalTime := time.Duration(0)
	mostBusyPaths := heap.NewHeap[*pathStat[A, P, T, D]]()

	recordStat := func(doneInfo doneInfo[A, P, T, D]) {
		handleCount++
		totalTime += doneInfo.handleTime

		if doneInfo.pathInfo.reportRound != reportRound {
			doneInfo.pathInfo.resetStat()
			doneInfo.pathInfo.reportRound = reportRound
		}

		doneInfo.pathInfo.pathStat.totalTime += doneInfo.handleTime
		doneInfo.pathInfo.pathStat.count++

		tryAddPathToBusyHeap(mostBusyPaths, doneInfo.pathInfo.pathStat, s.trackTopPaths)
	}

	reportStat := func() {
		select {
		case <-time.After(10 * time.Millisecond):
			// If the reportChan is full, we just drop the report.
			// It could happen when the scheduler is closing or too busy.
		case s.reportChan <- streamStat[A, P, T, D]{
			id:           s.id,
			period:       time.Since(lastReportTime),
			totalTime:    totalTime,
			count:        handleCount,
			pendingLen:   s.pendingQueue.pendingLength, // It is not very accurate, because this value is updated by the handle goroutine.
			mostBusyPath: mostBusyPaths,
		}:
		}
		reportRound = nextReportRound.Add(1)
		handleCount = 0
		totalTime = time.Duration(0)
		mostBusyPaths = heap.NewHeap[*pathStat[A, P, T, D]]()

		lastReportTime = time.Now()
		nextReportTime = lastReportTime.Add(s.option.ReportInterval)
		reportWait = time.After(time.Until(nextReportTime))
	}

	for {
		select {
		case <-reportWait:
			reportStat()
		case <-s.reportNow:
			reportStat()
		case doneInfo, ok := <-s.donChan:
			if !doneInfo.isZero() {
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
