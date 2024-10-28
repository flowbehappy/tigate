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

type pathStat[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	pathInfo  *pathInfo[A, P, T, D, H]
	totalTime time.Duration
	count     int
	heapIndex int
}

func (p *pathStat[A, P, T, D, H]) busyRatio(period time.Duration) float64 {
	if period == 0 {
		return 0
	} else {
		return float64(p.totalTime) / float64(period)
	}
}

// Implement heap.Item interface
func (p *pathStat[A, P, T, D, H]) SetHeapIndex(index int) { p.heapIndex = index }
func (p *pathStat[A, P, T, D, H]) GetHeapIndex() int      { return p.heapIndex }
func (p *pathStat[A, P, T, D, H]) LessThan(o *pathStat[A, P, T, D, H]) bool {
	return p.totalTime < o.totalTime
}

// pathInfo contains the status of a path.
// Note that although this struct is used by multiple goroutines, it doesn't need synchronization because
// different fields are either immutable or accessed by different goroutines.
// We use one struct to store them together to avoid mapping by path in different places in many times,
// and to avoid the overhead of creating a new struct.
type pathInfo[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	area A
	path P
	dest D

	// The current stream this path belongs to.
	stream *stream[A, P, T, D, H]
	// This field is used to mark the path as removed, so that the handle goroutine can ignore it.
	// Note that we should not need to use a atomic.Bool here, because this field is set by the RemovePaths method,
	// and we use sync.WaitGroup to wait for finish. So if RemovePaths is called in the handle goroutine, it should be
	// guaranteed to see the memory change of this field.
	removed bool
	// The path is blocked by the handler.
	blocking bool

	// The pending events of the path.
	pendingQueue *deque.Deque[eventWrap[A, P, T, D, H]]

	// Fields used by the reportStatLoop.
	reportRound int64
	pathStat    *pathStat[A, P, T, D, H]

	// Fields only use by the eventQueue.
	// Stream level area info.
	// Each stream has its own areaInfo map, after a path is assigned to a stream, the areaInfo is set.
	streamAreaInfo     *streamAreaInfo[A, P, T, D, H]
	timestampHeapIndex int
	queueTimeHeapIndex int
	sizeHeapIndex      int

	// Those values below are used to compare in the heap.
	frontTimestamp Timestamp // The timestamp of the front event.
	frontQueueTime time.Time // The queue time of the front event.

	areaMemStat *areaMemStat[A, P, T, D, H]

	pendingSize          int  // The total size of pending events
	paused               bool // The path is paused to send events.
	lastSwitchPausedTime time.Time
	lastSendFeedbackTime time.Time
}

func newPathInfo[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](area A, path P, dest D) *pathInfo[A, P, T, D, H] {
	pi := &pathInfo[A, P, T, D, H]{
		area:         area,
		path:         path,
		dest:         dest,
		pendingQueue: deque.NewDeque[eventWrap[A, P, T, D, H]](32, 0),
		pathStat:     &pathStat[A, P, T, D, H]{},
	}
	return pi
}

func (pi *pathInfo[A, P, T, D, H]) resetStat() {
	// Don't create a new pathStat on the heap, just reset the fields.
	(*pi.pathStat) = pathStat[A, P, T, D, H]{pathInfo: pi}
}

type streamStat[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	id int

	period    time.Duration
	totalTime time.Duration
	count     int

	pendingLen int

	mostBusyPath *heap.Heap[*pathStat[A, P, T, D, H]]
}

func (s streamStat[A, P, T, D, H]) getMostBusyPaths() []*pathStat[A, P, T, D, H] {
	if s.mostBusyPath.IsEmpty() {
		return nil
	}
	return s.mostBusyPath.All()
}

func tryAddPathToBusyHeap[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](heap *heap.Heap[*pathStat[A, P, T, D, H]], pi *pathStat[A, P, T, D, H], trackTop int) {
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
type eventWrap[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	event T
	wake  bool

	pathInfo *pathInfo[A, P, T, D, H]

	paused    bool
	eventSize int
	eventType EventType
	timestamp Timestamp
	queueTime time.Time
}

func (e eventWrap[A, P, T, D, H]) isZero() bool {
	return e.pathInfo == nil
}

type doneInfo[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	pathInfo   *pathInfo[A, P, T, D, H]
	handleTime time.Duration
}

func (d doneInfo[A, P, T, D, H]) isZero() bool {
	return d.pathInfo == nil
}

// A stream uses two goroutines
// 1. handleLoop: to handle the events.
// 2. reportStatLoop: to report the statistics.
type stream[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	id int

	handler Handler[A, P, T, D]

	inChan     chan eventWrap[A, P, T, D, H] // The buffer channel to receive the events.
	eventQueue eventQueue[A, P, T, D, H]     // The queue to store the pending events.
	donChan    chan doneInfo[A, P, T, D, H]  // The channel to receive the done events.

	reportNow chan struct{} // For test, make the reportStatLoop to report immediately.

	reportChan    chan streamStat[A, P, T, D, H]
	trackTopPaths int
	option        Option

	hasClosed atomic.Bool

	handleDone sync.WaitGroup
	reportDone sync.WaitGroup
}

func newStream[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](
	id int,
	handler H,
	reportChan chan streamStat[A, P, T, D, H],
	trackTopPaths int,
	option Option,
) *stream[A, P, T, D, H] {
	s := &stream[A, P, T, D, H]{
		id:            id,
		handler:       handler,
		inChan:        make(chan eventWrap[A, P, T, D, H], 64),
		eventQueue:    newEventQueue[A, P, T, D, H](option, handler),
		donChan:       make(chan doneInfo[A, P, T, D, H], 64),
		reportNow:     make(chan struct{}, 1),
		reportChan:    reportChan,
		trackTopPaths: trackTopPaths,
		option:        option,
	}
	return s
}

func (s *stream[A, P, T, D, H]) in() chan eventWrap[A, P, T, D, H] {
	return s.inChan
}

// Close the former streams, add the paths to the stream, and start the handleLoop and reportStatLoop.
func (s *stream[A, P, T, D, H]) start(acceptedPaths []*pathInfo[A, P, T, D, H], formerStreams ...*stream[A, P, T, D, H]) {
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
func (s *stream[A, P, T, D, H]) close(wait ...bool) {
	if s.hasClosed.CompareAndSwap(false, true) {
		close(s.inChan)
	}
	if len(wait) == 0 || wait[0] {
		s.handleDone.Wait()
	}
}

func (s *stream[A, P, T, D, H]) handleLoop(acceptedPaths []*pathInfo[A, P, T, D, H], formerStreams []*stream[A, P, T, D, H]) {
	pushToPendingQueue := func(e eventWrap[A, P, T, D, H]) {
		if e.wake {
			e.pathInfo.blocking = false
			s.eventQueue.wakePath(e.pathInfo)
		} else if e.pathInfo.removed {
			s.eventQueue.removePath(e.pathInfo)
		} else {
			s.eventQueue.appendEvent(e)
		}
	}

	defer func() {
		if r := recover(); r != nil {
			log.Panic("handleLoop panic",
				zap.Any("recover", r),
				zap.Stack("stack"))
		}
		close(s.donChan)

		// Move remaining events in the inChan to pendingQueue.
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
		s.eventQueue.addPath(p)
	}

	drainPending := false
	eventBuf := make([]T, 0, s.option.BatchCount)
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
				var path *pathInfo[A, P, T, D, H]
				eventBuf, path = s.eventQueue.popEvents(eventBuf)
				if len(eventBuf) == 0 {
					drainPending = true
					continue Loop
				}
				now := time.Now()
				path.blocking = s.handler.Handle(path.dest, eventBuf...)
				s.donChan <- doneInfo[A, P, T, D, H]{pathInfo: path, handleTime: time.Since(now)}

				if path.blocking {
					s.eventQueue.blockPath(path)
				}

				// clean up the eventBuf
				for i := range eventBuf {
					eventBuf[i] = zeroT
				}
				eventBuf = eventBuf[:0]
			}
		}
	}
}

func (s *stream[A, P, T, D, H]) reportStatLoop() {
	defer s.reportDone.Done()

	lastReportTime := time.Now()
	nextReportTime := lastReportTime.Add(s.option.ReportInterval)
	reportWait := time.After(time.Until(nextReportTime))

	reportRound := nextReportRound.Add(1)

	handleCount := 0
	totalTime := time.Duration(0)
	mostBusyPaths := heap.NewHeap[*pathStat[A, P, T, D, H]]()

	recordStat := func(doneInfo doneInfo[A, P, T, D, H]) {
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
		case s.reportChan <- streamStat[A, P, T, D, H]{
			id:           s.id,
			period:       time.Since(lastReportTime),
			totalTime:    totalTime,
			count:        handleCount,
			pendingLen:   s.eventQueue.totalPendingLength, // It is not very accurate, because this value is updated by the handle goroutine.
			mostBusyPath: mostBusyPaths,
		}:
		}
		reportRound = nextReportRound.Add(1)
		handleCount = 0
		totalTime = time.Duration(0)
		mostBusyPaths = heap.NewHeap[*pathStat[A, P, T, D, H]]()

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
