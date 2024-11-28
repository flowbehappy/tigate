package dynstream

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/utils/deque"
	"github.com/pingcap/ticdc/utils/heap"
)

var nextReportRound = atomic.Int64{}

const BlockLenInPendingQueue = 32

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
	handledTSHeapIndex int

	// Those values below are used to compare in the heap.
	frontTimestamp Timestamp // The timestamp of the front event.
	frontQueueTime time.Time // The queue time of the front event.

	areaMemStat *areaMemStat[A, P, T, D, H]

	pendingSize          int  // The total size(bytes) of pending events in the pendingQueue of the path.
	paused               bool // The path is paused to send events.
	lastSwitchPausedTime time.Time
	lastSendFeedbackTime time.Time

	lastHandledTS Timestamp
}

func newPathInfo[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](area A, path P, dest D) *pathInfo[A, P, T, D, H] {
	pi := &pathInfo[A, P, T, D, H]{
		area:         area,
		path:         path,
		dest:         dest,
		pendingQueue: deque.NewDeque[eventWrap[A, P, T, D, H]](BlockLenInPendingQueue, 0 /*unlimited*/),
		pathStat:     &pathStat[A, P, T, D, H]{},
	}
	return pi
}

func (pi *pathInfo[A, P, T, D, H]) setStream(stream *stream[A, P, T, D, H]) {
	pi.stream = stream
}

func (pi *pathInfo[A, P, T, D, H]) setEventBlockAllocator(eventBlockAllocator *deque.BlockAllocator[eventWrap[A, P, T, D, H]]) {
	pi.pendingQueue.SetBlockAllocator(eventBlockAllocator)
}

func (pi *pathInfo[A, P, T, D, H]) resetStat() {
	// Don't create a new pathStat on the heap, just reset the fields.
	(*pi.pathStat) = pathStat[A, P, T, D, H]{pathInfo: pi}
}

type streamStat[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	id int

	// Time elapsed since the last statistics report
	elapsedTime time.Duration
	// Total CPU time spent processing events
	processingTime time.Duration
	count          int

	pendingLen int

	mostBusyPath *heap.Heap[*pathStat[A, P, T, D, H]]
}

func (s streamStat[A, P, T, D, H]) getMostBusyPaths() []*pathStat[A, P, T, D, H] {
	if s.mostBusyPath == nil || s.mostBusyPath.IsEmpty() {
		return nil
	}
	return s.mostBusyPath.All()
}

// Try to add the path to the busy heap.
// If the heap is not full, add the path directly.
// If the heap is full, only add the path if it is busier than the least busy path in the heap.
func tryAddPathToBusyHeap[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](heap *heap.Heap[*pathStat[A, P, T, D, H]], pi *pathStat[A, P, T, D, H], maxBusyPathsToTrack int) {
	if heap.Len() < maxBusyPathsToTrack {
		heap.AddOrUpdate(pi)
	} else if top, _ := heap.PeekTop(); top.LessThan(pi) {
		heap.PopTop()
		heap.AddOrUpdate(pi)
	}
}

// eventWrap contains the event and the path info.
// It can be a event or a wake signal.
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

type doneInfo[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	pathInfo   *pathInfo[A, P, T, D, H]
	handleTime time.Duration
}

// A stream uses two goroutines
// 1. handleLoop: to handle the events.
// 2. reportStatLoop: to report the statistics.
type stream[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	id int

	handler Handler[A, P, T, D]

	eventBlockAllocator *deque.BlockAllocator[eventWrap[A, P, T, D, H]] // The allocator for blocks to store eventWraps in the pendingQueue.

	// The fields when UseBuffer is true.
	bufferCount atomic.Int64
	inChan      chan eventWrap[A, P, T, D, H] // The buffer channel to receive the events.
	outChan     chan eventWrap[A, P, T, D, H] // The buffer channel to send the events.

	// The channel used by the handleLoop to receive the events.
	eventChan chan eventWrap[A, P, T, D, H]

	eventQueue eventQueue[A, P, T, D, H]    // The queue to store the pending events.
	doneChan   chan doneInfo[A, P, T, D, H] // The channel to receive the done events.

	reportNow chan struct{} // For test, make the reportStatLoop to report immediately.

	reportChan          chan streamStat[A, P, T, D, H]
	maxBusyPathsToTrack int // The maximum number of paths to track the busy status.
	option              Option

	isClosed atomic.Bool

	handleWg sync.WaitGroup
	reportWg sync.WaitGroup

	startTime time.Time

	_statMinHandledTS atomic.Uint64
	_statSinceStart   atomic.Int64 // To avoid storing an atomic Time
}

func newStream[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](
	id int,
	handler H,
	reportChan chan streamStat[A, P, T, D, H],
	maxBusyPathsToTrack int,
	option Option,
) *stream[A, P, T, D, H] {
	s := &stream[A, P, T, D, H]{
		id:                  id,
		handler:             handler,
		eventQueue:          newEventQueue(option, handler),
		doneChan:            make(chan doneInfo[A, P, T, D, H], 64),
		reportNow:           make(chan struct{}, 1),
		reportChan:          reportChan,
		maxBusyPathsToTrack: maxBusyPathsToTrack,
		option:              option,
		startTime:           time.Now(),
	}
	if option.UseBuffer {
		s.inChan = make(chan eventWrap[A, P, T, D, H], 64)
		s.outChan = make(chan eventWrap[A, P, T, D, H], 64)

		s.eventChan = s.outChan
	} else {
		s.eventChan = make(chan eventWrap[A, P, T, D, H], 64)
	}
	a := deque.NewBlockAllocator[eventWrap[A, P, T, D, H]](BlockLenInPendingQueue, 64)
	s.eventBlockAllocator = &a
	return s
}

func (s *stream[A, P, T, D, H]) getPendingSize() int {
	if s.option.UseBuffer {
		return len(s.inChan) + int(s.bufferCount.Load()) + len(s.outChan) + int(s.eventQueue.totalPendingLength.Load())
	} else {
		return len(s.eventChan) + int(s.eventQueue.totalPendingLength.Load())
	}
}

func (s *stream[A, P, T, D, H]) getMinHandledTS() (uint64, int64) {
	return s._statMinHandledTS.Load(),
		int64(time.Since(s.startTime)) - s._statSinceStart.Load()
}

func (s *stream[A, P, T, D, H]) in() chan eventWrap[A, P, T, D, H] {
	if s.option.UseBuffer {
		return s.inChan
	} else {
		return s.eventChan
	}

}

// Start the stream.
func (s *stream[A, P, T, D, H]) start(acceptedPaths []*pathInfo[A, P, T, D, H], formerStreams ...*stream[A, P, T, D, H]) {
	if s.isClosed.Load() {
		panic("The stream has been closed.")
	}
	if s.option.UseBuffer {
		go s.reciever()
	}

	s.handleWg.Add(1)
	if s.option.UseBuffer {
		go s.handleLoop(acceptedPaths, formerStreams)
	} else {
		go s.handleLoop(acceptedPaths, formerStreams)
	}

	s.reportWg.Add(1)
	go s.reportStatLoop()
}

// Close the stream and wait for all goroutines to exit.
// wait is by default true, which means to wait for the goroutines to exit.
func (s *stream[A, P, T, D, H]) close(wait ...bool) {
	if s.isClosed.CompareAndSwap(false, true) {
		if s.option.UseBuffer {
			close(s.inChan)
		} else {
			close(s.eventChan)
		}
	}
	if len(wait) == 0 || wait[0] {
		s.handleWg.Wait()
	}
}

func (s *stream[A, P, T, D, H]) reciever() {
	buffer := deque.NewDeque[eventWrap[A, P, T, D, H]](BlockLenInPendingQueue, 0 /*unlimited*/)
	defer func() {
		// Move all remaining events in the buffer to the outChan.
		for {
			event, ok := buffer.FrontRef()
			if !ok {
				break
			} else {
				s.outChan <- *event
				buffer.PopFront()
				s.bufferCount.Add(-1)
			}
		}
		close(s.outChan)
	}()
	for {
		event, ok := buffer.FrontRef()
		if !ok {
			e, ok := <-s.inChan
			if !ok {
				return
			}
			buffer.PushBack(e)
			s.bufferCount.Add(1)
		} else {
			select {
			case e, ok := <-s.inChan:
				if !ok {
					return
				}
				buffer.PushBack(e)
				s.bufferCount.Add(1)
			case s.outChan <- *event:
				buffer.PopFront()
				s.bufferCount.Add(-1)
			}
		}
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
		close(s.doneChan)

		// Move remaining events in the eventChan to pendingQueue.
		for e := range s.eventChan {
			pushToPendingQueue(e)
		}

		s.reportWg.Wait()
		s.handleWg.Done()
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
		p.setEventBlockAllocator(s.eventBlockAllocator)
		s.eventQueue.addPath(p)
	}

	// Variables below will be used in the Loop below.
	// Declared here to avoid repeated allocation.
	var (
		eventQueueEmpty = false
		eventBuf        = make([]T, 0, s.option.BatchCount)
		zeroT           T
		cleanUpEventBuf = func() {
			for i := range eventBuf {
				eventBuf[i] = zeroT
			}
			eventBuf = eventBuf[:0]
		}
		path   *pathInfo[A, P, T, D, H]
		lastTS Timestamp
	)

	// For testing. Don't handle events until this wait group is done.
	if s.option.handleWait != nil {
		s.option.handleWait.Wait()
	}

	// 1. Drain the eventChan to pendingQueue.
	// 2. Pop events from the eventQueue and handle them.
Loop:
	for {
		if eventQueueEmpty {
			e, ok := <-s.eventChan
			if !ok {
				// The stream is closed.
				return
			}
			pushToPendingQueue(e)
			eventQueueEmpty = false
		} else {
			select {
			case e, ok := <-s.eventChan:
				if !ok {
					return
				}
				pushToPendingQueue(e)
				eventQueueEmpty = false
			default:
				eventBuf, path, lastTS = s.eventQueue.popEvents(eventBuf)
				if len(eventBuf) == 0 {
					eventQueueEmpty = true
					continue Loop
				}
				beginTime := time.Now()
				path.blocking = s.handler.Handle(path.dest, eventBuf...)
				doneTime := time.Now()
				s.doneChan <- doneInfo[A, P, T, D, H]{pathInfo: path, handleTime: doneTime.Sub(beginTime)}

				if path.blocking {
					s.eventQueue.blockPath(path)
				} else {
					path.lastHandledTS = lastTS
					minTS := s.eventQueue.onHandledTS(path)
					if minTS > 0 {
						s._statMinHandledTS.Store(uint64(minTS))
						s._statSinceStart.Store(int64(doneTime.Sub(s.startTime)))
					}
				}
				cleanUpEventBuf()
			}
		}
	}
}

func (s *stream[A, P, T, D, H]) reportStatLoop() {
	defer s.reportWg.Done()
	ticker := time.NewTicker(s.option.ReportInterval)
	defer ticker.Stop()

	// statistics variables that are used in the loop.
	var (
		// lastReportTime is used to calculate the period since the last report.
		lastReportTime = time.Now()
		reportRound    = nextReportRound.Add(1)
		handleCount    = 0
		totalTime      = time.Duration(0)
		mostBusyPaths  = heap.NewHeap[*pathStat[A, P, T, D, H]]()
	)

	resetStatistics := func() {
		lastReportTime = time.Now()
		reportRound = nextReportRound.Add(1)
		handleCount = 0
		totalTime = time.Duration(0)
		mostBusyPaths = heap.NewHeap[*pathStat[A, P, T, D, H]]()
	}

	recordStat := func(doneInfo doneInfo[A, P, T, D, H]) {
		handleCount++
		totalTime += doneInfo.handleTime

		if doneInfo.pathInfo.reportRound != reportRound {
			doneInfo.pathInfo.resetStat()
			doneInfo.pathInfo.reportRound = reportRound
		}

		doneInfo.pathInfo.pathStat.totalTime += doneInfo.handleTime
		doneInfo.pathInfo.pathStat.count++

		tryAddPathToBusyHeap(mostBusyPaths, doneInfo.pathInfo.pathStat, s.maxBusyPathsToTrack)
	}

	reportStat := func() {
		select {
		case <-time.After(10 * time.Millisecond):
			// If the reportChan is full, we just drop the report.
			// It could happen when the scheduler is closing or too busy.
		case s.reportChan <- streamStat[A, P, T, D, H]{
			id:             s.id,
			elapsedTime:    time.Since(lastReportTime),
			processingTime: totalTime,
			count:          handleCount,
			pendingLen:     int(s.eventQueue.totalPendingLength.Load()), // It is not very accurate, because this value is updated by the handle goroutine.
			mostBusyPath:   mostBusyPaths,
		}:
			// Only reset the statistics when the report is sent successfully.
			resetStatistics()
		}
	}

	for {
		select {
		case <-ticker.C:
			reportStat()
		case <-s.reportNow:
			reportStat()
		case doneInfo, ok := <-s.doneChan:
			if !ok {
				reportStat()
				return
			}
			recordStat(doneInfo)
		}
	}
}
