package dynstream

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/utils/chann"
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

	// lastHandledTS Timestamp
}

func newPathInfo[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](area A, path P, dest D) *pathInfo[A, P, T, D, H] {
	pi := &pathInfo[A, P, T, D, H]{
		area:         area,
		path:         path,
		dest:         dest,
		pendingQueue: deque.NewDeque[eventWrap[A, P, T, D, H]](BlockLenInPendingQueue),
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

func (s streamStat[A, P, T, D, H]) isValid() bool {
	return s.count != 0
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
	event   T
	wake    bool
	newPath bool

	pathInfo *pathInfo[A, P, T, D, H]

	paused    bool
	eventSize int
	eventType EventType
}

func (ew eventWrap[A, P, T, D, H]) Timestamp() Timestamp {
	return 0
}

func (ew eventWrap[A, P, T, D, H]) QueueTime() time.Time {
	var t time.Time
	return t
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

	inputQueue *chann.UnlimitedChannel[eventWrap[A, P, T, D, H]]

	// The fields when UseBuffer is true.
	// bufferCount atomic.Int64
	// inChan      chan eventWrap[A, P, T, D, H] // The buffer channel to receive the events.
	// outChan     chan eventWrap[A, P, T, D, H] // The buffer channel to send the events.

	// The channel used by the handleLoop to receive the events.
	// eventChan chan eventWrap[A, P, T, D, H]

	eventQueue eventQueueFast[A, P, T, D, H] // The queue to store the pending events.
	doneChan   chan doneInfo[A, P, T, D, H]  // The channel to receive the done events.

	reportNow chan struct{} // For test, make the reportStatLoop to report immediately.

	reportChan          chan streamStat[A, P, T, D, H]
	maxBusyPathsToTrack int // The maximum number of paths to track the busy status.
	option              Option

	isClosed atomic.Bool

	handleWg sync.WaitGroup
	reportWg sync.WaitGroup

	startTime time.Time
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
		inputQueue:          chann.NewUnlimitedChannelWithQueue[eventWrap[A, P, T, D, H]](deque.NewDeque[eventWrap[A, P, T, D, H]](128)),
		eventQueue:          newEventQueueFast(option, handler),
		reportNow:           make(chan struct{}, 1),
		reportChan:          reportChan,
		maxBusyPathsToTrack: maxBusyPathsToTrack,
		option:              option,
		startTime:           time.Now(),
	}

	if reportChan != nil {
		s.doneChan = make(chan doneInfo[A, P, T, D, H], 64)
	}

	return s
}

func (s *stream[A, P, T, D, H]) getPendingSize() int {
	return s.inputQueue.Len() + int(s.eventQueue.totalPendingLength.Load())
}

func (s *stream[A, P, T, D, H]) in() *chann.UnlimitedChannel[eventWrap[A, P, T, D, H]] {
	return s.inputQueue
}

// Start the stream.
func (s *stream[A, P, T, D, H]) start(acceptedPaths []*pathInfo[A, P, T, D, H], formerStreams ...*stream[A, P, T, D, H]) {
	if s.isClosed.Load() {
		panic("The stream has been closed.")
	}

	s.handleWg.Add(1)
	go s.handleLoop(acceptedPaths, formerStreams)

	if s.reportChan != nil {
		s.reportWg.Add(1)
		go s.reportStatLoop()
	}
}

// Close the stream and wait for all goroutines to exit.
// wait is by default true, which means to wait for the goroutines to exit.
func (s *stream[A, P, T, D, H]) close(wait ...bool) {
	if s.isClosed.CompareAndSwap(false, true) {
		s.inputQueue.Close()
	}
	if len(wait) == 0 || wait[0] {
		s.handleWg.Wait()
	}
}

func (s *stream[A, P, T, D, H]) handleLoop(acceptedPaths []*pathInfo[A, P, T, D, H], formerStreams []*stream[A, P, T, D, H]) {
	pushToPendingQueue := func(e eventWrap[A, P, T, D, H]) {
		if e.wake {
			s.eventQueue.wakePath(e.pathInfo)
		} else if e.newPath {
			s.eventQueue.initPath(e.pathInfo)
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
		if s.doneChan != nil {
			close(s.doneChan)
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
		s.eventQueue.initPath(p)
	}

	// Variables below will be used in the Loop below.
	// Declared here to avoid repeated allocation.
	var (
		eventQueueEmpty  = false
		ok               = false
		inputBuf         = make([]eventWrap[A, P, T, D, H], 0, 128)
		handleBuf        = make([]T, 0, s.option.BatchCount)
		zeroT            T
		cleanUpHandleBuf = func() {
			for i := range handleBuf {
				handleBuf[i] = zeroT
			}
			handleBuf = handleBuf[:0]
		}
		path *pathInfo[A, P, T, D, H]
		// property Property
		// lastTS   Timestamp
	)

	// For testing. Don't handle events until this wait group is done.
	if s.option.handleWait != nil {
		s.option.handleWait.Wait()
	}

	// 1. Drain the inputQueue to pendingQueue.
	// 2. Pop events from the pendingQueue and handle them.
Loop:
	for {
		if eventQueueEmpty {
			inputBuf, ok = s.inputQueue.GetMultipleNoGroupWait(inputBuf)
			if !ok {
				// The stream is closed.
				return
			}
			for _, e := range inputBuf {
				pushToPendingQueue(e)
			}
			inputBuf = inputBuf[:0]
			eventQueueEmpty = false
		} else {
			inputBuf, ok = s.inputQueue.GetMultipleNoGroupNoWait(inputBuf)
			if !ok {
				// The stream is closed.
				return
			}
			if len(inputBuf) != 0 {
				for _, e := range inputBuf {
					pushToPendingQueue(e)
				}
				inputBuf = inputBuf[:0]

				// Moving events from the inputQueue to the pendingQueue is a high priority task.
				continue
			}

			handleBuf, path = s.eventQueue.popEvents(handleBuf)
			if len(handleBuf) == 0 {
				eventQueueEmpty = true
				continue Loop
			}
			beginTime := time.Now()
			path.blocking = s.handler.Handle(path.dest, handleBuf...)
			if s.doneChan != nil {
				s.doneChan <- doneInfo[A, P, T, D, H]{pathInfo: path, handleTime: time.Since(beginTime)}
			}
			if path.blocking {
				s.eventQueue.blockPath(path)
			}
			cleanUpHandleBuf()
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
