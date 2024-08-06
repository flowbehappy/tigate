package dynstream

import (
	"time"

	"github.com/flowbehappy/tigate/utils/deque"
	"github.com/flowbehappy/tigate/utils/heap"
)

// Defines the destination of the events. For example, the id of Dispatcher.
type Path string

// The event interface. An event belongs to a path.
type Event interface {
	Path() Path
}

type Handler[T Event, D any] interface {
	Handle(event T, dest D)
}

type PathAndDest[D any] struct {
	Path Path
	Dest D
}

// ====== internal types ======

type pathStat[T Event, D any] struct {
	pathInfo   *pathInfo[T, D]
	totalTime  time.Duration
	count      int
	pendingLen int
	heapIndex  int
}

func (p *pathStat[T, D]) busyRatio(period time.Duration) float64 {
	if period == 0 {
		return 0
	} else {
		return float64(p.totalTime) / float64(period)
	}
}

// Implement heap.Item interface
func (p *pathStat[T, D]) SetHeapIndex(index int)          { p.heapIndex = index }
func (p *pathStat[T, D]) GetHeapIndex() int               { return p.heapIndex }
func (p *pathStat[T, D]) CompareTo(o *pathStat[T, D]) int { return int(p.totalTime - o.totalTime) } // It is safe on a 64-bit machine.

type pathInfo[T Event, D any] struct {
	// Note that although this struct is used by multiple goroutines, it doesn't need synchronization because
	// different fields are either immutable or accessed by different goroutines.
	// We use one struct to store them together to avoid mapping by path in different places in many times.

	path Path
	dest D

	stream *stream[T, D]

	pendingQueue *deque.Deque[T]
	blocking     bool

	reportRound int64
	pathStat    *pathStat[T, D]
}

func newPathInfo[T Event, D any](path Path, dest D) *pathInfo[T, D] {
	return &pathInfo[T, D]{
		path:         path,
		dest:         dest,
		pendingQueue: deque.NewDequeDefault[T](),
	}
}

func (pi *pathInfo[T, D]) resetStat() {
	pi.pathStat = &pathStat[T, D]{pathInfo: pi}
}

type streamStat[T Event, D any] struct {
	id int

	period    time.Duration
	totalTime time.Duration
	count     int

	pendingLen int

	mostBusyPath heap.Heap[*pathStat[T, D]]
}

func tryAddPathToBusyHeap[T Event, D any](heap heap.Heap[*pathStat[T, D]], pi *pathStat[T, D], trackTop int) {
	if heap.Len() < trackTop {
		heap.AddOrUpdate(pi)
	} else if top, _ := heap.PeekTop(); top.CompareTo(pi) < 0 {
		heap.PopTop()
		heap.AddOrUpdate(pi)
	}
}
