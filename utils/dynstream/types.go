package dynstream

import (
	"time"

	"github.com/flowbehappy/tigate/utils/deque"
	"github.com/flowbehappy/tigate/utils/heap"
)

// The path interface. A path is a unique identifier of a destination.
type Path comparable

// The event interface. An event belongs to a path.
type Event[P Path] interface {
	Path() P
}

type Handler[P Path, T Event[P], D any] interface {
	Handle(event T, dest D)
}

type PathAndDest[P Path, D any] struct {
	Path P
	Dest D
}

// ====== internal types ======

type pathStat[P Path, T Event[P], D any] struct {
	pathInfo  *pathInfo[P, T, D]
	totalTime time.Duration
	count     int
	heapIndex int
}

func (p *pathStat[P, T, D]) busyRatio(period time.Duration) float64 {
	if period == 0 {
		return 0
	} else {
		return float64(p.totalTime) / float64(period)
	}
}

// Implement heap.Item interface
func (p *pathStat[P, T, D]) SetHeapIndex(index int) { p.heapIndex = index }
func (p *pathStat[P, T, D]) GetHeapIndex() int      { return p.heapIndex }
func (p *pathStat[P, T, D]) CompareTo(o *pathStat[P, T, D]) int {
	return int(p.totalTime - o.totalTime)
} // It is safe on a 64-bit machine.

type pathInfo[P Path, T Event[P], D any] struct {
	// Note that although this struct is used by multiple goroutines, it doesn't need synchronization because
	// different fields are either immutable or accessed by different goroutines.
	// We use one struct to store them together to avoid mapping by path in different places in many times.

	path P
	dest D

	stream *stream[P, T, D]

	pendingQueue *deque.Deque[T]
	blocking     bool

	reportRound int64
	pathStat    *pathStat[P, T, D]
}

func newPathInfo[P Path, T Event[P], D any](path P, dest D) *pathInfo[P, T, D] {
	pi := &pathInfo[P, T, D]{
		path:         path,
		dest:         dest,
		pendingQueue: deque.NewDeque[T](32, 0),
	}
	pi.resetStat()
	return pi
}

func (pi *pathInfo[P, T, D]) resetStat() {
	pi.pathStat = &pathStat[P, T, D]{pathInfo: pi}
}

type streamStat[P Path, T Event[P], D any] struct {
	id int

	period    time.Duration
	totalTime time.Duration
	count     int

	pendingLen int

	mostBusyPath heap.Heap[*pathStat[P, T, D]]
}

func tryAddPathToBusyHeap[P Path, T Event[P], D any](heap heap.Heap[*pathStat[P, T, D]], pi *pathStat[P, T, D], trackTop int) {
	if heap.Len() < trackTop {
		heap.AddOrUpdate(pi)
	} else if top, _ := heap.PeekTop(); top.CompareTo(pi) < 0 {
		heap.PopTop()
		heap.AddOrUpdate(pi)
	}
}
