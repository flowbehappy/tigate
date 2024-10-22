package dynstream

import "github.com/flowbehappy/tigate/utils/heap"

type pathSizeStat[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] pathInfo[A, P, T, D, H]

func (p *pathSizeStat[A, P, T, D, H]) SetHeapIndex(index int) {
	(*pathInfo[A, P, T, D, H])(p).sizeHeapIndex = index
}

func (p *pathSizeStat[A, P, T, D, H]) GetHeapIndex() int {
	return (*pathInfo[A, P, T, D, H])(p).sizeHeapIndex
}

func (p *pathSizeStat[A, P, T, D, H]) LessThan(other *pathSizeStat[A, P, T, D, H]) bool {
	return p.pendingSize < other.pendingSize
}

// AreaStat is used to store the statistics of an area.
// It is a global level struct, not stream level.
type areaStat[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	area A

	pathSizeHeap heap.Heap[*pathSizeStat[A, P, T, D, H]]
	pendingSize  int
}

type areaStats[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	areaStats map[A]*areaStat[A, P, T, D, H]
}
