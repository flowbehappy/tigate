package dynstream

import "github.com/flowbehappy/tigate/utils/heap"

// The path node order by timestamp.
type tsPathNode[A Area, P Path, T Event, D Dest, TS Timestamp] pathInfo[A, P, T, D, TS]

// The path node order by queue time.
type qtPathNode[A Area, P Path, T Event, D Dest, TS Timestamp] pathInfo[A, P, T, D, TS]

func (n *tsPathNode[A, P, T, D, TS]) SetHeapIndex(index int) {
	(*pathInfo[A, P, T, D, TS])(n).tsHeapIndex = index
}
func (n *tsPathNode[A, P, T, D, TS]) GetHeapIndex() int {
	return (*pathInfo[A, P, T, D, TS])(n).tsHeapIndex
}

func (n *tsPathNode[A, P, T, D, TS]) LessThan(other *tsPathNode[A, P, T, D, TS]) bool {
	q1 := (*pathInfo[A, P, T, D, TS])(n).pendingQueue
	q2 := (*pathInfo[A, P, T, D, TS])(other).pendingQueue
	f1, _ := q1.FrontRef()
	f2, _ := q2.FrontRef()
	return f1.timestamp < f2.timestamp
}

func (n *qtPathNode[A, P, T, D, TS]) SetHeapIndex(index int) {
	(*pathInfo[A, P, T, D, TS])(n).qtHeapIndex = index
}
func (n *qtPathNode[A, P, T, D, TS]) GetHeapIndex() int {
	return (*pathInfo[A, P, T, D, TS])(n).qtHeapIndex
}
func (n *qtPathNode[A, P, T, D, TS]) LessThan(other *qtPathNode[A, P, T, D, TS]) bool {
	q1 := (*pathInfo[A, P, T, D, TS])(n).pendingQueue
	q2 := (*pathInfo[A, P, T, D, TS])(other).pendingQueue
	f1, _ := q1.FrontRef()
	f2, _ := q2.FrontRef()
	return f1.queueTime.Before(f2.queueTime)
}

type areaInfo[A Area, P Path, T Event, D Dest, TS Timestamp] struct {
	area   A
	tsHeap heap.Heap[*tsPathNode[A, P, T, D, TS]]
	qtHeap heap.Heap[*qtPathNode[A, P, T, D, TS]]
}

func (a *areaInfo[A, P, T, D, TS]) appendEvent(path *pathInfo[A, P, T, D, TS], event T) {

}

func (a *areaInfo[A, P, T, D, TS]) blockPath(path *pathInfo[A, P, T, D, TS]) {
}

func (a *areaInfo[A, P, T, D, TS]) wakePath(path *pathInfo[A, P, T, D, TS]) {
}
