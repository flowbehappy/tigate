package dynstream

import (
	"time"

	"github.com/flowbehappy/tigate/utils/heap"
)

// The path node order by timestamp.
type tsPathNode[A Area, P Path, T Event, D Dest] pathInfo[A, P, T, D]

func (n *tsPathNode[A, P, T, D]) SetHeapIndex(index int) {
	(*pathInfo[A, P, T, D])(n).tsHeapIndex = index
}
func (n *tsPathNode[A, P, T, D]) GetHeapIndex() int {
	return (*pathInfo[A, P, T, D])(n).tsHeapIndex
}

func (n *tsPathNode[A, P, T, D]) LessThan(other *tsPathNode[A, P, T, D]) bool {
	q1 := (*pathInfo[A, P, T, D])(n).pendingQueue
	q2 := (*pathInfo[A, P, T, D])(other).pendingQueue
	f1, _ := q1.FrontRef()
	f2, _ := q2.FrontRef()
	return f1.timestamp < f2.timestamp
}

// The path node order by queue time.
type qtPathNode[A Area, P Path, T Event, D Dest] pathInfo[A, P, T, D]

func (n *qtPathNode[A, P, T, D]) SetHeapIndex(index int) {
	(*pathInfo[A, P, T, D])(n).qtHeapIndex = index
}
func (n *qtPathNode[A, P, T, D]) GetHeapIndex() int {
	return (*pathInfo[A, P, T, D])(n).qtHeapIndex
}
func (n *qtPathNode[A, P, T, D]) LessThan(other *qtPathNode[A, P, T, D]) bool {
	q1 := (*pathInfo[A, P, T, D])(n).pendingQueue
	q2 := (*pathInfo[A, P, T, D])(other).pendingQueue
	f1, _ := q1.FrontRef()
	f2, _ := q2.FrontRef()
	return f1.queueTime.Before(f2.queueTime)
}

// A area info contains the path nodes of the area in a stream.
// Note that the instance is stream level, not global level.
type areaInfo[A Area, P Path, T Event, D Dest] struct {
	area A

	// The path bound to the area info instance.
	// Since tsHeap and qtHeap only store the paths who has pending events,
	// the pathCount could be larger than the length of the heaps.
	pathCount int

	tsHeap heap.Heap[*tsPathNode[A, P, T, D]]
	qtHeap heap.Heap[*qtPathNode[A, P, T, D]]

	qtHeapIndex int
}

func (a *areaInfo[A, P, T, D]) minQueueTime() time.Time {
	top, _ := a.qtHeap.PeekTop()
	front, _ := top.pendingQueue.FrontRef()
	return front.queueTime
}

func (a *areaInfo[A, P, T, D]) SetHeapIndex(index int) {
	a.qtHeapIndex = index
}

func (a *areaInfo[A, P, T, D]) GetHeapIndex() int {
	return a.qtHeapIndex
}

func (a *areaInfo[A, P, T, D]) LessThan(other *areaInfo[A, P, T, D]) bool {
	return a.minQueueTime().Before(other.minQueueTime())
}

type pendingQueue[A Area, P Path, T Event, D Dest] struct {
	option OptionEnhanced[A, P, T, D]

	areaMap  map[A]*areaInfo[A, P, T, D]
	areaHeap heap.Heap[*areaInfo[A, P, T, D]]

	pendingLength int
}

func newPendingQueue[A Area, P Path, T Event, D Dest](option OptionEnhanced[A, P, T, D]) pendingQueue[A, P, T, D] {
	return pendingQueue[A, P, T, D]{
		option:   option,
		areaMap:  make(map[A]*areaInfo[A, P, T, D]),
		areaHeap: heap.NewHeap[*areaInfo[A, P, T, D]](),
	}
}

func (q *pendingQueue[A, P, T, D]) updateHeapAfterUpdatePath(path *pathInfo[A, P, T, D], remove bool) {
	area := path.areaInfo

	if remove || path.pendingQueue.Length() == 0 {
		// Remove the path from heap
		area.tsHeap.Remove((*tsPathNode[A, P, T, D])(path))
		area.qtHeap.Remove((*qtPathNode[A, P, T, D])(path))

		if area.qtHeap.Len() == 0 {
			q.areaHeap.Remove(area)
		} else {
			q.areaHeap.AddOrUpdate(area)
		}
	} else {
		area.tsHeap.AddOrUpdate((*tsPathNode[A, P, T, D])(path))
		area.qtHeap.AddOrUpdate((*qtPathNode[A, P, T, D])(path))
		q.areaHeap.AddOrUpdate(area)
	}
}

func (q *pendingQueue[A, P, T, D]) addPath(path *pathInfo[A, P, T, D]) {
	area, ok := q.areaMap[path.area]
	if !ok {
		area = &areaInfo[A, P, T, D]{
			area:   path.area,
			tsHeap: heap.NewHeap[*tsPathNode[A, P, T, D]](),
			qtHeap: heap.NewHeap[*qtPathNode[A, P, T, D]](),
		}
		q.areaMap[path.area] = area
	}

	area.pathCount++
	path.areaInfo = area

	q.updateHeapAfterUpdatePath(path, false)
}

func (q *pendingQueue[A, P, T, D]) removePath(path *pathInfo[A, P, T, D]) {
	area := path.areaInfo
	area.pathCount--

	if area.pathCount == 0 {
		delete(q.areaMap, area.area)
	}

	q.updateHeapAfterUpdatePath(path, true)
	path.areaInfo = nil
}

func (q *pendingQueue[A, P, T, D]) appendEvent(event eventWrap[A, P, T, D]) {
	pi := event.pathInfo
	inc := pi.appendEvent(event, &q.option)
	if inc {
		q.pendingLength++
	}

	q.updateHeapAfterUpdatePath(pi, false)
}

func (q *pendingQueue[A, P, T, D]) popEvents(buf []T) ([]T, *pathInfo[A, P, T, D]) {
	batchSize := q.option.BatchSize
	if batchSize == 0 {
		batchSize = 1
	}
	area, ok := q.areaHeap.PeekTop()
	if !ok {
		return buf[:0], nil
	}
	top, ok := area.tsHeap.PeekTop()
	if !ok {
		panic("top is nil")
	}
	path := (*pathInfo[A, P, T, D])(top)
	var eType Type = 0
	for i := 0; i < batchSize; i++ {
		front, ok := path.pendingQueue.FrontRef()
		if !ok || (eType != 0 && eType != front.eType) {
			break
		}
		eType = front.eType
		buf = append(buf, front.event)
		path.pendingQueue.PopFront()
	}
	if len(buf) == 0 {
		panic("empty buf")
	}

	q.updateHeapAfterUpdatePath((*pathInfo[A, P, T, D])(path), false)
	q.pendingLength -= len(buf)

	return buf, path
}

func (q *pendingQueue[A, P, T, D]) blockPath(path *pathInfo[A, P, T, D]) {
	q.updateHeapAfterUpdatePath((*pathInfo[A, P, T, D])(path), true)
}

func (q *pendingQueue[A, P, T, D]) wakePath(path *pathInfo[A, P, T, D]) {
	q.updateHeapAfterUpdatePath((*pathInfo[A, P, T, D])(path), false)
}
