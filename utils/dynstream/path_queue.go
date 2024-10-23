package dynstream

import (
	"time"

	"github.com/flowbehappy/tigate/utils/heap"
)

// The path node order by timestamp.
type timestampPathNode[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] pathInfo[A, P, T, D, H]

func (n *timestampPathNode[A, P, T, D, H]) SetHeapIndex(index int) {
	(*pathInfo[A, P, T, D, H])(n).timestampHeapIndex = index
}
func (n *timestampPathNode[A, P, T, D, H]) GetHeapIndex() int {
	return (*pathInfo[A, P, T, D, H])(n).timestampHeapIndex
}

func (n *timestampPathNode[A, P, T, D, H]) LessThan(other *timestampPathNode[A, P, T, D, H]) bool {
	q1 := (*pathInfo[A, P, T, D, H])(n).pendingQueue
	q2 := (*pathInfo[A, P, T, D, H])(other).pendingQueue
	f1, _ := q1.FrontRef()
	f2, _ := q2.FrontRef()
	return f1.timestamp < f2.timestamp
}

// The path node order by queue time.
type queuePathNode[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] pathInfo[A, P, T, D, H]

func (n *queuePathNode[A, P, T, D, H]) SetHeapIndex(index int) {
	(*pathInfo[A, P, T, D, H])(n).queueTimeHeapIndex = index
}
func (n *queuePathNode[A, P, T, D, H]) GetHeapIndex() int {
	return (*pathInfo[A, P, T, D, H])(n).queueTimeHeapIndex
}
func (n *queuePathNode[A, P, T, D, H]) LessThan(other *queuePathNode[A, P, T, D, H]) bool {
	q1 := (*pathInfo[A, P, T, D, H])(n).pendingQueue
	q2 := (*pathInfo[A, P, T, D, H])(other).pendingQueue
	f1, _ := q1.FrontRef()
	f2, _ := q2.FrontRef()
	return f1.queueTime.Before(f2.queueTime)
}

type pathSizeStat[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] pathInfo[A, P, T, D, H]

func (p *pathSizeStat[A, P, T, D, H]) SetHeapIndex(index int) {
	(*pathInfo[A, P, T, D, H])(p).sizeHeapIndex = index
}

func (p *pathSizeStat[A, P, T, D, H]) GetHeapIndex() int {
	return (*pathInfo[A, P, T, D, H])(p).sizeHeapIndex
}

func (p *pathSizeStat[A, P, T, D, H]) LessThan(other *pathSizeStat[A, P, T, D, H]) bool {
	// The heap is in descending order.
	return p.pendingSize > other.pendingSize
}

// A area info contains the path nodes of the area in a stream.
// Note that the instance is stream level, not global level.
type streamAreaInfo[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	area A

	// The path bound to the area info instance.
	// Since timestampHeap and queueTimeHeap only store the paths who has pending events,
	// the pathCount could be larger than the length of the heaps.
	pathCount int

	timestampHeap heap.Heap[*timestampPathNode[A, P, T, D, H]]
	queueTimeHeap heap.Heap[*queuePathNode[A, P, T, D, H]]
	pathSizeHeap  heap.Heap[*pathSizeStat[A, P, T, D, H]]

	queueTimeHeapIndex int
}

func (a *streamAreaInfo[A, P, T, D, H]) minQueueTime() time.Time {
	top, _ := a.queueTimeHeap.PeekTop()
	front, _ := top.pendingQueue.FrontRef()
	return front.queueTime
}

func (a *streamAreaInfo[A, P, T, D, H]) SetHeapIndex(index int) {
	a.queueTimeHeapIndex = index
}

func (a *streamAreaInfo[A, P, T, D, H]) GetHeapIndex() int {
	return a.queueTimeHeapIndex
}

func (a *streamAreaInfo[A, P, T, D, H]) LessThan(other *streamAreaInfo[A, P, T, D, H]) bool {
	return a.minQueueTime().Before(other.minQueueTime())
}

type pathQueue[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	option  Option
	handler H

	areaMap  map[A]*streamAreaInfo[A, P, T, D, H]
	areaHeap heap.Heap[*streamAreaInfo[A, P, T, D, H]]

	totalPendingLength int
}

func newPathQueue[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](option Option, handler H) pathQueue[A, P, T, D, H] {
	return pathQueue[A, P, T, D, H]{
		option:   option,
		areaMap:  make(map[A]*streamAreaInfo[A, P, T, D, H]),
		areaHeap: heap.NewHeap[*streamAreaInfo[A, P, T, D, H]](),
	}
}

func (q *pathQueue[A, P, T, D, H]) updateHeapAfterUpdatePath(path *pathInfo[A, P, T, D, H]) {
	area := path.streamAreaInfo

	if path.removed || path.blocking || path.pendingQueue.Length() == 0 {
		// Remove the path from heap
		area.timestampHeap.Remove((*timestampPathNode[A, P, T, D, H])(path))
		area.queueTimeHeap.Remove((*queuePathNode[A, P, T, D, H])(path))
		area.pathSizeHeap.Remove((*pathSizeStat[A, P, T, D, H])(path))

		if area.queueTimeHeap.Len() == 0 {
			q.areaHeap.Remove(area)
		} else {
			q.areaHeap.AddOrUpdate(area)
		}
	} else {
		area.timestampHeap.AddOrUpdate((*timestampPathNode[A, P, T, D, H])(path))
		area.queueTimeHeap.AddOrUpdate((*queuePathNode[A, P, T, D, H])(path))
		area.pathSizeHeap.AddOrUpdate((*pathSizeStat[A, P, T, D, H])(path))

		q.areaHeap.AddOrUpdate(area)
	}
}

func (q *pathQueue[A, P, T, D, H]) addPath(path *pathInfo[A, P, T, D, H]) {
	area, ok := q.areaMap[path.area]
	if !ok {
		area = &streamAreaInfo[A, P, T, D, H]{
			area:          path.area,
			timestampHeap: heap.NewHeap[*timestampPathNode[A, P, T, D, H]](),
			queueTimeHeap: heap.NewHeap[*queuePathNode[A, P, T, D, H]](),
			pathSizeHeap:  heap.NewHeap[*pathSizeStat[A, P, T, D, H]](),
		}
		q.areaMap[path.area] = area
	}

	area.pathCount++

	path.streamAreaInfo = area
	path.queueTimeHeapIndex = 0
	path.timestampHeapIndex = 0
	path.sizeHeapIndex = 0

	q.totalPendingLength += path.pendingQueue.Length()
	q.updateHeapAfterUpdatePath(path)
}

func (q *pathQueue[A, P, T, D, H]) removePath(path *pathInfo[A, P, T, D, H]) {
	area := path.streamAreaInfo
	if area == nil {
		return
	}
	area.pathCount--

	if area.pathCount == 0 {
		delete(q.areaMap, area.area)
	}

	q.totalPendingLength -= path.pendingQueue.Length()
	q.updateHeapAfterUpdatePath(path)
	path.streamAreaInfo = nil

	if path.areaMemStat != nil {
		path.areaMemStat.memControl.removePathFromArea(path)
	}
}

func (q *pathQueue[A, P, T, D, H]) appendEvent(event eventWrap[A, P, T, D, H]) {
	path := event.pathInfo
	if path.streamAreaInfo == nil {
		// A newly added path sends the first event.
		q.addPath(path)
	}

	if path.areaMemStat != nil {
		// updateHeapAfterUpdatePath is called already in areaMemStat.appendEvent
		path.areaMemStat.appendEvent(path, event, q.handler, q)
	}
}

func (q *pathQueue[A, P, T, D, H]) popEvents(buf []T) ([]T, *pathInfo[A, P, T, D, H]) {
	batchSize := q.option.BatchCount
	if batchSize == 0 {
		batchSize = 1
	}
	for {
		area, ok := q.areaHeap.PeekTop()
		if !ok {
			return buf[:0], nil
		}
		top, ok := area.timestampHeap.PeekTop()
		if !ok {
			panic("top is nil")
		}
		path := (*pathInfo[A, P, T, D, H])(top)
		if path.removed {
			q.updateHeapAfterUpdatePath(path)

			continue
		} else {
			var eType EventType = 0
			for i := 0; i < batchSize; i++ {
				front, ok := path.pendingQueue.FrontRef()
				if !ok || (eType != 0 && eType != front.eventType) {
					break
				}
				eType = front.eventType
				buf = append(buf, front.event)
				path.pendingQueue.PopFront()
				path.pendingSize -= front.eventSize
			}
			if len(buf) == 0 {
				panic("empty buf")
			}

			q.totalPendingLength -= len(buf)
			q.updateHeapAfterUpdatePath((*pathInfo[A, P, T, D, H])(path))
			return buf, path
		}
	}
}

func (q *pathQueue[A, P, T, D, H]) blockPath(path *pathInfo[A, P, T, D, H]) {
	q.updateHeapAfterUpdatePath((*pathInfo[A, P, T, D, H])(path))
}

func (q *pathQueue[A, P, T, D, H]) wakePath(path *pathInfo[A, P, T, D, H]) {
	q.updateHeapAfterUpdatePath((*pathInfo[A, P, T, D, H])(path))
}
