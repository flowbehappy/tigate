package dynstream

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/utils/heap"
	"go.uber.org/zap"
)

// timestampPathNode is order by timestamp.
type timestampPathNode[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] pathInfo[A, P, T, D, H]

func (n *timestampPathNode[A, P, T, D, H]) updateFrontTimestamp() {
	f, ok := n.pendingQueue.FrontRef()
	if !ok {
		n.frontTimestamp = 0
	} else {
		n.frontTimestamp = f.timestamp
	}
}

func (n *timestampPathNode[A, P, T, D, H]) SetHeapIndex(index int) {
	n.timestampHeapIndex = index
}

func (n *timestampPathNode[A, P, T, D, H]) GetHeapIndex() int {
	return n.timestampHeapIndex
}

func (n *timestampPathNode[A, P, T, D, H]) LessThan(other *timestampPathNode[A, P, T, D, H]) bool {
	return n.frontTimestamp < other.frontTimestamp
}

// queuePathNode is order by queue time.
type queuePathNode[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] pathInfo[A, P, T, D, H]

func (n *queuePathNode[A, P, T, D, H]) updateFrontQueueTime() {
	f, ok := n.pendingQueue.FrontRef()
	if !ok {
		n.frontQueueTime = time.Time{}
	} else {
		n.frontQueueTime = f.queueTime
	}
}

func (n *queuePathNode[A, P, T, D, H]) SetHeapIndex(index int) {
	n.queueTimeHeapIndex = index
}
func (n *queuePathNode[A, P, T, D, H]) GetHeapIndex() int {
	return n.queueTimeHeapIndex
}
func (n *queuePathNode[A, P, T, D, H]) LessThan(other *queuePathNode[A, P, T, D, H]) bool {
	return n.frontQueueTime.Before(other.frontQueueTime)
}

// pathSizeStat is order by pending size.
type pathSizeStat[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] pathInfo[A, P, T, D, H]

func (p *pathSizeStat[A, P, T, D, H]) SetHeapIndex(index int) {
	p.sizeHeapIndex = index
}

func (p *pathSizeStat[A, P, T, D, H]) GetHeapIndex() int {
	return p.sizeHeapIndex
}

func (p *pathSizeStat[A, P, T, D, H]) LessThan(other *pathSizeStat[A, P, T, D, H]) bool {
	// pathSizeHeap should be in descending order. That say the node with the largest pending size is the top.
	return p.pendingSize > other.pendingSize
}

// An area info contains the path nodes of the area in a stream.
// Note that the instance is stream level, not global level.
type streamAreaInfo[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	area A

	// The path bound to the area info instance.
	// Since timestampHeap and queueTimeHeap only store the paths who has pending events,
	// the pathCount could be larger than the length of the heaps.
	pathCount int

	timestampHeap *heap.Heap[*timestampPathNode[A, P, T, D, H]]
	queueTimeHeap *heap.Heap[*queuePathNode[A, P, T, D, H]]
	pathSizeHeap  *heap.Heap[*pathSizeStat[A, P, T, D, H]]

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

type eventQueue[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	option  Option
	handler H

	areaMap             map[A]*streamAreaInfo[A, P, T, D, H]
	eventQueueTimeQueue *heap.Heap[*streamAreaInfo[A, P, T, D, H]]

	totalPendingLength atomic.Int64
}

func newEventQueue[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](option Option, handler H) eventQueue[A, P, T, D, H] {
	return eventQueue[A, P, T, D, H]{
		option:              option,
		areaMap:             make(map[A]*streamAreaInfo[A, P, T, D, H]),
		eventQueueTimeQueue: heap.NewHeap[*streamAreaInfo[A, P, T, D, H]](),
		handler:             handler,
	}
}

func (q *eventQueue[A, P, T, D, H]) updateHeapAfterUpdatePath(path *pathInfo[A, P, T, D, H]) {
	area := path.streamAreaInfo
	// If the path is remove but the stream still receives its event,
	// the streamAreaInfo is nil.
	if area == nil {
		return
	}
	if path.removed || path.blocking || path.pendingQueue.Length() == 0 {
		// Remove the path from heap
		area.timestampHeap.Remove((*timestampPathNode[A, P, T, D, H])(path))
		area.queueTimeHeap.Remove((*queuePathNode[A, P, T, D, H])(path))
		area.pathSizeHeap.Remove((*pathSizeStat[A, P, T, D, H])(path))

		if area.queueTimeHeap.Len() == 0 {
			q.eventQueueTimeQueue.Remove(area)
		} else {
			q.eventQueueTimeQueue.AddOrUpdate(area)
		}
	} else {
		(*timestampPathNode[A, P, T, D, H])(path).updateFrontTimestamp()
		(*queuePathNode[A, P, T, D, H])(path).updateFrontQueueTime()

		area.timestampHeap.AddOrUpdate((*timestampPathNode[A, P, T, D, H])(path))
		area.queueTimeHeap.AddOrUpdate((*queuePathNode[A, P, T, D, H])(path))
		area.pathSizeHeap.AddOrUpdate((*pathSizeStat[A, P, T, D, H])(path))

		q.eventQueueTimeQueue.AddOrUpdate(area)
	}
}

func (q *eventQueue[A, P, T, D, H]) addPath(path *pathInfo[A, P, T, D, H]) {
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

	q.totalPendingLength.Add(int64(path.pendingQueue.Length()))
	q.updateHeapAfterUpdatePath(path)
}

func (q *eventQueue[A, P, T, D, H]) removePath(path *pathInfo[A, P, T, D, H]) {
	if area := path.streamAreaInfo; area != nil {
		area.pathCount--

		if area.pathCount == 0 {
			delete(q.areaMap, area.area)
		}

		q.totalPendingLength.Add(-int64(path.pendingQueue.Length()))
		q.updateHeapAfterUpdatePath(path)
		path.streamAreaInfo = nil
	}
	if path.areaMemStat != nil {
		path.areaMemStat.memControl.removePathFromArea(path)
	}
}

func (q *eventQueue[A, P, T, D, H]) appendEvent(event eventWrap[A, P, T, D, H]) {
	path := event.pathInfo
	if path.streamAreaInfo == nil {
		// A newly added path sends the first event.
		q.addPath(path)
	}
	// If memory control is enabled, use the memory control to append the event.
	if path.areaMemStat != nil {
		path.areaMemStat.appendEvent(path, event, q.handler, q)
		// updateHeapAfterUpdatePath is called already in areaMemStat.appendEvent
		return
	}

	// Shortcut when memory control is disabled.
	replaced := false
	if event.eventType.Property == PeriodicSignal {
		back, ok := path.pendingQueue.BackRef()
		if ok && back.eventType.Property == PeriodicSignal {
			// Replace the repeated signal.
			// Note that since the size of the repeated signal is the same, we don't need to update the pending size.
			*back = event
			replaced = true
			q.updateHeapAfterUpdatePath(path)
		}
	}
	if !replaced {
		path.pendingQueue.PushBack(event)
		q.updateHeapAfterUpdatePath(path)
		q.totalPendingLength.Add(1)
	}
}

func (q *eventQueue[A, P, T, D, H]) popEvents(buf []T) ([]T, *pathInfo[A, P, T, D, H]) {
	batchSize := q.option.BatchCount
	if batchSize == 0 {
		batchSize = 1
	}
	for {
		area, ok := q.eventQueueTimeQueue.PeekTop()
		if !ok {
			return buf[:0], nil
		}
		top, ok := area.timestampHeap.PeekTop()
		if !ok {
			panic("top is nil")
		}
		path := (*pathInfo[A, P, T, D, H])(top)
		if path.removed {
			// Remove the path from the heap.
			q.updateHeapAfterUpdatePath(path)
			continue
		} else {
			group := DefaultEventType.DataGroup
			for i := 0; i < batchSize; i++ {
				front, ok := path.pendingQueue.PopFront()
				if !ok || (group != DefaultEventType.DataGroup && group != front.eventType.DataGroup) {
					break
				}
				group = front.eventType.DataGroup
				buf = append(buf, front.event)
				path.pendingSize -= front.eventSize
				q.totalPendingLength.Add(-1)

				// Reduce the total pending size of the area.
				if path.areaMemStat != nil {
					path.areaMemStat.totalPendingSize.Add(-int64(front.eventSize))
					if front.eventType.Property != PeriodicSignal {
						log.Info("hyy pop event", zap.Any("path", path.path), zap.Any("commitTs", front.timestamp), zap.Any("event", front.event))
					}
				}

				if front.eventType.Property == NonBatchable {
					break
				}
			}
			if len(buf) == 0 {
				panic("empty buf")
			}

			q.updateHeapAfterUpdatePath((*pathInfo[A, P, T, D, H])(path))
			return buf, path
		}
	}
}

func (q *eventQueue[A, P, T, D, H]) blockPath(path *pathInfo[A, P, T, D, H]) {
	q.updateHeapAfterUpdatePath(path)
}

func (q *eventQueue[A, P, T, D, H]) wakePath(path *pathInfo[A, P, T, D, H]) {
	q.updateHeapAfterUpdatePath(path)
}
