package dynstream

import (
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/utils/deque"
)

type eventSignal[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	pathInfo   *pathInfo[A, P, T, D, H]
	eventCount int
}

type eventQueueFast[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	option  Option
	handler H

	// Used to reduce the block allocation in the paths' pending queue.
	eventBlockAlloc *deque.BlockAllocator[eventWrap[A, P, T, D, H]]

	signalQueue        *deque.Deque[eventSignal[A, P, T, D, H]]
	totalPendingLength atomic.Int64 // The total signal count in the queue.
}

func newEventQueueFast[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](option Option, handler H) eventQueueFast[A, P, T, D, H] {
	blockAlloc := deque.NewBlockAllocator[eventSignal[A, P, T, D, H]](1024, 32)
	return eventQueueFast[A, P, T, D, H]{
		option:          option,
		handler:         handler,
		eventBlockAlloc: deque.NewBlockAllocator[eventWrap[A, P, T, D, H]](32, 1024),
		signalQueue:     deque.NewDeque[eventSignal[A, P, T, D, H]](1024, blockAlloc),
	}
}

func (q *eventQueueFast[A, P, T, D, H]) initPath(path *pathInfo[A, P, T, D, H]) {
	path.pendingQueue.SetBlockAllocator(q.eventBlockAlloc)
	if len := path.pendingQueue.Length(); len > 0 {
		q.signalQueue.PushBack(eventSignal[A, P, T, D, H]{pathInfo: path, eventCount: len})
		q.totalPendingLength.Add(int64(len))
	}
}

func (q *eventQueueFast[A, P, T, D, H]) removePath(path *pathInfo[A, P, T, D, H]) {
	// Do nothing here. Instead, we remove the path when the path is popped.
}

func (q *eventQueueFast[A, P, T, D, H]) appendEvent(event eventWrap[A, P, T, D, H]) {
	path := event.pathInfo

	addSignal := func() {
		back, ok := q.signalQueue.BackRef()
		if ok && back.pathInfo == path {
			back.eventCount++
		} else {
			q.signalQueue.PushBack(eventSignal[A, P, T, D, H]{pathInfo: path, eventCount: 1})
		}
		q.totalPendingLength.Add(1)
	}

	if event.eventType.Property == PeriodicSignal {
		back, ok := path.pendingQueue.BackRef()
		if !ok || back.eventType.Property != PeriodicSignal {
			path.pendingQueue.PushBack(event)
			addSignal()
		} else {
			// If the last event is a periodic signal, we only need to keep the latest one.
			// And we don't need to add a new signal.
			*back = event
		}
		// Don't count the size of periodic signals
	} else {
		path.pendingQueue.PushBack(event)
		path.pendingSize += event.eventSize

		addSignal()
	}
}

func (q *eventQueueFast[A, P, T, D, H]) blockPath(path *pathInfo[A, P, T, D, H]) {
	path.blocking = false
}

func (q *eventQueueFast[A, P, T, D, H]) wakePath(path *pathInfo[A, P, T, D, H]) {
	path.blocking = false
	count := path.pendingQueue.Length()
	if count > 0 {
		q.signalQueue.PushFront(eventSignal[A, P, T, D, H]{pathInfo: path, eventCount: count})
		q.totalPendingLength.Add(int64(count))
	}
}

func (q *eventQueueFast[A, P, T, D, H]) popEvents(buf []T) ([]T, *pathInfo[A, P, T, D, H]) {
	// Append the event to the buffer
	appendToBuf := func(event *eventWrap[A, P, T, D, H], path *pathInfo[A, P, T, D, H]) {
		buf = append(buf, event.event)

		path.pendingQueue.PopFront()
		path.pendingSize -= event.eventSize
	}

	for {
		signal, ok := q.signalQueue.FrontRef() // We are going to update the signal directly, so we need the reference.
		if !ok {
			return buf, nil
		}

		path := signal.pathInfo
		pendingQueue := path.pendingQueue

		if signal.eventCount == 0 {
			panic("signal event count is zero")
		}
		if path.blocking || path.removed {
			// The path is blocking or removed, we should ignore the signal completely.
			q.signalQueue.PopFront()
			q.totalPendingLength.Add(-int64(signal.eventCount))
			continue
		}

		batchSize := min(signal.eventCount, q.option.BatchCount)

		firstEvent, ok := pendingQueue.FrontRef()
		if !ok {
			log.Panic("firstEvent is nil, it should not happen")
		}
		firstGroup := firstEvent.eventType.DataGroup
		appendToBuf(firstEvent, path)

		// Try to batch events with the same data group.
		count := 1
		for ; count < batchSize; count++ {
			// Get the reference of the front event of the path.
			// We don't use PopFront here because we need to keep the event in the path.
			// Otherwise, the event may lost when the loop is break below.
			front, ok := pendingQueue.FrontRef()
			// Only batch events with the same data group and when the event is batchable.
			if !ok ||
				(firstGroup != front.eventType.DataGroup) ||
				front.eventType.Property == NonBatchable {
				break
			}
			appendToBuf(front, path)
		}

		signal.eventCount -= count
		if signal.eventCount == 0 {
			q.signalQueue.PopFront()
		}
		q.totalPendingLength.Add(-int64(count))

		return buf, path
	}
}
