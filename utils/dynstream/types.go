package dynstream

import "time"

// Defines the destination of the events. For example, the id of Dispatcher.
type Path string

// The event interface. An event belongs to a path.
type Event interface {
	Path() Path
	Weight() int64
}

type EventWrap[T Event, D any] struct {
	event T

	inQueueTime time.Time

	pathInfo *pathInfo[T, D]
}

func (ew *EventWrap[T, D]) Event() Event { return ew.event }
func (ew *EventWrap[T, D]) Path() Path   { return ew.pathInfo.path }
func (ew *EventWrap[T, D]) Dest() D      { return ew.pathInfo.dest }

type Handler[T Event, D any] interface {
	Handle(events []*EventWrap[T, D])
	Drop(events []*EventWrap[T, D])
}

type Batcher[T Event, D any] interface {
	IsBatch(batch []*EventWrap[T, D], next *EventWrap[T, D]) bool
}

type pathInfo[T Event, D any] struct {
	// Note that although this struct is used by multiple goroutines, it doesn't need synchronization because
	// 1. path & dest are immutable.
	// 2. stream is only accessed by the main goroutine in the DynamicStream.
	// 3. totalTime & waitLen are only accessed by the background goroutine in the stream.
	// We use one struct to store them together to avoid mapping by path in different places in many times.

	path Path
	dest D

	stream *stream[T, D]

	totalTime time.Duration // The total time to handle the events in a period of time.
	waitLen   int64         // Current waiting events count.
}
