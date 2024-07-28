package dynstream

import (
	"sync/atomic"
	"time"
)

// Defines the destination of the events. For example, the id of Dispatcher.
type Path string

// The event interface. An event belongs to a path.
type Event interface {
	Path() Path
	Weight() int64
}

type EventWrap[T Event, D any] struct {
	event T

	pathInfo *pathInfo[T, D]

	inQueueTime time.Time
	// Those two fields are accessed by the worker and background goroutines in the stream.
	// So they need synchronization.
	startTime atomic.Value
	doneTime  atomic.Value
}

func (ew *EventWrap[T, D]) Event() Event { return ew.event }
func (ew *EventWrap[T, D]) Path() Path   { return ew.pathInfo.path }
func (ew *EventWrap[T, D]) Dest() D      { return ew.pathInfo.dest }

type Handler[T Event, D any] interface {
	Handle(events *EventWrap[T, D])
	OnDrop(events *EventWrap[T, D])
}

type pathInfo[T Event, D any] struct {
	// Note that although this struct is used by multiple goroutines, it doesn't need synchronization because
	// 1. path & dest are immutable.
	// 2. stream updated by the main goroutine in the DynamicStream, and read by the worker goroutine after the EventWrap is passed through the channel.
	// 3. totalTime & waitLen are only accessed by the background goroutine in the stream.
	// We use one struct to store them together to avoid mapping by path in different places in many times.

	path Path
	dest D

	stream *stream[T, D]

	totalTime time.Duration // The total time to handle the events in a period of time.
	waitLen   int64         // Current waiting events count.
}
