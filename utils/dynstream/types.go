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

	path Path
	dest D

	inQueueTime time.Time

	pathStat *pathStat[D]
}

func (ew *EventWrap[T, D]) Event() Event { return ew.event }
func (ew *EventWrap[T, D]) Path() Path   { return ew.path }
func (ew *EventWrap[T, D]) Dest() D      { return ew.dest }

type Handler[T Event, D any] interface {
	Handle(events []*EventWrap[T, D])
	Drop(events []*EventWrap[T, D])
}

type Batcher[T Event, D any] interface {
	IsBatch(batch []*EventWrap[T, D], next *EventWrap[T, D]) bool
}

type pathStat[D any] struct {
	path Path
	dest D

	totalTime time.Duration // The total time to handle the events in a period of time.
	waitCount int64         // Current waiting events count.
}
