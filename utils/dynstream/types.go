package dynstream

// Defines the destination of the events. For example, the id of Dispatcher.
type Path string

// The event interface. An event belongs to a path.
type Event interface {
	Path() Path
	Weight() int64
}

type Handler[T Event, D any] interface {
	Handle(event T, dest D)
}

type PathAndDest[D any] struct {
	Path Path
	Dest D
}
