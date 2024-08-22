package dynstream

import (
	"runtime"
	"time"
)

// The path interface. A path is a unique identifier of a destination.
type Path comparable

// An event belongs to a path.
type Event any

// A destination is the place where the event is sent to.
type Dest any

// The handler interface. The handler processes the event.
type Handler[P Path, T Event, D Dest] interface {
	// Get the path of the event. This method is called once for each event.
	Path(event T) P
	// Handle processes the event.
	// The dest is included in the argument to avoid the requirement of another mapping to get the destination.
	// If the event is processed successfully, it should return false.
	// If the event is processed asynchronously, it should return true. The later events of the path are blocked
	// until a wake signal is sent to DynamicStream's Wake channel.
	Handle(event T, dest D) (await bool)
}

type PathAndDest[P Path, D Dest] struct {
	Path P
	Dest D
}

/*
Dynamic stream is a stream that can process events with from different paths concurrently.
  - Events from the same path are processed sequentially.
  - Events from different paths are processed concurrently.

We assume that the handler is CPU-bound and should not be blocked by any waiting. Otherwise, events from other paths will be blocked.
*/
type DynamicStream[P Path, T Event, D Dest] interface {
	// Start starts the dynamic stream.
	// It should be called before any other methods.
	Start()
	// Close closes the dynamic stream.
	// No more events can be sent to or processed by the stream after it is closed.
	Close()

	In() chan<- T
	Wake() chan<- P

	// AddPath adds the paths to the dynamic stream to receive the events.
	// An event with a path not already added will be dropped.
	//
	// If some paths already exist, it will return an ErrorTypeDuplicate error. And no paths are added.
	// If the stream is closed, it will return an ErrorTypeClosed error.
	AddPath(paths ...PathAndDest[P, D]) error
	AddOnePath(path P, dest D) error

	// RemovePath removes the paths from the dynamic stream.
	// After this call return, future events with the paths will be dropped, including events which are already in the stream.
	//
	// If some paths don't exist, it will return ErrorTypeNotExist errors. But the existed paths are still removed.
	// If all paths are removed successfully, return nil.
	RemovePath(paths ...P) []error
}

const DefaultSchedulerInterval = 5 * time.Second
const DefaultReportInterval = 500 * time.Millisecond

// We don't need lots of streams because the hanle of events should be CPU-bound and should not be blocked by any waiting.
const DefaultStreamCount = 128

func NewDynamicStreamDefault[P Path, T Event, D Dest](handler Handler[P, T, D]) DynamicStream[P, T, D] {
	streamCount := max(DefaultStreamCount, runtime.NumCPU())
	return NewDynamicStream[P, T, D](handler, DefaultSchedulerInterval, DefaultReportInterval, streamCount)
}

func NewDynamicStream[P Path, T Event, D Dest](
	handler Handler[P, T, D],
	schedulerInterval time.Duration,
	reportInterval time.Duration,
	streamCount int,
) DynamicStream[P, T, D] {
	return newDynamicStreamImpl(handler, schedulerInterval, reportInterval, streamCount)
}
