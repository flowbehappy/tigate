package dynstream

import (
	"sync"
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
	// If the events are processed successfully, it should return false.
	// If the events are processed asynchronously, it should return true. The later events of the path are blocked
	// until a wake signal is sent to DynamicStream's Wake channel.
	// The len(events) is guaranteed to be greater than 0.
	Handle(dest D, events ...T) (await bool)
}

type DropListener[P Path, T Event, D Dest] interface {
	// OnDrop is called when an event is dropped.
	OnDrop(dest D, event T)
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

	// AddPaths adds the paths to the dynamic stream to receive the events.
	// An event with a path not already added will be dropped.
	//
	// If some paths already exist, it will return an ErrorTypeDuplicate error. And no paths are added.
	// If the stream is closed, it will return an ErrorTypeClosed error.
	AddPaths(paths ...PathAndDest[P, D]) error
	AddPath(path P, dest D) error

	// RemovePaths removes the paths from the dynamic stream.
	// After this call return, future events with the paths will be dropped, including events which are already in the stream.
	//
	// If some paths don't exist, it will return ErrorTypeNotExist errors. But the existed paths are still removed.
	// If all paths are removed successfully, return nil.
	RemovePaths(paths ...P) []error
}

const DefaultSchedulerInterval = 1 * time.Second
const DefaultReportInterval = 500 * time.Millisecond

// We don't need lots of streams because the hanle of events should be CPU-bound and should not be blocked by any waiting.
const DefaultStreamCount = 64

type DropPolicy int

const (
	// Drop the late come events of the path.
	DropLate DropPolicy = 0
	// Drop the early come events of the path.
	DropEarly DropPolicy = 1
)

type Option struct {
	SchedulerInterval time.Duration // The interval of the scheduler. The scheduler is used to balance the paths between streams.
	ReportInterval    time.Duration // The interval of reporting the status of stream, the status is used by the scheduler.
	StreamCount       int           // The count of streams. I.e. the count of goroutines to handle events.
	BatchSize         int           // The batch size of handling events. <= 1 means no batch.

	// Note that if you specify MaxPendingLength and DropPolicy, the handler can implement the DropListener interface to listen to the dropped events.
	// Otherwise the events will be dropped silently.
	MaxPendingLength int        // The max pending length of a path. <= 0 means no limit.
	DropPolicy       DropPolicy // The drop policy of the events of a path when the pending length is greater than MaxPendingLength.

	handleWait *sync.WaitGroup // For testing. Don't handle events until this wait group is done.
}

func NewOption() Option {
	return Option{
		SchedulerInterval: DefaultSchedulerInterval,
		ReportInterval:    DefaultReportInterval,
		StreamCount:       DefaultStreamCount,
		BatchSize:         1,
		MaxPendingLength:  0,
		DropPolicy:        DropLate,
	}
}

func (o *Option) fix() {
	if o.BatchSize <= 0 {
		o.BatchSize = 1
	}
	if o.MaxPendingLength < 0 {
		o.MaxPendingLength = 0
	}
}

func NewDynamicStream[P Path, T Event, D Dest](handler Handler[P, T, D], option ...Option) DynamicStream[P, T, D] {
	opt := NewOption()
	if len(option) > 0 {
		opt = option[0]
	}
	opt.fix()
	return newDynamicStreamImpl(handler, opt)
}
