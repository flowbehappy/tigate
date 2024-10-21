package dynstream

import (
	"runtime"
	"sync"
	"time"
)

// The path interface. A path is a unique identifier of a destination.
type Path comparable

// A path can only belong to an area. An area is a group of paths.
// Area is normally a GID
type Area comparable

// The timestamp an event carries. E.g. the commit TS of a DML.
// Normally, events with smaller timestamps are processed first amoung the same Area, but it is not guaranteed.
// In a path, events come earlier should have smaller timestamps. DynamicStream will not check the
// order of the timestamps, it is the handler's responsibility to handle the events in the correct order.
type Timestamp uint64

// An event belongs to a path.
type Event any

// A destination is the place where the event is sent to.
type Dest any

// The type of the event. The type is used to group the events for the handler to process.
// Events with different types will not be processed in a group by the handler.
type EventType int

const (
	// Events are sent repeatedly, and don't carry any data except indicating something happens.
	// DynamicStream could drop the eary come repeated signals to reduce the load. E.g. the resolved TS.
	RepeatedSignal EventType = iota
	// E.g. the DMLs
	DataType1
	// E.g. the DDLs
	DataType2
)

// The handler interface. The handler processes the event.
type Handler[A Area, P Path, T Event, D Dest] interface {
	// Get the path of the event. This method is called once for each event.
	Path(event T) P
	// Handle processes the event.
	// The dest is included in the argument to avoid the requirement of another mapping to get the destination.
	// If the events are processed successfully, it should return false.
	// If the events are processed asynchronously, it should return true. The later events of the path are blocked
	// until a wake signal is sent to DynamicStream's Wake channel.
	// The len(events) is guaranteed to be greater than 0.
	Handle(dest D, events ...T) (await bool)

	// The methods below are optional.

	// Get the size of the event. This method is called once for each event.
	// Return 0 by default implementation, if the size is not used.
	GetSize(event T) int
	// Get the area of the path. This method is called once for each path.
	// Return zero by default implementation. I.e. all paths are in the default area.
	GetArea(path P) A
	// Get the timestamp of the event. This method is called once for each event.
	// Events are processed in the order of the timestamps.
	// Return zero by default implementation. In this case, the events are processed
	// in the order of the arrival.
	GetTimestamp(event T) Timestamp
	// Get the timestamp of the event. This method is called once for each event.
	// Return zero by default implementation. I.e. all events are in the same type.
	GetType(event T) EventType
	// OnDrop is called when an event is dropped. Could be caused by the DropPolicy or cannot find the path.
	// Do nothing by default implementation.
	OnDrop(event T)
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
type DynamicStream[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] interface {
	// Start starts the dynamic stream.
	// It should be called before any other methods.
	Start()
	// Close closes the dynamic stream.
	// No more events can be sent to or processed by the stream after it is closed.
	Close()

	// In returns the channel to send events into the dynamic stream.
	In() chan<- T
	// Wake returns the channel to mark the the path as ready to process the next event.
	Wake() chan<- P

	// AddPaths adds the paths to the dynamic stream to receive the events.
	// An event with a path not already added will be dropped.
	//
	// If some paths already exist, it will return ErrorTypeDuplicate errors. But the non-existed paths are still added.
	// If all paths are added successfully, return nil.
	AddPaths(paths ...PathAndDest[P, D]) []error
	AddPath(path P, dest D) error

	// RemovePaths removes the paths from the dynamic stream.
	// After this call return, future events with the paths will be dropped, including events which are already in the stream.
	//
	// If some paths don't exist, it will return ErrorTypeNotExist errors. But the existed paths are still removed.
	// If all paths are removed successfully, return nil.
	RemovePaths(paths ...P) []error
	RemovePath(path P) error
}

const DefaultSchedulerInterval = 1 * time.Second
const DefaultReportInterval = 500 * time.Millisecond

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
	StreamCount       int           // The count of streams. I.e. the count of goroutines to handle events. By default 0, means runtime.NumCPU().
	BatchCount        int           // The batch size of handling events. <= 1 means no batch.

	MaxPendingLength int        // The max pending length of a path. <= 0 means no limit.
	DropPolicy       DropPolicy // The drop policy of the events of a path when the pending length is greater than MaxPendingLength.

	handleWait *sync.WaitGroup // For testing. Don't handle events until this wait group is done.
}

func NewOption() Option {
	return Option{
		SchedulerInterval: DefaultSchedulerInterval,
		ReportInterval:    DefaultReportInterval,
		StreamCount:       0,
		BatchCount:        1,
	}
}

func (o *Option) fix() {
	if o.StreamCount == 0 {
		o.StreamCount = runtime.NumCPU()
	}
	if o.BatchCount <= 0 {
		o.BatchCount = 1
	}
	if o.MaxPendingLength < 0 {
		o.MaxPendingLength = 0
	}
}

func NewDynamicStream[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](handler H, option ...Option) DynamicStream[A, P, T, D, H] {
	opt := NewOption()
	if len(option) > 0 {
		opt = option[0]
	}
	return newDynamicStreamImpl(handler, opt)
}
