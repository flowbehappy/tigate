package dynstream

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	checkLastSize = 10
)

type Event interface {
	HandlerId() string
}

type Handler[T Event] interface {
	Handle(events []*EventStat[T])
}

type Batcher[T Event] interface {
	IsBatch(batch []*EventStat[T], next *EventStat[T]) bool
}

type handlerStat[T Event, H Handler[T]] struct {
	handler H
	latest  []BoundedRingBuffer[time.Duration]
}

type EventStat[T Event] struct {
	Event T

	InQueueTime time.Time
	HandleTime  time.Time
	DoneTime    time.Time
}

type stream[T Event] struct {
	worker  Handler[T]
	batcher Batcher[T]

	statMap map[string]*handlerStat[T, Handler[T]]

	inChan     chan *EventStat[T]
	handleChan chan []*EventStat[T]

	batch []*EventStat[T]

	ready atomic.Bool

	formerStreams []*(stream[T])

	remainingEvents []T

	closeSignal chan struct{}
	hasDone     sync.WaitGroup
}

// bufSize: 102400
func newStream[T Event](worker Handler[T], batcher Batcher[T], acceptedIds []string, bufSize int, formerStreams ...*stream[T]) *stream[T] {
	s := &stream[T]{
		worker:        worker,
		batcher:       batcher,
		statMap:       make(map[string]*handlerStat[T, Handler[T]], len(acceptedIds)),
		inChan:        make(chan *EventStat[T], bufSize),
		handleChan:    make(chan []*EventStat[T]), // No need to buffer this channel.
		formerStreams: formerStreams,
		closeSignal:   make(chan struct{})}

	for _, id := range acceptedIds {
		s.statMap[id] = &handlerStat[T, Handler[T]]{
			handler: worker,
			latest:  make([]BoundedRingBuffer[time.Duration], checkLastSize),
		}
	}
	return s
}

func (s *stream[T]) readyChan() chan T {
	if !s.ready.Load() {
		return nil
	}
	return s.inChan
}

func (s *stream[T]) wait() ([]T, chan T) {
	s.hasDone.Wait()
	return s.remainingEvents, s.inChan
}

func (s *stream[T]) close() {
	close(s.closeSignal)
}

func (s *stream[T]) start() {
	s.hasDone.Add(1)

	// Start worker to handle events.
	go func() {
		for {
			events, ok := <-s.handleChan
			if !ok {
				break
			}

			s.worker.Handle(events)

			now := time.Now()
			for _, e := range events {
				e.DoneTime = now
			}
		}
	}()

	go func() {
		for {
			select {
			case e, ok := <-s.inChan:
				if !ok {
					close(s.handleChan)
					return
				}
				if s.batcher.IsBatch(s.batch, e) {
					s.batch = append(s.batch, e)
					continue
				} else {
					s.handleChan <- s.batch

					// We set the handle time in the maintaining goroutine to avoid
					// the value being changed by another goroutine, i.e. the worker.
					now := time.Now()
					for _, e := range s.batch {
						e.HandleTime = now
					}

					s.batch = []*EventStat[T]{e}
				}
			}
		}
	}()

	// Close and wait for the former streams to finish, and move the remaining events to the inChan.
	// If the close signal is received, close the inChan and wait for the worker to finish.
	go func() {
		for i := 0; i < len(s.formerStreams); i++ {
			fs := s.formerStreams[i]
			fs.close()
		}
		buf := make([]T, 0, len(s.inChan))
		// Move the former remaining events from former streams to buffer
		for i := 0; i < len(s.formerStreams); i++ {
			fs := s.formerStreams[i]
			lastEvents, c := fs.wait()
			buf = append(buf, lastEvents...)
			for e := range c {
				buf = append(buf, e)
			}
		}

		beforeClose := func(buf []T) {
			close(s.inChan)

			// Drain the remaining events in the inChan.
			remains := make([]T, 0, 1+len(s.inChan)+len(buf))

			if e := <-workerRemain; e.b {
				remains = append(remains, e.e)
			}
			for e := range s.inChan {
				remains = append(remains, e)
			}
			// Move the events in buf to unhandled.
			remains = append(remains, buf...)
			s.remainingEvents = remains

			s.hasDone.Done()
		}

		// Push the remaining events from former streams to the inChan.
		for i, e := range buf {
			select {
			case s.inChan <- e:
			case <-s.closeSignal:
				beforeClose(buf[i:])
				return
			}
		}

		// Clear the buffer to release memory.
		buf = nil

		// Everything is good, ready to accept new events.
		s.ready.Store(true)

		// Wait for the close signal.
		<-s.closeSignal

		beforeClose(nil)
	}()
}

type DynamicStream[T any] struct {
	sourceChan chan T

	workerGen WorkerGenerator[T]
	idleTime  time.Duration
	busyTime  time.Duration
	maxWorker int

	wg          sync.WaitGroup
	closeSignal chan struct{}
}

// sourceChan: the source channel to read data from
// workerGen: the worker generator to generate worker
func NewDynamicStream[T any](
	sourceChan chan T, workerGen WorkerGenerator[T],
	idleTime time.Duration, busyTime time.Duration, maxWorker int) *DynamicStream[T] {
	return &DynamicStream[T]{
		sourceChan: sourceChan,
		workerGen:  workerGen,
		idleTime:   idleTime,
		busyTime:   busyTime,
		maxWorker:  maxWorker,
	}
}

func (ds *DynamicStream[T]) Start() {
	ds.wg.Add(1)
	go ds.loop()
}

func (ds *DynamicStream[T]) Stop() {
	select {
	case ds.closeSignal <- struct{}{}:
	default:
		break
	}
	ds.wg.Wait()
}

func (ds *DynamicStream[T]) loop() {
	defer ds.wg.Done()
}
