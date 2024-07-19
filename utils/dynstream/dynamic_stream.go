package dynstream

import (
	"sync"
	"sync/atomic"
	"time"
)

type WorkerGenerator[T any] interface {
	GenWorker() Worker[T]
}

type Worker[T any] interface {
	// The worker pull events from the inChan and process it.
	// If the closeSignal is received, the worker should stop.
	// The worker should return the unhandled event if any.
	Handle(inChan <-chan T, closeSignal <-chan struct{}) (last T, has bool)
}

type stream[T any] struct {
	worker Worker[T]

	inChan chan T

	ready atomic.Bool

	formerStreams []*(stream[T])

	remainingEvents []T

	closeSignal chan struct{}
	hasDone     sync.WaitGroup
}

func newStream[T any](worker Worker[T], bufSize int, formerStreams ...*stream[T]) *stream[T] {
	s := &stream[T]{
		worker:        worker,
		inChan:        make(chan T, bufSize),
		formerStreams: formerStreams,
		closeSignal:   make(chan struct{})}
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

	type EventAndBool struct {
		e T
		b bool
	}

	workerRemain := make(chan EventAndBool)
	// Start worker to handle events.
	go func() {
		e, b := s.worker.Handle(s.inChan, s.closeSignal)
		workerRemain <- EventAndBool{e, b}
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

			if e, ok := <-workerRemain; ok {
				remains = append(remains, e)
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
