package dynstream

import (
	"sync"
	"time"
)

type destination[T Event, D any] struct {
	path   Path
	dest   D
	stream *stream[T, D]
}

type DynamicStream[T Event, D any] struct {
	sourceChan chan T

	destinations map[Path]*destination[T, D]

	expectedLatency time.Duration
	minStream       int

	wg          sync.WaitGroup
	closeSignal chan struct{}
}

// sourceChan: the source channel to read data from
// workerGen: the worker generator to generate worker
func NewDynamicStream[T any](
	sourceChan chan T,
	idleTime time.Duration, busyTime time.Duration, maxWorker int) *DynamicStream[T] {
	return &DynamicStream[T]{
		sourceChan: sourceChan,
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
