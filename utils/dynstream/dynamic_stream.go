package dynstream

import (
	"sync"
	"time"
)

type WorkerGenerator[T any] interface {
	GenWorker() Worker[T]
}

type Worker[T any] interface {
	HandleEvent(inChan chan T, outChan <-chan T)
}

type DynamicStream[T any] struct {
	sourceChan chan T

	workerGen WorkerGenerator[T]
	idleTime  time.Duration
	busyTime  time.Duration
	maxWorker int

	wg         sync.WaitGroup
	stopSignal chan struct{}
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
	case ds.stopSignal <- struct{}{}:
	default:
		break
	}
	ds.wg.Wait()
}

func (ds *DynamicStream[T]) loop() {
	defer ds.wg.Done()
    for {
		select
	}
}
