package dynstream

import (
	"sync"
	"time"
)

type DynamicStream[T Event, D any] struct {
	sourceChan chan T

	pathMap map[Path]*pathInfo[T, D]

	expectedLatency time.Duration
	minStream       int

	wg          sync.WaitGroup
	closeSignal chan struct{}
}
