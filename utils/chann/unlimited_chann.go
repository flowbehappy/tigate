package chann

import (
	"sync"

	"github.com/pingcap/ticdc/utils/deque"
)

// UnlimitedChannel is a channel with unlimited buffer.
// It is safe for concurrent use.
// It supports get multiple elements at once, which is suitable for batch processing.
type UnlimitedChannel[T any] struct {
	queue deque.Deque[T]

	mu     sync.RWMutex
	cond   *sync.Cond
	closed bool
}

func NewUnlimitedChannel[T any]() *UnlimitedChannel[T] {
	ch := &UnlimitedChannel[T]{
		queue: *deque.NewDequeDefault[T](),
	}
	ch.cond = sync.NewCond(&ch.mu)
	return ch
}

func (c *UnlimitedChannel[T]) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.cond.Broadcast()
}

func (c *UnlimitedChannel[T]) Push(v T) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		panic("push to closed ulimited channel")
	}

	c.queue.PushBack(v)
	c.cond.Signal()
}

// Get retrieves an element from the channel.
// Return the element and a boolean indicating whether the channel is available.
// Return false if the channel is closed.
func (c *UnlimitedChannel[T]) Get() (T, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for !c.closed && c.queue.Length() == 0 {
		c.cond.Wait()
	}
	var zero T
	if c.closed && c.queue.Length() == 0 {
		return zero, false
	}

	v, ok := c.queue.PopFront()
	if !ok {
		panic("unreachable")
	}

	return v, true
}

// Get retrieves up to cap(buffer) elements from the channel and stores them in buffer.
// Return the original buffer and a boolean indicating whether the channel is available.
// Return false if the channel is closed.
func (c *UnlimitedChannel[T]) GetMultiple(buffer []T) ([]T, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for !c.closed && c.queue.Length() == 0 {
		c.cond.Wait()
	}

	if c.closed && c.queue.Length() == 0 {
		return buffer, false
	}

	if c.queue.Length() == 0 {
		panic("unreachable")
	}

	cap := cap(buffer)
	for {
		if len(buffer) == cap {
			break
		}
		v, ok := c.queue.PopFront()
		if !ok {
			break
		}
		buffer = append(buffer, v)
	}

	return buffer, true
}

func (c *UnlimitedChannel[T]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.queue.Length()
}
