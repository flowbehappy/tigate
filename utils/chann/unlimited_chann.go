package chann

import (
	"math"
	"sync"

	"github.com/pingcap/ticdc/utils/deque"
)

type Group comparable

type Grouper[T any, G Group] func(v T) G

type Sizer[T any] func(v T) int

// UnlimitedChannel is a channel with unlimited buffer.
// It is safe for concurrent use.
// It supports get multiple elements at once, which is suitable for batch processing.
type UnlimitedChannel[T any, G Group] struct {
	grouper Grouper[T, G]
	sizer   Sizer[T]
	queue   deque.Deque[T]

	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
}

func NewUnlimitedChannelDefault[T any]() *UnlimitedChannel[T, any] {
	return NewUnlimitedChannel[T, any](nil, nil)
}

// NewUnlimitedChannel creates a new UnlimitedChannel.
// grouper is a function that returns the group of the element belongs to.
// sizer is a function that returns the number of bytes of the element, by default the bytes is 0.
func NewUnlimitedChannel[T any, G Group](grouper Grouper[T, G], sizer Sizer[T]) *UnlimitedChannel[T, G] {
	ch := &UnlimitedChannel[T, G]{
		grouper: grouper,
		sizer:   sizer,
		queue:   *deque.NewDequeDefault[T](),
	}
	ch.cond = sync.NewCond(&ch.mu)
	return ch
}

func (c *UnlimitedChannel[T, G]) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.cond.Broadcast()
}

func (c *UnlimitedChannel[T, G]) Push(values ...T) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		panic("push to closed ulimited channel")
	}
	for _, v := range values {
		c.queue.PushBack(v)
	}

	c.cond.Signal()
}

// Get retrieves an element from the channel.
// Return the element and a boolean indicating whether the channel is available.
// Return false if the channel is closed.
func (c *UnlimitedChannel[T, G]) Get() (T, bool) {
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

// Get multiple elements from the channel.
// If grouper is provided, it will return all consecutive elements that in the same group,
// even if they already exceeds the cap(buffer) and batchBytes; And then it try to fill the buffer and the batch bytes.
// If grouper is not provided, it will return up to cap(buffer) elements or batchBytes bytes.
// Return the original buffer and a boolean indicating whether the channel is available.
// Return false if the channel is closed.
func (c *UnlimitedChannel[T, G]) GetMultiple(buffer []T, batchBytes ...int) ([]T, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for !c.closed && c.queue.Length() == 0 {
		c.cond.Wait()
	}

	if c.closed && c.queue.Length() == 0 {
		return buffer, false
	}

	maxBytes := math.MaxInt
	if len(batchBytes) > 0 {
		maxBytes = batchBytes[0]
	}
	getBytes := func(v T) int {
		if c.sizer == nil {
			return 0
		}
		return c.sizer(v)
	}

	capSize := cap(buffer)
	bytes := 0

	if c.grouper != nil {
		first, ok := c.queue.FrontRef()
		if !ok {
			panic("unreachable")
		}
		lastGroup := c.grouper(*first)
		for {
			v, ok := c.queue.FrontRef()
			if !ok {
				break
			}

			curGroup := c.grouper(*v)
			if curGroup != lastGroup && (len(buffer) >= capSize || bytes >= maxBytes) {
				break
			}
			lastGroup = curGroup

			buffer = append(buffer, *v)
			bytes += getBytes(*v)

			c.queue.PopFront()
		}
	} else {
		for {
			if len(buffer) >= capSize || bytes >= maxBytes {
				break
			}
			v, ok := c.queue.PopFront()
			if !ok {
				break
			}
			buffer = append(buffer, v)
			bytes += getBytes(v)
		}
	}

	return buffer, true
}

func (c *UnlimitedChannel[T, G]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.queue.Length()
}
