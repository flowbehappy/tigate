package chann

import (
	"math"
	"sync"

	"github.com/pingcap/ticdc/utils/deque"
)

type Grouper[T any] func(v T) int

type Sizer[T any] func(v T) int

// UnlimitedChannel is a channel with unlimited buffer.
// It is safe for concurrent use.
// It supports get multiple elements at once, which is suitable for batch processing.
type UnlimitedChannel[T any] struct {
	grouper Grouper[T]
	sizer   Sizer[T]
	queue   *deque.Deque[T]

	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
}

func NewUnlimitedChannelDefault[T any]() *UnlimitedChannel[T] {
	return NewUnlimitedChannel[T](nil, nil, deque.NewDequeDefault[T]())
}

func NewUnlimitedChannelWithQueue[T any](queue *deque.Deque[T]) *UnlimitedChannel[T] {
	return NewUnlimitedChannel[T](nil, nil, queue)
}

// NewUnlimitedChannel creates a new UnlimitedChannel.
// grouper is a function that returns the group of the element belongs to.
// sizer is a function that returns the number of bytes of the element, by default the bytes is 0.
func NewUnlimitedChannel[T any](grouper Grouper[T], sizer Sizer[T], queue *deque.Deque[T]) *UnlimitedChannel[T] {
	ch := &UnlimitedChannel[T]{
		grouper: grouper,
		sizer:   sizer,
	}
	if queue == nil {
		ch.queue = deque.NewDequeDefault[T]()
	} else {
		ch.queue = queue
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

func (c *UnlimitedChannel[T]) Push(values ...T) {
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

type getMultType int

const (
	getMultNoGroup getMultType = iota
	getMultMixdGroupCons
	getMultSingleGroup
)

func (c *UnlimitedChannel[T]) getMultiple(gmt getMultType, wait bool, buffer []T, batchBytes ...int) ([]T, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if wait {
		for !c.closed && c.queue.Length() == 0 {
			c.cond.Wait()
		}
	}

	if c.closed && c.queue.Length() == 0 {
		return buffer, false
	} else if c.queue.Length() == 0 {
		return buffer, true
	}

	maxBytes := math.MaxInt
	if len(batchBytes) > 0 {
		maxBytes = batchBytes[0]
	}

	cap := cap(buffer)
	bytes := 0

	switch gmt {
	case getMultNoGroup:
		if c.sizer != nil && maxBytes != math.MaxInt {
			for {
				if len(buffer) >= cap || bytes >= maxBytes {
					break
				}
				v, ok := c.queue.PopFront()
				if !ok {
					break
				}
				buffer = append(buffer, v)
				bytes += c.sizer(v)
			}
		} else {
			for {
				if len(buffer) >= cap {
					break
				}
				v, ok := c.queue.PopFront()
				if !ok {
					break
				}
				buffer = append(buffer, v)
			}
		}
	case getMultMixdGroupCons, getMultSingleGroup:
		first, ok := c.queue.FrontRef()
		if !ok {
			panic("unreachable")
		}
		lastGroup := c.grouper(*first)

		if c.sizer != nil && maxBytes != math.MaxInt {
			for {
				v, ok := c.queue.FrontRef()
				if !ok {
					break
				}

				curGroup := c.grouper(*v)
				if gmt == getMultSingleGroup && (curGroup != lastGroup || (len(buffer) >= cap || bytes >= maxBytes)) {
					break
				} else if curGroup != lastGroup && (len(buffer) >= cap || bytes >= maxBytes) {
					break
				}
				lastGroup = curGroup

				buffer = append(buffer, *v)
				bytes += c.sizer(*v)

				c.queue.PopFront()
			}
		} else {
			for {
				v, ok := c.queue.FrontRef()
				if !ok {
					break
				}

				curGroup := c.grouper(*v)
				if gmt == getMultSingleGroup && (curGroup != lastGroup || len(buffer) >= cap) {
					break
				} else if curGroup != lastGroup && (len(buffer) >= cap || bytes >= maxBytes) {
					break
				}
				lastGroup = curGroup

				buffer = append(buffer, *v)

				c.queue.PopFront()
			}
		}
	}

	return buffer, true
}

// Get multiple elements from the channel.
func (c *UnlimitedChannel[T]) GetMultipleNoGroupWait(buffer []T, batchBytes ...int) ([]T, bool) {
	return c.getMultiple(getMultNoGroup, true, buffer, batchBytes...)
}

func (c *UnlimitedChannel[T]) GetMultipleNoGroupNoWait(buffer []T, batchBytes ...int) ([]T, bool) {
	return c.getMultiple(getMultNoGroup, false, buffer, batchBytes...)
}

// Get multiple elements from the channel. Grouper must be provided.
//
// It returns ALL consecutive elements that in the same group, even if they already exceeds the cap(buffer) and batchBytes;
// And then it try to fill the buffer and the batch bytes.
//
// Return the original buffer and a boolean indicating whether the channel is available.
// Return false if the channel is closed.
// Note that different groups could be mixed in the result.
func (c *UnlimitedChannel[T]) GetMultipleMixdGroupConsecutive(buffer []T, batchBytes ...int) ([]T, bool) {
	if c.grouper == nil {
		panic("grouper is required")
	}
	return c.getMultiple(getMultMixdGroupCons, true, buffer, batchBytes...)
}

// Get multiple elements from the channel. Grouper must be provided.
//
// Note that it only returns the elements in the same group, and tries to fill the buffer and the batch bytes.
func (c *UnlimitedChannel[T]) GetMultipleSingleGroup(buffer []T, batchBytes ...int) ([]T, bool) {
	if c.grouper == nil {
		panic("grouper is required")
	}
	return c.getMultiple(getMultSingleGroup, true, buffer, batchBytes...)
}

func (c *UnlimitedChannel[T]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.queue.Length()
}
