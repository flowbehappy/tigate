package ringbuffer

type RingBuffer[T any] struct {
	buffer     []T
	capacity   int
	head, tail int

	zero T
}

type Iter[T any] struct {
	rb    *RingBuffer[T]
	index int
}

func (rb *RingBuffer[T]) Iterator() *Iter[T] {
	return &Iter[T]{rb: rb, index: rb.head}
}

func (it *Iter[T]) Next() (T, bool) {
	if it.index == it.rb.tail {
		return it.rb.zero, false
	}
	item := it.rb.buffer[it.index]
	it.index = (it.index + 1) % (it.rb.capacity + 1)
	return item, true
}

func NewRingBuffer[T any](capacity int) *RingBuffer[T] {
	return &RingBuffer[T]{
		buffer:   make([]T, capacity+1),
		capacity: capacity,
	}
}

func (rb *RingBuffer[T]) Head() (T, bool) {
	if rb.head == rb.tail {
		// Buffer is empty
		return rb.zero, false
	}
	return rb.buffer[rb.head], true
}

func (rb *RingBuffer[T]) Tail() (T, bool) {
	if rb.head == rb.tail {
		// Buffer is empty
		return rb.zero, false
	}
	return rb.buffer[(rb.tail+rb.capacity)%(rb.capacity+1)], true
}

// Push to tail
func (rb *RingBuffer[T]) PushTail(item T) {
	insertIndex := rb.tail % (rb.capacity + 1)
	rb.buffer[insertIndex] = item
	rb.tail = (rb.tail + 1) % (rb.capacity + 1)

	if rb.tail == rb.head {
		// Buffer is full, move head to overwrite the oldest item
		rb.buffer[rb.head] = rb.zero
		rb.head = (rb.head + 1) % (rb.capacity + 1)
	}
}

// Pop from head
func (rb *RingBuffer[T]) PopHead() (T, bool) {
	if rb.head == rb.tail {
		// Buffer is empty
		return rb.zero, false
	}
	item := rb.buffer[rb.head]
	rb.buffer[rb.head] = rb.zero
	rb.head = (rb.head + 1) % (rb.capacity + 1)
	return item, true
}

func (rb *RingBuffer[T]) IsFull() bool {
	return (rb.tail+1)%rb.capacity == rb.head
}

func (rb *RingBuffer[T]) IsEmpty() bool {
	return rb.head == rb.tail
}

func (rb *RingBuffer[T]) Size() int {
	if rb.tail >= rb.head {
		return rb.tail - rb.head
	} else {
		return rb.tail + rb.capacity + 1 - rb.head
	}
}
