package dynstream

type BoundedRingBuffer[T any] struct {
	buffer     []T
	capacity   int
	head, tail int
}

func NewBoundedRingBuffer[T any](capacity int) *BoundedRingBuffer[T] {
	return &BoundedRingBuffer[T]{
		buffer:   make([]T, capacity+1),
		capacity: capacity,
	}
}

func (rb *BoundedRingBuffer[T]) Head() (T, bool) {
	var zero T
	if rb.head == rb.tail {
		// Buffer is empty
		return zero, false
	}
	return rb.buffer[rb.head], true
}

func (rb *BoundedRingBuffer[T]) Tail() (T, bool) {
	var zero T
	if rb.head == rb.tail {
		// Buffer is empty
		return zero, false
	}
	return rb.buffer[(rb.tail+rb.capacity)%(rb.capacity+1)], true
}

func (rb *BoundedRingBuffer[T]) Append(item T) {
	insertIndex := rb.tail % (rb.capacity + 1)
	rb.buffer[insertIndex] = item
	rb.tail = (rb.tail + 1) % (rb.capacity + 1)

	if rb.tail == rb.head {
		// Buffer is full, move head to overwrite the oldest item
		rb.head = (rb.head + 1) % (rb.capacity + 1)
	}
}

func (rb *BoundedRingBuffer[T]) Remove() (T, bool) {
	var zero T
	if rb.head == rb.tail {
		// Buffer is empty
		return zero, false
	}
	item := rb.buffer[rb.head]
	rb.head = (rb.head + 1) % (rb.capacity + 1)
	return item, true
}

func (rb *BoundedRingBuffer[T]) IsFull() bool {
	return (rb.tail+1)%rb.capacity == rb.head
}

func (rb *BoundedRingBuffer[T]) IsEmpty() bool {
	return rb.head == rb.tail
}

func (rb *BoundedRingBuffer[T]) Size() int {
	if rb.tail >= rb.head {
		return rb.tail - rb.head
	} else {
		return rb.tail + rb.capacity + 1 - rb.head
	}
}
