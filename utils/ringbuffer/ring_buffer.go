package ringbuffer

type RingBuffer[T any] struct {
	buffer      []T
	capacity    int
	front, back int

	zero T
}

type ForwardIter[T any] struct {
	rb    *RingBuffer[T]
	index int
}

func (rb *RingBuffer[T]) ForwardIterator() *ForwardIter[T] {
	return &ForwardIter[T]{rb: rb, index: rb.front}
}

func (it *ForwardIter[T]) Next() (T, bool) {
	if it.index == it.rb.back {
		return it.rb.zero, false
	}
	item := it.rb.buffer[it.index]
	it.index = (it.index + 1) % (it.rb.capacity + 1)
	return item, true
}

type BackwardIter[T any] struct {
	rb    *RingBuffer[T]
	index int
}

func (rb *RingBuffer[T]) BackwardIterator() *BackwardIter[T] {
	return &BackwardIter[T]{rb: rb, index: rb.back}
}

func (it *BackwardIter[T]) Next() (T, bool) {
	if it.index == it.rb.front {
		return it.rb.zero, false
	}
	it.index = (it.index + it.rb.capacity) % (it.rb.capacity + 1)
	item := it.rb.buffer[it.index]
	return item, true
}

func NewRingBuffer[T any](capacity int) RingBuffer[T] {
	return RingBuffer[T]{
		buffer:   make([]T, capacity+1),
		capacity: capacity,
	}
}

func (rb *RingBuffer[T]) Front() (T, bool) {
	if rb.front == rb.back {
		// Buffer is empty
		return rb.zero, false
	}
	return rb.buffer[rb.front], true
}

func (rb *RingBuffer[T]) Back() (T, bool) {
	if rb.front == rb.back {
		// Buffer is empty
		return rb.zero, false
	}
	return rb.buffer[(rb.back+rb.capacity)%(rb.capacity+1)], true
}

// Push to back
func (rb *RingBuffer[T]) PushBack(item T) {
	insertIndex := rb.back % (rb.capacity + 1)
	rb.buffer[insertIndex] = item
	rb.back = (rb.back + 1) % (rb.capacity + 1)

	if rb.back == rb.front {
		// Buffer is full, move front to overwrite the oldest item
		rb.buffer[rb.front] = rb.zero
		rb.front = (rb.front + 1) % (rb.capacity + 1)
	}
}

// Push to front
func (rb *RingBuffer[T]) PushFront(item T) {
	// equivalent to
	// if rb.front == 0 {
	//     rb.front = rb.capacity
	// } else {
	//     rb.front--
	// }
	rb.front = (rb.front + rb.capacity) % (rb.capacity + 1)
	rb.buffer[rb.front] = item

	if rb.back == rb.front {
		// Buffer is full, move back to overwrite the oldest item
		rb.back = (rb.back + rb.capacity) % (rb.capacity + 1)
		rb.buffer[rb.back] = rb.zero
	}
}

// Pop from front
func (rb *RingBuffer[T]) PopFront() (T, bool) {
	if rb.front == rb.back {
		// Buffer is empty
		return rb.zero, false
	}
	item := rb.buffer[rb.front]
	rb.buffer[rb.front] = rb.zero
	rb.front = (rb.front + 1) % (rb.capacity + 1)
	return item, true
}

// Pop from back
func (rb *RingBuffer[T]) PopBack() (T, bool) {
	if rb.front == rb.back {
		// Buffer is empty
		return rb.zero, false
	}
	rb.back = (rb.back + rb.capacity) % (rb.capacity + 1)
	item := rb.buffer[rb.back]
	rb.buffer[rb.back] = rb.zero
	return item, true
}

func (rb *RingBuffer[T]) IsFull() bool {
	return (rb.back+1)%(rb.capacity+1) == rb.front
}

func (rb *RingBuffer[T]) IsEmpty() bool {
	return rb.front == rb.back
}

func (rb *RingBuffer[T]) Length() int {
	if rb.back >= rb.front {
		return rb.back - rb.front
	} else {
		return rb.back + rb.capacity + 1 - rb.front
	}
}
