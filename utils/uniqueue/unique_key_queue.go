package uniqueue

import (
	"sync"

	"github.com/pingcap/ticdc/utils/deque"
)

// KeyGetter is an interface that defines a method to get a key of type K
type KeyGetter[K any] interface {
	GetKey() K
}

// UniqueKeyQueue is a thread-safe queue that ensures uniqueness of elements based on their keys.
// It uses a map to track unique keys and a deque to maintain the order of elements.
// K is the key type which must be comparable
// T is the value type which must implement KeyGetter[K]
type UniqueKeyQueue[K comparable, T KeyGetter[K]] struct {
	mu     sync.RWMutex
	set    map[K]struct{}
	queue  *deque.Deque[T]
	notify chan struct{}
}

// NewUniqueKeyQueue creates and initializes a new UniqueKeyQueue with default capacity.
// Returns a pointer to the newly created queue.
func NewUniqueKeyQueue[K comparable, T KeyGetter[K]]() *UniqueKeyQueue[K, T] {
	return &UniqueKeyQueue[K, T]{
		set:    make(map[K]struct{}, 1024),
		queue:  deque.NewDequeDefault[T](),
		notify: make(chan struct{}, 1024),
	}
}

// Push adds a new element to the queue if its key is not already present.
// If the key already exists, the element is ignored.
// This operation is thread-safe.
func (q *UniqueKeyQueue[K, T]) Push(value T) {
	q.mu.Lock()
	defer q.mu.Unlock()

	key := value.GetKey()
	if _, ok := q.set[key]; ok {
		return
	}
	q.set[key] = struct{}{}
	q.queue.PushBack(value)

	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// Pop removes and returns the first element from the queue.
// Returns the element and true if successful, or zero value and false if the queue is empty.
// This operation is thread-safe.
func (q *UniqueKeyQueue[K, T]) Pop() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var zero T
	if q.queue.Length() == 0 {
		return zero, false
	}
	v, ok := q.queue.PopFront()
	if !ok {
		return zero, false
	}
	delete(q.set, v.GetKey())

	return v, true
}

// Notify returns a channel that receives a signal whenever a new element is pushed to the queue.
// The channel can be used to wait for new elements.
func (q *UniqueKeyQueue[K, T]) Notify() <-chan struct{} {
	return q.notify
}

func (q *UniqueKeyQueue[K, T]) Length() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.queue.Length()
}
