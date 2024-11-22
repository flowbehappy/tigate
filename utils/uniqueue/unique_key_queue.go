package uniqueue

import (
	"sync"

	"github.com/pingcap/ticdc/utils/deque"
)

type KeyGetter[K any] interface {
	GetKey() K
}

type UniqueKeyQueue[K comparable, T KeyGetter[K]] struct {
	mu     sync.RWMutex
	set    map[K]struct{}
	queue  *deque.Deque[T]
	notify chan struct{}
}

func NewUniqueKeyQueue[K comparable, T KeyGetter[K]]() *UniqueKeyQueue[K, T] {
	return &UniqueKeyQueue[K, T]{
		set:    make(map[K]struct{}, 1024),
		queue:  deque.NewDequeDefault[T](),
		notify: make(chan struct{}, 1024),
	}
}

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

func (q *UniqueKeyQueue[K, T]) Notify() <-chan struct{} {
	return q.notify
}
