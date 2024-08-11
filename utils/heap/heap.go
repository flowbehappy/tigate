package heap

import (
	"container/heap"
)

// A generic heap implementation.
// Check heap_test.go for usage.
type Heap[T Item[T]] interface {
	Len() int
	AddOrUpdate(t T)
	PopTop() (T, bool)
	PeekTop() (T, bool)
	Remove(t T) bool
	All() []T // Return all the items in the heap
}

// The item interface for the heap.
// The heap index is used to indicate the position of the item in the heap. To make the initialization of a item easier,
// heap index value 0 means it's not in the heap.
// CompareTo is used to decide the items' order. The heap will be a min heap if the return value is decided by the natural order.
type Item[T any] interface {
	SetHeapIndex(int)
	GetHeapIndex() int
	// Return negative value if this item is smaller than the other item,
	// 0 if they are equal,
	// positive value if this item is larger than the other item.
	CompareTo(T) int
}

type heapImp[T Item[T]] struct {
	items []T
}

func NewHeap[T Item[T]]() Heap[T] {
	return &heapImp[T]{
		items: make([]T, 0),
	}
}

// ====================================
// Notice: Don't call those methods below directly! They are only called by heap package.

func (h heapImp[T]) Len() int { return len(h.items) }
func (h heapImp[T]) Less(i, j int) bool {
	return h.items[i].CompareTo(h.items[j]) < 0
}
func (h heapImp[T]) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].SetHeapIndex(i + 1)
	h.items[j].SetHeapIndex(j + 1)
}

func (h *heapImp[T]) Push(x interface{}) {
	index := len(h.items)
	h.items = append(h.items, x.(T))
	h.items[index].SetHeapIndex(index + 1)
}

// Pop the last element of the slice.
func (h *heapImp[T]) Pop() interface{} {
	var zero T
	old := h.items
	n := len(old)
	x := old[n-1]
	old[n-1] = zero // avoid memory leak
	h.items = old[0 : n-1]
	return x
}

// ====================================

func (h *heapImp[T]) AddOrUpdate(t T) {
	// 0 means the item is not in the heap
	// So the actual index is GetHeapIndex() - 1
	idx := t.GetHeapIndex() - 1
	if idx >= 0 {
		heap.Fix(h, idx)
	} else {
		heap.Push(h, t)
	}
}

func (h *heapImp[T]) PopTop() (T, bool) {
	var zero T
	if h.Len() == 0 {
		return zero, false
	}
	t := heap.Pop(h).(T)
	t.SetHeapIndex(0) // Mark the item is not in the heap
	return t, true
}
func (h *heapImp[T]) PeekTop() (T, bool) {
	var zero T
	if h.Len() == 0 {
		return zero, false
	}
	return h.items[0], true
}

func (h *heapImp[T]) Remove(t T) bool {
	idx := t.GetHeapIndex() - 1
	if idx >= 0 {
		heap.Remove(h, idx)
		t.SetHeapIndex(0) // Mark the item is not in the heap
		return true
	}
	return false
}

func (h *heapImp[T]) All() []T {
	return h.items
}
