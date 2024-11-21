package deque

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeque(t *testing.T) {
	allocator := NewBlockAllocator[int](2, 1)
	deque := NewDeque[int](2, 5, &allocator)

	// Test empty deque
	assert.Equal(t, 0, deque.Length())

	// Test PushBack
	deque.PushBack(1)
	deque.PushBack(2)
	deque.PushBack(3)
	assert.Equal(t, 3, deque.Length())
	back, ok := deque.Back()
	assert.True(t, ok)
	assert.Equal(t, 3, back)

	// Test PushFront
	deque.PushFront(0)
	assert.Equal(t, 4, deque.Length())
	front, ok := deque.Front()
	assert.True(t, ok)
	assert.Equal(t, 0, front)

	// Test PopBack
	item, ok := deque.PopBack()
	assert.True(t, ok)
	assert.Equal(t, 3, item)
	assert.Equal(t, 3, deque.Length())

	// Test PopFront
	item, ok = deque.PopFront()
	assert.True(t, ok)
	assert.Equal(t, 0, item)
	assert.Equal(t, 2, deque.Length())

	deque.PopFront()
	deque.PopFront()
	item, ok = deque.PopFront()
	assert.False(t, ok)
	assert.Equal(t, 0, item)
	assert.Equal(t, 0, deque.Length())

	deque.PushFront(1)
	deque.PushFront(2)
	deque.PushFront(3)
	deque.PushFront(4)
	deque.PushFront(5)
	deque.PushFront(6)
	assert.Equal(t, 5, deque.Length())
	item, ok = deque.Back()
	assert.True(t, ok)
	assert.Equal(t, 2, item)

	// Test Forward and Backward Iterator
	{
		itr := deque.ForwardIterator()
		items := make([]int, 0, deque.Length())
		for item, ok := itr.Next(); ok; item, ok = itr.Next() {
			items = append(items, item)
		}
		assert.Equal(t, []int{6, 5, 4, 3, 2}, items)
	}

	{
		itr := deque.BackwardIterator()
		items := make([]int, 0, deque.Length())
		for item, ok := itr.Next(); ok; item, ok = itr.Next() {
			items = append(items, item)
		}
		assert.Equal(t, []int{2, 3, 4, 5, 6}, items)
	}
}

func TestDequeReverse(t *testing.T) {
	deque := NewDeque[int](2, 5)

	// Test empty deque
	assert.Equal(t, 0, deque.Length())

	// Test PushFront
	deque.PushFront(1)
	deque.PushFront(2)
	deque.PushFront(3)
	assert.Equal(t, 3, deque.Length())
	back, ok := deque.Front()
	assert.True(t, ok)
	assert.Equal(t, 3, back)

	// Test PushBack
	deque.PushBack(0)
	assert.Equal(t, 4, deque.Length())
	front, ok := deque.Back()
	assert.True(t, ok)
	assert.Equal(t, 0, front)

	// Test PopFront
	item, ok := deque.PopFront()
	assert.True(t, ok)
	assert.Equal(t, 3, item)
	assert.Equal(t, 3, deque.Length())

	// Test PopBack
	item, ok = deque.PopBack()
	assert.True(t, ok)
	assert.Equal(t, 0, item)
	assert.Equal(t, 2, deque.Length())

	deque.PopBack()
	deque.PopBack()
	item, ok = deque.PopBack()
	assert.False(t, ok)
	assert.Equal(t, 0, item)
	assert.Equal(t, 0, deque.Length())

	deque.PushBack(1)
	deque.PushBack(2)
	deque.PushBack(3)
	deque.PushBack(4)
	deque.PushBack(5)
	deque.PushBack(6)
	assert.Equal(t, 5, deque.Length())
	item, ok = deque.Front()
	assert.True(t, ok)
	assert.Equal(t, 2, item)

	// Test Backward and Forward Iterator
	{
		itr := deque.BackwardIterator()
		items := make([]int, 0, deque.Length())
		for item, ok := itr.Next(); ok; item, ok = itr.Next() {
			items = append(items, item)
		}
		assert.Equal(t, []int{6, 5, 4, 3, 2}, items)
	}

	{
		itr := deque.ForwardIterator()
		items := make([]int, 0, deque.Length())
		for item, ok := itr.Next(); ok; item, ok = itr.Next() {
			items = append(items, item)
		}
		assert.Equal(t, []int{2, 3, 4, 5, 6}, items)
	}

}

func TestDequeBlockIt(t *testing.T) {
	deque := NewDeque[int](2, 0)
	// [1, 2] [3, 0]
	deque.PushBack(1)
	deque.PushBack(2)
	deque.PushBack(3)

	f := deque.ForwardBlockIterator()
	block, ok := f.Next()
	assert.True(t, ok)
	assert.Equal(t, 2, len(block))
	assert.Equal(t, 1, block[0])
	assert.Equal(t, 2, block[1])

	block, ok = f.Next()
	assert.True(t, ok)
	assert.Equal(t, 1, len(block))
	assert.Equal(t, 3, block[0])

	_, ok = f.Next()
	assert.False(t, ok)

	b := deque.BackwardBlockIterator()
	block, ok = b.Next()
	assert.True(t, ok)
	assert.Equal(t, 1, len(block))
	assert.Equal(t, 3, block[0])

	block, ok = b.Next()
	assert.True(t, ok)
	assert.Equal(t, 2, len(block))
	assert.Equal(t, 1, block[0])
	assert.Equal(t, 2, block[1])

	block, ok = b.Next()
	assert.False(t, ok)
	assert.Equal(t, 0, len(block))
}
