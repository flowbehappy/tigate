package ringbuffer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRingBuffer(t *testing.T) {
	rb := NewRingBuffer[int](3)

	rb.PushBack(1)
	rb.PushBack(2)
	rb.PushBack(3)

	{
		item, ok := rb.Front()
		assert.Equal(t, true, ok)
		assert.Equal(t, 1, item)
	}

	rb.PushBack(4)

	{
		item, ok := rb.Front()
		assert.Equal(t, true, ok)
		assert.Equal(t, 2, item)
	}

	{
		item, ok := rb.Back()
		assert.Equal(t, true, ok)
		assert.Equal(t, 4, item)
	}

	rb.PushBack(5)
	rb.PushBack(6)
	rb.PushBack(7)
	rb.PushBack(8)

	assert.Equal(t, true, rb.IsFull())
	{
		item, ok := rb.Front()
		assert.Equal(t, true, ok)
		assert.Equal(t, 6, item)
	}

	{
		item, ok := rb.Back()
		assert.Equal(t, true, ok)
		assert.Equal(t, 8, item)
	}

	{
		item, ok := rb.PopFront()
		assert.Equal(t, true, ok)
		assert.Equal(t, 6, item)
	}
	{
		item, ok := rb.PopFront()
		assert.Equal(t, true, ok)
		assert.Equal(t, 7, item)
	}
	{
		item, ok := rb.PopFront()
		assert.Equal(t, true, ok)
		assert.Equal(t, 8, item)
	}
	{
		item, ok := rb.PopFront()
		assert.Equal(t, false, ok)
		assert.Equal(t, 0, item)
	}
	assert.Equal(t, true, rb.IsEmpty())

	rb.PushBack(1)
	rb.PushBack(2)
	assert.Equal(t, 2, rb.Length())

	rb.PushBack(3)

	assert.Equal(t, 3, rb.Length())

	{
		item, ok := rb.Front()
		assert.Equal(t, true, ok)
		assert.Equal(t, 1, item)
	}

	rb.PushBack(4)

	{
		item, ok := rb.Front()
		assert.Equal(t, true, ok)
		assert.Equal(t, 2, item)
	}

	{
		item, ok := rb.Back()
		assert.Equal(t, true, ok)
		assert.Equal(t, 4, item)
	}

	{
		itr := rb.ForwardIterator()
		items := make([]int, 0)
		for item, ok := itr.Next(); ok; item, ok = itr.Next() {
			items = append(items, item)
		}
		assert.Equal(t, []int{2, 3, 4}, items)
	}

	{
		itr := rb.BackwardIterator()
		items := make([]int, 0)
		for item, ok := itr.Next(); ok; item, ok = itr.Next() {
			items = append(items, item)
		}
		assert.Equal(t, []int{4, 3, 2}, items)
	}

	{
		item, ok := rb.PopBack()
		assert.Equal(t, true, ok)
		assert.Equal(t, 4, item)
		item, ok = rb.PopBack()
		assert.Equal(t, true, ok)
		assert.Equal(t, 3, item)
		item, ok = rb.PopBack()
		assert.Equal(t, true, ok)
		assert.Equal(t, 2, item)
		_, ok = rb.PopBack()
		assert.Equal(t, false, ok)
	}

	{
		rb.PushBack(1)
		rb.PushBack(2)

		rb.PushBack(3)
		rb.PushBack(4)
		rb.PushBack(5)

		item, ok := rb.PopBack()
		assert.Equal(t, true, ok)
		assert.Equal(t, 5, item)
		item, ok = rb.PopBack()
		assert.Equal(t, true, ok)
		assert.Equal(t, 4, item)
		item, ok = rb.PopBack()
		assert.Equal(t, true, ok)
		assert.Equal(t, 3, item)
		_, ok = rb.PopBack()
		assert.Equal(t, false, ok)
	}

	{
		rb.PushBack(1)
		rb.PushBack(2)

		rb.PushBack(3)
		rb.PushBack(4)
		rb.PushBack(5)

		rb.PushFront(6)
		rb.PushFront(7)
	}

	{
		itr := rb.ForwardIterator()
		items := make([]int, 0)
		for item, ok := itr.Next(); ok; item, ok = itr.Next() {
			items = append(items, item)
		}
		assert.Equal(t, []int{7, 6, 3}, items)
	}

	{
		itr := rb.BackwardIterator()
		items := make([]int, 0)
		for item, ok := itr.Next(); ok; item, ok = itr.Next() {
			items = append(items, item)
		}
		assert.Equal(t, []int{3, 6, 7}, items)
	}
}
