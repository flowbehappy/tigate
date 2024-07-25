package dynstream

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRingBuffer(t *testing.T) {
	rb := NewRingBuffer[int](3)

	rb.Push(1)
	rb.Push(2)
	rb.Push(3)

	{
		item, ok := rb.Head()
		assert.Equal(t, true, ok)
		assert.Equal(t, 1, item)
	}

	rb.Push(4)

	{
		item, ok := rb.Head()
		assert.Equal(t, true, ok)
		assert.Equal(t, 2, item)
	}

	{
		item, ok := rb.Tail()
		assert.Equal(t, true, ok)
		assert.Equal(t, 4, item)
	}

	rb.Push(5)
	rb.Push(6)
	rb.Push(7)
	rb.Push(8)

	assert.Equal(t, true, rb.IsFull())
	{
		item, ok := rb.Head()
		assert.Equal(t, true, ok)
		assert.Equal(t, 6, item)
	}

	{
		item, ok := rb.Tail()
		assert.Equal(t, true, ok)
		assert.Equal(t, 8, item)
	}

	{
		item, ok := rb.Pop()
		assert.Equal(t, true, ok)
		assert.Equal(t, 6, item)
	}
	{
		item, ok := rb.Pop()
		assert.Equal(t, true, ok)
		assert.Equal(t, 7, item)
	}
	{
		item, ok := rb.Pop()
		assert.Equal(t, true, ok)
		assert.Equal(t, 8, item)
	}
	{
		item, ok := rb.Pop()
		assert.Equal(t, false, ok)
		assert.Equal(t, 0, item)
	}
	assert.Equal(t, true, rb.IsEmpty())

	rb.Push(1)
	rb.Push(2)
	assert.Equal(t, 2, rb.Size())

	rb.Push(3)

	assert.Equal(t, 3, rb.Size())

	{
		item, ok := rb.Head()
		assert.Equal(t, true, ok)
		assert.Equal(t, 1, item)
	}

	rb.Push(4)

	{
		item, ok := rb.Head()
		assert.Equal(t, true, ok)
		assert.Equal(t, 2, item)
	}

	{
		item, ok := rb.Tail()
		assert.Equal(t, true, ok)
		assert.Equal(t, 4, item)
	}

	{
		itr := rb.Iterator()
		items := make([]int, 0)
		for item, ok := itr.Next(); ok; item, ok = itr.Next() {
			items = append(items, item)
		}
		assert.Equal(t, []int{2, 3, 4}, items)
	}
}
