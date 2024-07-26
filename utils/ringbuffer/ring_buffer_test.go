package ringbuffer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRingBuffer(t *testing.T) {
	rb := NewRingBuffer[int](3)

	rb.PushTail(1)
	rb.PushTail(2)
	rb.PushTail(3)

	{
		item, ok := rb.Head()
		assert.Equal(t, true, ok)
		assert.Equal(t, 1, item)
	}

	rb.PushTail(4)

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

	rb.PushTail(5)
	rb.PushTail(6)
	rb.PushTail(7)
	rb.PushTail(8)

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
		item, ok := rb.PopHead()
		assert.Equal(t, true, ok)
		assert.Equal(t, 6, item)
	}
	{
		item, ok := rb.PopHead()
		assert.Equal(t, true, ok)
		assert.Equal(t, 7, item)
	}
	{
		item, ok := rb.PopHead()
		assert.Equal(t, true, ok)
		assert.Equal(t, 8, item)
	}
	{
		item, ok := rb.PopHead()
		assert.Equal(t, false, ok)
		assert.Equal(t, 0, item)
	}
	assert.Equal(t, true, rb.IsEmpty())

	rb.PushTail(1)
	rb.PushTail(2)
	assert.Equal(t, 2, rb.Size())

	rb.PushTail(3)

	assert.Equal(t, 3, rb.Size())

	{
		item, ok := rb.Head()
		assert.Equal(t, true, ok)
		assert.Equal(t, 1, item)
	}

	rb.PushTail(4)

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
