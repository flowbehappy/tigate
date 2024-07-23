package dynstream

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBoundedRingBuffer(t *testing.T) {
	rb := NewBoundedRingBuffer[int](3)

	rb.Append(1)
	rb.Append(2)
	rb.Append(3)

	{
		item, ok := rb.Head()
		assert.Equal(t, true, ok)
		assert.Equal(t, 1, item)
	}

	rb.Append(4)

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

	rb.Append(5)
	rb.Append(6)
	rb.Append(7)
	rb.Append(8)

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
		item, ok := rb.Remove()
		assert.Equal(t, true, ok)
		assert.Equal(t, 6, item)
	}
	{
		item, ok := rb.Remove()
		assert.Equal(t, true, ok)
		assert.Equal(t, 7, item)
	}
	{
		item, ok := rb.Remove()
		assert.Equal(t, true, ok)
		assert.Equal(t, 8, item)
	}
	{
		item, ok := rb.Remove()
		assert.Equal(t, false, ok)
		assert.Equal(t, 0, item)
	}
	assert.Equal(t, true, rb.IsEmpty())

	rb.Append(1)
	rb.Append(2)
	assert.Equal(t, 2, rb.Size())

	rb.Append(3)

	assert.Equal(t, 3, rb.Size())

	{
		item, ok := rb.Head()
		assert.Equal(t, true, ok)
		assert.Equal(t, 1, item)
	}

	rb.Append(4)

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
}
