package uniqueue

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type testKeyGetter[K comparable] struct {
	key K
}

func (t *testKeyGetter[K]) GetKey() K {
	return t.key
}

func TestUniqueKeyQueue(t *testing.T) {
	t.Run("basic push and pop operations", func(t *testing.T) {
		queue := NewUniqueKeyQueue[string, *testKeyGetter[string]]()

		// Test Push operation
		queue.Push(&testKeyGetter[string]{key: "key1"})
		queue.Push(&testKeyGetter[string]{key: "key2"})
		queue.Push(&testKeyGetter[string]{key: "key1"}) // Duplicate key should be ignored

		// Test Pop operation
		val1, ok1 := queue.Pop()
		require.True(t, ok1)
		require.Equal(t, 1, val1)

		val2, ok2 := queue.Pop()
		require.True(t, ok2)
		require.Equal(t, 2, val2)

		// The queue should be empty
		_, ok3 := queue.Pop()
		require.False(t, ok3)
	})

	t.Run("empty queue operations", func(t *testing.T) {
		queue := NewUniqueKeyQueue[int, *testKeyGetter[int]]()

		// Pop from an empty queue
		val, ok := queue.Pop()
		require.False(t, ok)
		require.Empty(t, val)
	})

}

func TestUniqueKeyQueueConcurrent(t *testing.T) {
	uniQueue := NewUniqueKeyQueue[uuid.UUID, *testKeyGetter[uuid.UUID]]()
	numGoroutines := 10
	operationsPerGoroutine := 1000
	wg := sync.WaitGroup{}
	// Start producer goroutines

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				key := uuid.New()
				uniQueue.Push(&testKeyGetter[uuid.UUID]{key: key})
			}
		}()
	}

	// Start consumer goroutines
	popCount := atomic.Int32{}
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				if _, ok := uniQueue.Pop(); ok {
					popCount.Add(1)
				}
			}
		}()
	}
	// Wait for all goroutines to complete
	wg.Wait()

	// Pop all the data from the queue
	for {
		_, ok := uniQueue.Pop()
		if !ok {
			break
		}
		popCount.Add(1)
	}

	// Verify that we processed the expected number of items
	require.Equal(t, int32(numGoroutines*operationsPerGoroutine), popCount.Load())
	// Verify queue is empty
	_, ok := uniQueue.Pop()
	require.False(t, ok)
}
