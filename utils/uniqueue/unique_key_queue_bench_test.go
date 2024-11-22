package uniqueue

import (
	"strconv"
	"testing"
)

func BenchmarkUniqueKeyQueue(b *testing.B) {
	b.Run("Push", func(b *testing.B) {
		queue := NewUniqueKeyQueue[string, *testKeyGetter[string]]()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := strconv.Itoa(i)
			queue.Push(&testKeyGetter[string]{key: key})
		}
	})

	b.Run("Push-Same-Key", func(b *testing.B) {
		queue := NewUniqueKeyQueue[string, *testKeyGetter[string]]()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			queue.Push(&testKeyGetter[string]{key: "same-key"})
		}
	})

	b.Run("Push-Pop", func(b *testing.B) {
		queue := NewUniqueKeyQueue[string, *testKeyGetter[string]]()
		// 预先填充一些数据
		for i := 0; i < 1000; i++ {
			key := strconv.Itoa(i)
			queue.Push(&testKeyGetter[string]{key: key})
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if i%2 == 0 {
				key := strconv.Itoa(i + 1000)
				queue.Push(&testKeyGetter[string]{key: key})
			} else {
				queue.Pop()
			}
		}
	})

	b.Run("Parallel-Push-Pop", func(b *testing.B) {
		queue := NewUniqueKeyQueue[string, *testKeyGetter[string]]()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if i%2 == 0 {
					key := strconv.Itoa(i)
					queue.Push(&testKeyGetter[string]{key: key})
				} else {
					queue.Pop()
				}
				i++
			}
		})
	})
}
