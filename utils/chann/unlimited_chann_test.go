package chann

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestUnlimitedChannel(t *testing.T) {
	ch := NewUnlimitedChannel[int]()
	total := 10000
	var sent atomic.Int64

	wgSend := &sync.WaitGroup{}
	for g := 0; g < 10; g++ {
		wgSend.Add(1)
		go func() {
			for {
				cur := sent.Add(1)
				if cur > int64(total) {
					break
				}
				time.Sleep(time.Duration(g) * time.Nanosecond)
				ch.Push(1)
			}
			wgSend.Done()
		}()
	}

	go func() {
		wgSend.Wait()
		ch.Close()
	}()

	var result atomic.Int64
	wgRecive := &sync.WaitGroup{}

	for g := 0; g < 10; g++ {
		wgRecive.Add(1)
		go func() {
			if g%2 == 0 {
				for {
					v, ok := ch.Get()
					if !ok {
						break
					}
					result.Add(int64(v))
				}
			} else {
				for {
					buffer := make([]int, 0, 3)
					buffer, ok := ch.GetMultiple(buffer)
					if !ok {
						break
					}
					for _, v := range buffer {
						result.Add(int64(v))
					}
				}
			}
			wgRecive.Done()
		}()
	}

	wgRecive.Wait()

	assert.Equal(t, result.Load(), int64(total))
}
