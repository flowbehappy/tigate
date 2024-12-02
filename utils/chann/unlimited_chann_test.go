package chann

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestUnlimitedChannel(t *testing.T) {
	ch := NewUnlimitedChannelDefault[int]()
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
					buffer, ok := ch.GetMultipleNoGroup(buffer)
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

func TestUnlimitedChannelGroup(t *testing.T) {
	ch := NewUnlimitedChannel[int, int](
		func(v int) int {
			return v
		},
		func(v int) int {
			return 1
		})
	total := 100000
	var sent atomic.Int64
	var bytes atomic.Int64

	wgSend := &sync.WaitGroup{}
	for g := 0; g < 10; g++ {
		wgSend.Add(1)
		go func() {
			for {
				cur := sent.Add(int64(g))
				time.Sleep(time.Duration(g) * time.Nanosecond)
				for i := 0; i < g; i++ {
					ch.Push(g)
					bytes.Add(int64(g))
				}

				if cur > int64(total) {
					break
				}
			}
			wgSend.Done()
		}()
	}

	go func() {
		wgSend.Wait()
		ch.Close()
	}()

	var resultCount atomic.Int64
	var resultBytes atomic.Int64
	var incCap atomic.Int64
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
					resultCount.Add(int64(1))
					resultBytes.Add(int64(v))
				}
			} else {
				for {
					buffer := make([]int, 0, 5)
					beforeCap := cap(buffer)
					buffer, ok := ch.GetMultipleNoGroup(buffer)
					afterCap := cap(buffer)
					if !ok {
						break
					}
					if beforeCap != afterCap {
						incCap.Add(int64(1))
					}
					for _, v := range buffer {
						resultCount.Add(int64(1))
						resultBytes.Add(int64(v))
					}
				}
			}
			wgRecive.Done()
		}()
	}

	wgRecive.Wait()

	assert.Equal(t, resultCount.Load(), sent.Load())
	assert.Equal(t, resultBytes.Load(), bytes.Load())
	fmt.Printf("incCap: %d\n", incCap.Load())
}
