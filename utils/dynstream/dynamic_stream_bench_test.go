package dynstream

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

type intEvent int

type intEventHandler struct {
	inc   *atomic.Int64
	times int

	wg *sync.WaitGroup
}

func (h *intEventHandler) Path(event intEvent) int {
	return int(event)
}
func (h *intEventHandler) Handle(dest D, events ...intEvent) (await bool) {
	for i := 0; i < h.times; i++ {
		h.inc.Add(1)
	}
	h.wg.Done()
	return false
}

func prepareDynamicStream(pathCount int, eventCount int, times int) (DynamicStream[int, intEvent, D], *atomic.Int64, *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	wg.Add(eventCount * pathCount)
	inc := &atomic.Int64{}

	handler := &intEventHandler{
		inc:   inc,
		times: times,
		wg:    wg}

	ds := NewDynamicStreamDefault(handler)
	ds.Start()

	for i := 0; i < pathCount; i++ {
		ds.AddPaths(PathAndDest[int, D]{Path: i, Dest: D{}})
	}

	return ds, inc, wg
}

func runDynamicStream(ds DynamicStream[int, intEvent, D], pathCount int, eventCount int) {
	cpuCount := runtime.NumCPU()
	step := int(math.Ceil(float64(pathCount) / float64(cpuCount)))
	for s := 0; s < cpuCount; s++ {
		from := s * step
		to := min((s+1)*step, pathCount)
		go func(from, to, eventCount int) {
			for i := 0; i < eventCount; i++ {
				for p := from; p < to; p++ {
					ds.In() <- intEvent(p)
				}
			}
		}(from, to, eventCount)
	}

	// for p := 0; p < pathCount; p++ {
	// 	go func(path int) {
	// 		for i := 0; i < eventCount; i++ {
	// 			ds.In() <- intEvent(path)
	// 		}
	// 	}(p)
	// }
}

func prepareGoroutine(pathCount int, eventCount int, times int) ([]chan intEvent, *intEventHandler, *atomic.Int64, *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	wg.Add(eventCount * pathCount)
	inc := &atomic.Int64{}

	handler := &intEventHandler{
		inc:   inc,
		times: times,
		wg:    wg,
	}

	chans := make([]chan intEvent, pathCount)
	for i := 0; i < pathCount; i++ {
		chans[i] = make(chan intEvent, 64)
	}

	return chans, handler, inc, wg
}

func runGoroutine(chans []chan intEvent, pathCount int, eventCount int, handler *intEventHandler) {
	cpuCount := runtime.NumCPU()
	step := int(math.Ceil(float64(pathCount) / float64(cpuCount)))
	for s := 0; s < cpuCount; s++ {
		from := s * step
		to := min((s+1)*step, pathCount)
		go func(from, to, eventCount int) {
			for i := 0; i < eventCount; i++ {
				for p := from; p < to; p++ {
					chans[p] <- intEvent(p)
				}
			}
		}(from, to, eventCount)
	}

	// for i := 0; i < pathCount; i++ {
	// 	go func(ch chan intEvent, path int) {
	// 		for i := 0; i < eventCount; i++ {
	// 			ch <- intEvent(path)
	// 		}
	// 	}(chans[i], i)
	// }

	for i := 0; i < pathCount; i++ {
		go func(ch chan intEvent) {
			for e := range ch {
				handler.Handle(D{}, e)
			}
		}(chans[i])
	}
}

func BenchmarkDSDynamicSt1000x1000x100(b *testing.B) {
	ds, inc, wg := prepareDynamicStream(1000, 1000, 100)

	b.ResetTimer()

	for k := 0; k < b.N; k++ {
		inc.Store(0)
		runDynamicStream(ds, 1000, 1000)
		wg.Wait()

		if inc.Load() != int64(1000*1000*100) {
			panic(fmt.Sprintf("total: %d, expected: %d", inc.Load(), 1000*1000*100))
		}
	}

	ds.Close()
}

func BenchmarkDSDynamicSt1000000x20x50(b *testing.B) {
	ds, inc, wg := prepareDynamicStream(1000000, 20, 50)

	b.ResetTimer()

	for k := 0; k < b.N; k++ {
		inc.Store(0)
		runDynamicStream(ds, 1000000, 20)
		wg.Wait()

		if inc.Load() != int64(1000000*20*50) {
			panic(fmt.Sprintf("total: %d, expected: %d", inc.Load(), 1000000*20*50))
		}
	}

	ds.Close()
}

func BenchmarkDSGoroutine1000x1000x100(b *testing.B) {
	chans, handler, inc, wg := prepareGoroutine(1000, 1000, 100)

	b.ResetTimer()

	for k := 0; k < b.N; k++ {
		inc.Store(0)
		runGoroutine(chans, 1000, 1000, handler)
		wg.Wait()

		if inc.Load() != int64(1000*1000*100) {
			panic(fmt.Sprintf("total: %d, expected: %d", inc.Load(), 1000*1000*100))
		}
	}

	for _, c := range chans {
		close(c)
	}
}

func BenchmarkDSGoroutine1000000x20x50(b *testing.B) {
	chans, handler, inc, wg := prepareGoroutine(1000000, 20, 50)

	b.ResetTimer()

	for k := 0; k < b.N; k++ {
		inc.Store(0)
		runGoroutine(chans, 1000000, 20, handler)
		wg.Wait()

		if inc.Load() != int64(1000000*20*50) {
			panic(fmt.Sprintf("total: %d, expected: %d", inc.Load(), 1000000*20*50))
		}
	}

	for _, c := range chans {
		close(c)
	}
}
