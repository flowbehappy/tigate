package dynstream

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
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

func (h *intEventHandler) GetSize(event intEvent) int            { return 0 }
func (h *intEventHandler) GetArea(path int, dest D) int          { return 0 }
func (h *intEventHandler) GetTimestamp(event intEvent) Timestamp { return 0 }
func (h *intEventHandler) GetType(event intEvent) EventType      { return DefaultEventType }
func (h *intEventHandler) IsPaused(event intEvent) bool          { return false }
func (h *intEventHandler) OnDrop(event intEvent)                 {}

func prepareDynamicStream(pathCount int, eventCount int, times int) (DynamicStream[int, int, intEvent, D, *intEventHandler], *atomic.Int64, *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	wg.Add(eventCount * pathCount)
	inc := &atomic.Int64{}

	handler := &intEventHandler{
		inc:   inc,
		times: times,
		wg:    wg}

	ds := NewDynamicStream(handler)
	ds.Start()

	for i := 0; i < pathCount; i++ {
		ds.AddPath(i, D{})
	}

	return ds, inc, wg
}

func runDynamicStream(ds DynamicStream[int, int, intEvent, D, *intEventHandler], pathCount int, eventCount int) {
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
}

func prepareGoroutine(pathCount int, eventCount int, times int) ([]chan intEvent, *atomic.Int64, *sync.WaitGroup) {
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

	for i := 0; i < pathCount; i++ {
		go func(ch chan intEvent) {
			for e := range ch {
				handler.Handle(D{}, e)
			}
		}(chans[i])
	}

	return chans, inc, wg
}

func runGoroutine(chans []chan intEvent, pathCount int, eventCount int) {
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
	// 	go func(ch chan intEvent) {
	// 		for e := range ch {
	// 			handler.Handle(D{}, e)
	// 		}
	// 	}(chans[i])
	// }
}

func startHeapProfile(filename string) func() {
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	return func() {
		pprof.StopCPUProfile()
		f.Close()
	}
}

func BenchmarkDSDynamicSt1000x1000x100(b *testing.B) {
	for k := 0; k < b.N; k++ {
		// stopHeapProfile := startHeapProfile("heap_profile_1000x1000x100.prof")
		// defer stopHeapProfile()

		ds, inc, wg := prepareDynamicStream(1000, 1000, 100)

		b.ResetTimer()
		inc.Store(0)
		runDynamicStream(ds, 1000, 1000)
		wg.Wait()

		if inc.Load() != int64(1000*1000*100) {
			panic(fmt.Sprintf("total: %d, expected: %d", inc.Load(), 1000*1000*100))
		}
		ds.Close()

	}
}

func BenchmarkDSDynamicSt1000000x20x50(b *testing.B) {
	for k := 0; k < b.N; k++ {
		// stopHeapProfile := startHeapProfile("heap_profile_1000000x20x50.prof")
		// defer stopHeapProfile()

		ds, inc, wg := prepareDynamicStream(1000000, 20, 50)
		b.ResetTimer()
		inc.Store(0)
		runDynamicStream(ds, 1000000, 20)
		wg.Wait()

		if inc.Load() != int64(1000000*20*50) {
			panic(fmt.Sprintf("total: %d, expected: %d", inc.Load(), 1000000*20*50))
		}
		ds.Close()
	}
}

func BenchmarkDSGoroutine1000x1000x100(b *testing.B) {
	for k := 0; k < b.N; k++ {
		chans, inc, wg := prepareGoroutine(1000, 1000, 100)
		b.ResetTimer()

		inc.Store(0)
		runGoroutine(chans, 1000, 1000)
		wg.Wait()

		if inc.Load() != int64(1000*1000*100) {
			panic(fmt.Sprintf("total: %d, expected: %d", inc.Load(), 1000*1000*100))
		}
		for _, c := range chans {
			close(c)
		}
	}
}

func BenchmarkDSGoroutine1000000x20x50(b *testing.B) {
	for k := 0; k < b.N; k++ {
		chans, inc, wg := prepareGoroutine(1000000, 20, 50)

		b.ResetTimer()
		inc.Store(0)
		runGoroutine(chans, 1000000, 20)
		wg.Wait()

		if inc.Load() != int64(1000000*20*50) {
			panic(fmt.Sprintf("total: %d, expected: %d", inc.Load(), 1000000*20*50))
		}
		for _, c := range chans {
			close(c)
		}
	}
}
