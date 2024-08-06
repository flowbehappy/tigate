package dynstream

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func runDynamicStream(pathCount int, eventCount int, times int) {
	handler := &incHandler{}

	ds := NewDynamicStreamDefault(handler)
	ds.Start()

	for i := 0; i < pathCount; i++ {
		ds.AddPath(PathAndDest[D]{Path: Path(fmt.Sprintf("p%d", i)), Dest: D{}})
	}

	wg := &sync.WaitGroup{}
	wg.Add(eventCount * pathCount)

	sendEvents := func(path Path, wg *sync.WaitGroup) {
		total := &atomic.Int64{}
		for i := 0; i < eventCount; i++ {
			ds.In() <- &inc{times: times, n: total, done: wg, path: path}
		}
	}

	for i := 0; i < pathCount; i++ {
		sendEvents(Path(fmt.Sprintf("p%d", i)), wg)
	}

	wg.Wait()
	ds.Close()
}

func runGoroutine(pathCount int, eventCount int, times int) {
	handler := &incHandler{}
	chans := make([]chan *inc, pathCount)
	for i := 0; i < pathCount; i++ {
		chans[i] = make(chan *inc, eventCount)
	}

	wg := &sync.WaitGroup{}
	wg.Add(eventCount * pathCount)

	sendEvents := func(ch chan *inc, wg *sync.WaitGroup) {
		total := &atomic.Int64{}
		for i := 0; i < eventCount; i++ {
			ch <- &inc{times: times, n: total, done: wg}
		}
	}

	for i := 0; i < pathCount; i++ {
		go sendEvents(chans[i], wg)
	}

	for i := 0; i < pathCount; i++ {
		go func(ch chan *inc) {
			for e := range ch {
				handler.Handle(e, D{})
			}
		}(chans[i])
	}

	wg.Wait()
	for i := 0; i < pathCount; i++ {
		close(chans[i])
	}
}

func BenchmarkDSDynamicStream1000x1000x100(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runDynamicStream(1000, 1000, 100)
	}
}

func BenchmarkDSDynamicStream1000000x1000x100(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runDynamicStream(1000000, 100, 100)
	}
}

func BenchmarkDSGroutine1000x1000x100(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runGoroutine(1000, 1000, 100)
	}
}

func BenchmarkDSGroutine1000000x1000x100(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runGoroutine(1000000, 100, 100)
	}
}
