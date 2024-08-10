package dynstream

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type inc struct {
	times int
	n     *atomic.Int64
	done  *sync.WaitGroup

	path string
}

func (e *inc) Path() string { return e.path }

type D struct{}
type incHandler struct{}

func (h *incHandler) Handle(event *inc, dest D) {
	for i := 0; i < event.times; i++ {
		event.n.Add(1)
	}
	event.done.Done()
}

func runStream(eventCount int, times int) {
	handler := &incHandler{}
	reportChan := make(chan streamStat[string, *inc, D], 100)

	pi := newPathInfo[string, *inc, D]("p1", D{})
	stream := newStream[string, *inc, D](1 /*id*/, handler, reportChan, 8*time.Millisecond /*reportInterval*/, 10)
	stream.start([]*pathInfo[string, *inc, D]{pi})

	go func() {
		// Drain the report channel. To avoid the report channel blocking.
		for range reportChan {
		}
	}()

	total := &atomic.Int64{}
	done := &sync.WaitGroup{}

	done.Add(eventCount)
	for i := 0; i < eventCount; i++ {
		stream.in() <- eventWrap[string, *inc, D]{event: &inc{times: times, n: total, done: done}, pathInfo: pi}
	}

	done.Wait()
	stream.close()

	close(reportChan)
}

func runLoop(eventCount int, times int) {
	total := &atomic.Int64{}
	done := &sync.WaitGroup{}

	done.Add(eventCount)

	signal := make(chan struct{}, 100)

	go func() {
		for range signal {
			for j := 0; j < times; j++ {
				total.Add(1)
			}
			done.Done()
		}
	}()

	for i := 0; i < eventCount; i++ {
		signal <- struct{}{}
	}
	done.Wait()
}

func BenchmarkSStream1000x1(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runStream(1000, 1)
	}
}

func BenchmarkSStream1000x100(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runStream(1000, 100)
	}
}

func BenchmarkSStream100000x100(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runStream(100000, 100)
	}
}

func BenchmarkSLoop1000x1(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runLoop(1000, 1)
	}
}

func BenchmarkSLoop1000x100(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runLoop(1000, 100)
	}
}

func BenchmarkSLoop100000x100(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runLoop(100000, 100)
	}
}

/*
goos: darwin
goarch: amd64
pkg: github.com/flowbehappy/tigate/utils/dynstream
cpu: Intel(R) Core(TM) i5-8500 CPU @ 3.00GHz
BenchmarkSStream1000x1-6                     	    1842	    687181 ns/op	  309550 B/op	    6098 allocs/op
BenchmarkSStream1000x100-6                   	    1192	   1076355 ns/op	  309529 B/op	    6097 allocs/op
BenchmarkSStream100000x100-6                 	      12	 104457546 ns/op	30560586 B/op	  606404 allocs/op
BenchmarkSLoop1000x1-6                       	   17620	     68181 ns/op	     264 B/op	       5 allocs/op
BenchmarkSLoop1000x100-6                     	    2115	    566871 ns/op	     263 B/op	       4 allocs/op
BenchmarkSLoop100000x100-6                   	      20	  56798304 ns/op	     264 B/op	       5 allocs/op
*/
