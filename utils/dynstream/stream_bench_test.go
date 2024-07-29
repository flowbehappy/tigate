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
}

func (e *inc) Path() Path    { return "" }
func (e *inc) Weight() int64 { return 1 }

type incHandler struct{}

func (h *incHandler) Handle(event *EventWrap[*inc, any]) {
	for i := 0; i < event.event.times; i++ {
		event.event.n.Add(1)
	}
	event.event.done.Done()
}

func runStream(eventCount int, times int) {
	handler := &incHandler{}
	reportChan := make(chan *streamStat, 100)

	pi := &pathInfo[*inc, any]{path: Path("p1"), dest: "d1"}
	stream := newStream(1 /*id*/, 1*time.Millisecond, 8*time.Millisecond /*reportInterval*/, []*pathInfo[*inc, any]{pi}, handler, reportChan)
	stream.start()

	go func() {
		// Drain the report channel. To avoid the report channel blocking.
		for {
			_, ok := <-reportChan
			if !ok {
				return
			}
		}
	}()

	total := &atomic.Int64{}
	done := &sync.WaitGroup{}

	done.Add(eventCount)
	for i := 0; i < eventCount; i++ {
		stream.in() <- &EventWrap[*inc, any]{event: &inc{times: times, n: total, done: done}, pathInfo: pi}
	}

	done.Wait()
	close(reportChan)
}

func runLoop(eventCount int, times int) {
	total := &atomic.Int64{}
	for i := 0; i < eventCount; i++ {
		for j := 0; j < times; j++ {
			total.Add(1)
		}
	}
}

func BenchmarkStream1000x1(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runStream(1000, 1)
	}
}

func BenchmarkStream1000x100(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runStream(1000, 100)
	}
}

func BenchmarkStream100000x100(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runStream(100000, 100)
	}
}

func BenchmarkLoop1000x1(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runLoop(1000, 1)
	}
}

func BenchmarkLoop1000x100(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runLoop(1000, 100)
	}
}

func BenchmarkLoop100000x100(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runLoop(100000, 100)
	}
}
