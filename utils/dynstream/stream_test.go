package dynstream

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	timeFormat = "2006-01-02 15:04:05.000"
)

type mockWork interface {
	Do()
}

type mockEvent struct {
	id    int
	path  Path
	sleep time.Duration

	work mockWork

	start *sync.WaitGroup
	done  *sync.WaitGroup
}

func newMockEvent(id int, path Path, sleep time.Duration, work mockWork, start *sync.WaitGroup, done *sync.WaitGroup) *mockEvent {
	e := &mockEvent{id: id, path: path, sleep: sleep, work: work, start: start, done: done}
	if e.start != nil {
		e.start.Add(1)
	}
	if e.done != nil {
		e.done.Add(1)
	}
	return e
}

func (e *mockEvent) Id() int       { return e.id }
func (e *mockEvent) Path() Path    { return e.path }
func (e *mockEvent) Weight() int64 { return 1 }

type mockHandler struct{}

func (h *mockHandler) Handle(event *EventWrap[*mockEvent, any]) {
	if event.event.start != nil {
		event.event.start.Done()
	}

	if event.event.sleep > 0 {
		time.Sleep(event.event.sleep)
	}

	event.event.work.Do()

	if event.event.done != nil {
		event.event.done.Done()
	}
}

type Inc struct {
	num int
	inc *atomic.Int32
}

func (i *Inc) Do() {
	i.inc.Add(int32(i.num))
}

func TestStreamBasic(t *testing.T) {
	handler := &mockHandler{}
	reportChan := make(chan *streamStat, 10)
	streamStats := make([]*streamStat, 0)
	statWait := sync.WaitGroup{}
	statWait.Add(1)
	go func() {
		defer statWait.Done()

		// Wait for 3 rounds, 8*3=24ms
		stop := time.NewTimer(30 * time.Millisecond)
		for {
			select {
			case stat := <-reportChan:
				streamStats = append(streamStats, stat)
			case <-stop.C:
				return
			}
		}
	}()

	p1 := &pathInfo[*mockEvent, any]{path: Path("p1"), dest: "d1"}
	p2 := &pathInfo[*mockEvent, any]{path: Path("p2"), dest: "d2"}
	s1 := newStream(1 /*id*/, 1*time.Millisecond, 8*time.Millisecond /*reportInterval*/, []*pathInfo[*mockEvent, any]{p1}, handler, reportChan)
	s2 := newStream(2 /*id*/, 1*time.Millisecond, 8*time.Millisecond /*reportInterval*/, []*pathInfo[*mockEvent, any]{p2}, handler, reportChan)

	s1.start()
	s2.start()

	incr := &atomic.Int32{}

	eventDone := &sync.WaitGroup{}
	event1 := &EventWrap[*mockEvent, any]{event: newMockEvent(1, Path("p1"), 10*time.Millisecond /*sleep*/, &Inc{num: 1, inc: incr}, nil, eventDone), pathInfo: p1}
	event2 := &EventWrap[*mockEvent, any]{event: newMockEvent(2, Path("p2"), 10*time.Millisecond /*sleep*/, &Inc{num: 2, inc: incr}, nil, eventDone), pathInfo: p2}
	event3 := &EventWrap[*mockEvent, any]{event: newMockEvent(3, Path("p1"), 10*time.Millisecond /*sleep*/, &Inc{num: 3, inc: incr}, nil, eventDone), pathInfo: p1}
	event4 := &EventWrap[*mockEvent, any]{event: newMockEvent(4, Path("p2"), 10*time.Millisecond /*sleep*/, &Inc{num: 4, inc: incr}, nil, eventDone), pathInfo: p2}

	s1.in() <- event1
	s1.in() <- event3

	s2.in() <- event2
	s2.in() <- event4

	eventDone.Wait()

	assert.Equal(t, int32(10), incr.Load())

	statWait.Wait()
	s1.close()
	s2.close()

	assert.Equal(t, 3*2, len(streamStats))

	s1Stat := make([]*streamStat, 0, 3)
	s2Stat := make([]*streamStat, 0, 3)
	for _, stat := range streamStats {
		if stat.id == 1 {
			s1Stat = append(s1Stat, stat)
		} else {
			s2Stat = append(s2Stat, stat)
		}
	}

	assert.Equal(t, 3, len(s1Stat))
	assert.Equal(t, 3, len(s2Stat))

Loop:
	for {
		// Drain the reportChan
		select {
		case <-reportChan:
		default:
			break Loop
		}
	}

	s3 := newStream(3 /*id*/, 1*time.Millisecond, 8*time.Millisecond /*reportInterval*/, []*pathInfo[*mockEvent, any]{p1, p2}, handler, reportChan)
	s3.start(s1, s2)

	eventDone = &sync.WaitGroup{}
	event5 := &EventWrap[*mockEvent, any]{event: newMockEvent(5, Path("p1"), 0 /*sleep*/, &Inc{num: 5, inc: incr}, nil, eventDone), pathInfo: p1}
	event6 := &EventWrap[*mockEvent, any]{event: newMockEvent(6, Path("p2"), 0 /*sleep*/, &Inc{num: 6, inc: incr}, nil, eventDone), pathInfo: p2}
	event7 := &EventWrap[*mockEvent, any]{event: newMockEvent(7, Path("p1"), 0 /*sleep*/, &Inc{num: 7, inc: incr}, nil, eventDone), pathInfo: p1}
	event8 := &EventWrap[*mockEvent, any]{event: newMockEvent(8, Path("p2"), 0 /*sleep*/, &Inc{num: 8, inc: incr}, nil, eventDone), pathInfo: p2}

	s3.in() <- event5
	s3.in() <- event6
	s3.in() <- event7
	s3.in() <- event8

	eventDone.Wait()

	stat := <-reportChan
	assert.Equal(t, int64(3), stat.id)
	assert.Equal(t, int64(0), stat.queueLen)
	assert.Equal(t, int64(4), stat.handleCountOnNum)
}

func TestStreamMerge(t *testing.T) {
	handler := &mockHandler{}
	reportChan := make(chan *streamStat, 10)

	p1 := &pathInfo[*mockEvent, any]{path: Path("p1"), dest: "d1"}
	p2 := &pathInfo[*mockEvent, any]{path: Path("p2"), dest: "d2"}
	s1 := newStream(1 /*id*/, 1*time.Millisecond, 8*time.Millisecond /*reportInterval*/, []*pathInfo[*mockEvent, any]{p1}, handler, reportChan)
	s2 := newStream(2 /*id*/, 1*time.Millisecond, 8*time.Millisecond /*reportInterval*/, []*pathInfo[*mockEvent, any]{p2}, handler, reportChan)

	s1.start()
	s2.start()

	incr := &atomic.Int32{}

	secondStart := &sync.WaitGroup{}
	event1 := &EventWrap[*mockEvent, any]{event: newMockEvent(1, Path("p1"), 0*time.Millisecond /*sleep*/, &Inc{num: 1, inc: incr}, nil, nil), pathInfo: p1}
	event2 := &EventWrap[*mockEvent, any]{event: newMockEvent(2, Path("p2"), 0*time.Millisecond /*sleep*/, &Inc{num: 2, inc: incr}, nil, nil), pathInfo: p2}
	event3 := &EventWrap[*mockEvent, any]{event: newMockEvent(3, Path("p1"), 50*time.Millisecond /*sleep*/, &Inc{num: 3, inc: incr}, secondStart, nil), pathInfo: p1}
	event4 := &EventWrap[*mockEvent, any]{event: newMockEvent(4, Path("p2"), 50*time.Millisecond /*sleep*/, &Inc{num: 4, inc: incr}, secondStart, nil), pathInfo: p2}

	s1.in() <- event1
	s1.in() <- event3

	s2.in() <- event2
	s2.in() <- event4

	secondStart.Wait()

	s3 := newStream(3 /*id*/, 1*time.Millisecond, 8*time.Millisecond /*reportInterval*/, []*pathInfo[*mockEvent, any]{p1, p2}, handler, reportChan)
	s3.start(s1, s2)

	s3.prepareDone.Wait()

	rt := s3.runningTasks

	assert.Equal(t, 2, len(rt))
	assert.Equal(t, Path("p1"), rt[0].path)
	assert.Equal(t, Path("p2"), rt[1].path)

}
