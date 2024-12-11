package dynstream

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type mockWork interface {
	Do()
}

type mockEvent struct {
	id    int
	path  string
	sleep time.Duration

	work mockWork

	start *sync.WaitGroup
	done  *sync.WaitGroup
}

func newMockEvent(id int, path string, sleep time.Duration, work mockWork, start *sync.WaitGroup, done *sync.WaitGroup) *mockEvent {
	e := &mockEvent{id: id, path: path, sleep: sleep, work: work, start: start, done: done}
	if e.start != nil {
		e.start.Add(1)
	}
	if e.done != nil {
		e.done.Add(1)
	}
	return e
}

type mockHandler struct {
	droppedEvents []*mockEvent
}

func (h *mockHandler) Path(event *mockEvent) string {
	return event.path
}

func (h *mockHandler) Handle(dest any, events ...*mockEvent) (await bool) {
	event := events[0]
	if event.start != nil {
		event.start.Done()
	}

	if event.sleep > 0 {
		time.Sleep(event.sleep)
	}

	if event.work != nil {
		event.work.Do()
	}

	if event.done != nil {
		event.done.Done()
	}

	return false
}

func (h *mockHandler) GetSize(event *mockEvent) int            { return 0 }
func (h *mockHandler) GetArea(path string, dest any) int       { return 0 }
func (h *mockHandler) GetTimestamp(event *mockEvent) Timestamp { return 0 }
func (h *mockHandler) GetType(event *mockEvent) EventType      { return DefaultEventType }
func (h *mockHandler) IsPaused(event *mockEvent) bool          { return false }
func (h *mockHandler) OnDrop(event *mockEvent) {
	h.droppedEvents = append(h.droppedEvents, event)
}
func (h *mockHandler) drainDroppedEvents() []*mockEvent {
	events := h.droppedEvents
	h.droppedEvents = nil
	return events
}

type Inc struct {
	num int64
	inc *atomic.Int64
}

func (i *Inc) Do() {
	i.inc.Add(i.num)
}

func TestStreamBasic(t *testing.T) {
	handler := &mockHandler{}
	option := NewOption()
	option.ReportInterval = 8 * time.Millisecond
	reportChan := make(chan streamStat[int, string, *mockEvent, any, *mockHandler], 10)
	stats := make([]streamStat[int, string, *mockEvent, any, *mockHandler], 0)
	statWait := sync.WaitGroup{}
	statWait.Add(1)
	go func() {
		defer statWait.Done()

		// Wait for 3 rounds
		// stop := time.NewTimer(option.ReportInterval*3 + option.ReportInterval/2)
		for {
			if len(stats) == 3*2 {
				return
			}
			stat := <-reportChan
			stats = append(stats, stat)
			// case <-stop.C:
			// 	return
		}
	}()

	p1 := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "p1", "d1")
	p2 := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "p2", "d2")
	p3 := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "p3", "d3")
	s1 := newStream(1 /*id*/, handler, reportChan, 10, option)
	s2 := newStream(2 /*id*/, handler, reportChan, 10, option)

	s1.start([]*pathInfo[int, string, *mockEvent, any, *mockHandler]{p1})
	s2.start([]*pathInfo[int, string, *mockEvent, any, *mockHandler]{p2})

	incr := &atomic.Int64{}

	eventDone := &sync.WaitGroup{}
	event1 := eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(1, "p1", 10*time.Millisecond /*sleep*/, &Inc{num: 1, inc: incr}, nil, eventDone), pathInfo: p1}
	event2 := eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(2, "p2", 10*time.Millisecond /*sleep*/, &Inc{num: 2, inc: incr}, nil, eventDone), pathInfo: p2}
	event3 := eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(3, "p1", 10*time.Millisecond /*sleep*/, &Inc{num: 3, inc: incr}, nil, eventDone), pathInfo: p1}
	event4 := eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(4, "p2", 10*time.Millisecond /*sleep*/, &Inc{num: 4, inc: incr}, nil, eventDone), pathInfo: p2}

	s1.in().Push(event1)
	s1.in().Push(event3)

	s2.in().Push(event2)
	s2.in().Push(event4)

	eventDone.Wait()

	assert.Equal(t, int64(10), incr.Load())

	statWait.Wait()
	s1.close()
	s2.close()

	assert.Equal(t, 3*2, len(stats))

	s1Stat := make([]streamStat[int, string, *mockEvent, any, *mockHandler], 0, 3)
	s2Stat := make([]streamStat[int, string, *mockEvent, any, *mockHandler], 0, 3)
	for _, stat := range stats {
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

	option = NewOption()
	option.ReportInterval = 1 * time.Hour /*don't report*/
	s3 := newStream(3 /*id*/, handler, reportChan, 10, option)
	s3.start([]*pathInfo[int, string, *mockEvent, any, *mockHandler]{p1, p2}, s1, s2)

	eventDone = &sync.WaitGroup{}
	event5 := eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(5, "p1", 0 /*sleep*/, &Inc{num: 5, inc: incr}, nil, eventDone), pathInfo: p1}
	event6 := eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(6, "p2", 0 /*sleep*/, &Inc{num: 6, inc: incr}, nil, eventDone), pathInfo: p2}
	event7 := eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(7, "p1", 0 /*sleep*/, &Inc{num: 7, inc: incr}, nil, eventDone), pathInfo: p1}
	event8 := eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(8, "p2", 0 /*sleep*/, &Inc{num: 8, inc: incr}, nil, eventDone), pathInfo: p2}
	event9 := eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(9, "p3", 0 /*sleep*/, &Inc{num: 9, inc: incr}, nil, eventDone), pathInfo: p3}
	event10 := eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(10, "p2", 0 /*sleep*/, &Inc{num: 10, inc: incr}, nil, eventDone), pathInfo: p2}

	s3.in().Push(event5)
	s3.in().Push(event6)
	s3.in().Push(event7)
	s3.in().Push(event8)
	s3.in().Push(event9)
	s3.in().Push(event10)

	eventDone.Wait()
	s3.close(true)
	close(reportChan)

	assert.Equal(t, int64(1+2+3+4+5+6+7+8+9+10), incr.Load())

	stats = stats[:0]
	for stat := range reportChan {
		stats = append(stats, stat)
	}
	assert.Equal(t, 1, len(stats))

	stat := stats[0]
	assert.Equal(t, 3, stat.id)
	assert.Equal(t, 6, stat.count)

	assert.Equal(t, 3, stat.mostBusyPath.Len())
	top, ok := stat.mostBusyPath.PopTop()
	assert.True(t, ok)
	assert.Equal(t, "p3", top.pathInfo.path)
	_, ok = stat.mostBusyPath.PopTop()
	assert.True(t, ok)
	// assert.Equal(t, Path("p1"), top.pathInfo.path)
	_, ok = stat.mostBusyPath.PopTop()
	assert.True(t, ok)
	// assert.Equal(t, Path("p2"), top.pathInfo.path)
}

func TestStreamMerge(t *testing.T) {
	handler := &mockHandler{}
	reportChan := make(chan streamStat[int, string, *mockEvent, any, *mockHandler], 10)

	p1 := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "p1", "d1")
	p2 := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "p2", "d2")
	option := NewOption()
	option.ReportInterval = 1 * time.Hour /*don't report*/
	s1 := newStream(1 /*id*/, handler, reportChan, 10, option)
	s2 := newStream(2 /*id*/, handler, reportChan, 10, option)

	s1.start([]*pathInfo[int, string, *mockEvent, any, *mockHandler]{p1})
	s2.start([]*pathInfo[int, string, *mockEvent, any, *mockHandler]{p2})

	incr := &atomic.Int64{}

	wg := &sync.WaitGroup{}

	s1.in().Push(eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(1, "p1", 0*time.Millisecond /*sleep*/, &Inc{num: 1, inc: incr}, nil, nil), pathInfo: p1})
	s1.in().Push(eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(3, "p1", 50*time.Millisecond /*sleep*/, &Inc{num: 3, inc: incr}, wg, nil), pathInfo: p1})

	s2.in().Push(eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(2, "p2", 0*time.Millisecond /*sleep*/, &Inc{num: 2, inc: incr}, nil, nil), pathInfo: p2})
	s2.in().Push(eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(4, "p2", 50*time.Millisecond /*sleep*/, &Inc{num: 4, inc: incr}, wg, nil), pathInfo: p2})

	wg.Wait()

	s3 := newStream(3 /*id*/, handler, reportChan, 10, option)
	s3.start([]*pathInfo[int, string, *mockEvent, any, *mockHandler]{p1, p2}, s1, s2)

	wg = &sync.WaitGroup{}
	s3.in().Push(eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(5, "p2", 50*time.Millisecond /*sleep*/, &Inc{num: 5, inc: incr}, wg, nil), pathInfo: p2})
	s3.in().Push(eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(6, "p2", 50*time.Millisecond /*sleep*/, &Inc{num: 6, inc: incr}, wg, nil), pathInfo: p2})
	s3.in().Push(eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(7, "p2", 50*time.Millisecond /*sleep*/, &Inc{num: 7, inc: incr}, wg, nil), pathInfo: p2})

	wg.Wait()
	s3.close()

	assert.Equal(t, int64(1+2+3+4+5+6+7), incr.Load())

	close(reportChan)

	stats := make([]streamStat[int, string, *mockEvent, any, *mockHandler], 0)
	for stat := range reportChan {
		stats = append(stats, stat)
	}
	assert.Equal(t, 3, len(stats))

	stat := stats[2]
	assert.Equal(t, 3, stat.id)
	assert.Equal(t, 3, stat.count)

	assert.Equal(t, 1, stat.mostBusyPath.Len())
	top, ok := stat.mostBusyPath.PopTop()
	assert.True(t, ok)
	assert.Equal(t, "p2", top.pathInfo.path)
}

func TestStreamManyEvents(t *testing.T) {
	handler := &mockHandler{}
	reportChan := make(chan streamStat[int, string, *mockEvent, any, *mockHandler], 10)

	p1 := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "p1", "d1")
	option := NewOption()
	option.ReportInterval = 1 * time.Hour
	s1 := newStream(1 /*id*/, handler, reportChan, 10, option)
	s1.start([]*pathInfo[int, string, *mockEvent, any, *mockHandler]{p1})

	incr := &atomic.Int64{}
	wg := &sync.WaitGroup{}
	total := 100000
	for i := 0; i < total; i++ {
		s1.in().Push(eventWrap[int, string, *mockEvent, any, *mockHandler]{
			event: newMockEvent(i, "p1", 0 /*sleep*/, &Inc{num: 1, inc: incr}, nil, wg), pathInfo: p1})
	}
	wg.Wait()
	s1.close()

	assert.Equal(t, int64(total), incr.Load())

	close(reportChan)
	stats := make([]streamStat[int, string, *mockEvent, any, *mockHandler], 0)
	for stat := range reportChan {
		stats = append(stats, stat)
	}
	assert.Equal(t, 1, len(stats))
	stat := stats[0]
	assert.Equal(t, 1, stat.id)
	assert.Equal(t, total, stat.count)
}

func TestAA(t *testing.T) {
	f1 := 0.9
	f2 := 1.0

	a := 100
	assert.Equal(t, 90, int(float64(a)*f1))
	assert.Equal(t, 100, int(float64(a)*f2))
}
