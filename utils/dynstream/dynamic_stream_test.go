package dynstream

import (
	"sync"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

type simpleEvent struct {
	path  string
	sleep time.Duration
	wg    *sync.WaitGroup
}

func newSimpleEvent(path string, wg *sync.WaitGroup) *simpleEvent {
	wg.Add(1)
	return &simpleEvent{path: path, wg: wg}
}

func newSimpleEventSleep(path string, wg *sync.WaitGroup, sleep time.Duration) *simpleEvent {
	wg.Add(1)
	return &simpleEvent{path: path, sleep: sleep, wg: wg}
}

type simpleHandler struct{}

func (h *simpleHandler) Path(event *simpleEvent) string {
	return event.path
}
func (h *simpleHandler) Handle(dest struct{}, events ...*simpleEvent) (await bool) {
	for _, event := range events {
		if event.sleep > 0 {
			time.Sleep(event.sleep)
		}
		if event.wg != nil {
			event.wg.Done()
		}
	}
	return false
}

func TestDynamicStreamBasic(t *testing.T) {
	handler := &simpleHandler{}
	ds := NewDynamicStream(handler, DefaultSchedulerInterval, DefaultReportInterval, 3, Option{BatchSize: 2})
	ds.Start()

	ds.AddPaths([]PathAndDest[string, struct{}]{
		{"path1", struct{}{}},
		{"path2", struct{}{}},
		{"path3", struct{}{}},
		{"path4", struct{}{}},
	}...)

	wg := &sync.WaitGroup{}
	ds.In() <- newSimpleEvent("path1", wg)
	ds.In() <- newSimpleEvent("path2", wg)
	ds.In() <- newSimpleEvent("path3", wg)
	ds.In() <- newSimpleEvent("path3", wg)
	ds.In() <- newSimpleEvent("path3", wg)
	ds.In() <- newSimpleEvent("path4", wg)

	wg.Wait()

	ds.RemovePaths("path1", "path2", "path3", "path4")

	ds.Close()
}

func TestDynamicStreamSchedule(t *testing.T) {
	handler := &simpleHandler{}
	ds := newDynamicStreamImpl(handler, 1, 1*time.Hour, 1*time.Hour, 3)
	ds.Start()

	scheduleNow := func(rule ruleType, period time.Duration) {
		r := &reportAndScheduleCmd{rule: rule, period: period}
		r.wg.Add(1)
		ds.cmdToSchd <- &cmd{cmdType: typeReportAndSchedule, cmd: r}
		r.wg.Wait()
	}

	ds.AddPaths([]PathAndDest[string, struct{}]{
		{"p1", struct{}{}},
		{"p2", struct{}{}},
		{"p3", struct{}{}},
		{"p4", struct{}{}},
		{"p5", struct{}{}},
	}...)

	assert.Equal(t, 3, len(ds.streamInfos))
	assert.Equal(t, 2, len(ds.streamInfos[0].pathMap)) // p1, p4 round-robin, the first stream has 2 paths
	assert.Equal(t, 2, len(ds.streamInfos[1].pathMap)) // p2, p5
	assert.Equal(t, 1, len(ds.streamInfos[2].pathMap)) // p3

	wg := &sync.WaitGroup{}
	ds.In() <- newSimpleEvent("p1", wg)
	ds.In() <- newSimpleEvent("p2", wg)
	ds.In() <- newSimpleEvent("p3", wg)
	ds.In() <- newSimpleEventSleep("p4", wg, 8*time.Millisecond)
	ds.In() <- newSimpleEventSleep("p5", wg, 8*time.Millisecond)
	wg.Wait()

	scheduleNow(createSoloPath, 8*time.Millisecond)

	// path4 is very busy, so it should be moved to a solo stream
	assert.Equal(t, 5, len(ds.streamInfos))
	assert.Equal(t, 1, len(ds.streamInfos[0].pathMap)) // p1
	assert.Equal(t, 1, len(ds.streamInfos[1].pathMap)) // p2
	assert.Equal(t, 1, len(ds.streamInfos[2].pathMap)) // p3
	assert.Equal(t, 1, len(ds.streamInfos[3].pathMap)) // p4, Solo stream
	assert.Equal(t, 1, len(ds.streamInfos[4].pathMap)) // p5, Solo stream

	wg = &sync.WaitGroup{}
	ds.In() <- newSimpleEvent("p1", wg)
	ds.In() <- newSimpleEvent("p2", wg)
	ds.In() <- newSimpleEvent("p3", wg)
	ds.In() <- newSimpleEvent("p4", wg)
	ds.In() <- newSimpleEventSleep("p5", wg, 8*time.Millisecond)
	// time.Sleep(8 * time.Millisecond)
	wg.Wait()

	scheduleNow(removeSoloPath, 8*time.Millisecond)

	// path4 is idle now, so it should be moved back to the the first stream
	// path5 is still busy, it remains in a solo stream
	assert.Equal(t, 4, len(ds.streamInfos))
	assert.Equal(t, 2, len(ds.streamInfos[0].pathMap)) // p1, p4
	assert.Equal(t, 1, len(ds.streamInfos[1].pathMap)) // p2
	assert.Equal(t, 1, len(ds.streamInfos[2].pathMap)) // p3
	assert.Equal(t, 1, len(ds.streamInfos[3].pathMap)) // p5, Solo stream

	ds.AddPaths([]PathAndDest[string, struct{}]{
		{"p6", struct{}{}},
		{"p7", struct{}{}},
		{"p8", struct{}{}},
		{"p9", struct{}{}},
		{"p10", struct{}{}},
	}...)

	scheduleNow(shuffleStreams, 8*time.Millisecond)
	// scheduleNow(createSoloPath, 10 * time.Millisecond)

	assert.Equal(t, 4, len(ds.streamInfos))
	assert.Equal(t, 4, len(ds.streamInfos[0].pathMap)) // p1, p4, p7, p10
	assert.Equal(t, 2, len(ds.streamInfos[1].pathMap)) // p2, p8
	assert.Equal(t, 3, len(ds.streamInfos[2].pathMap)) // p3, p6, p9
	assert.Equal(t, 1, len(ds.streamInfos[3].pathMap)) // p5, Solo stream

	wg = &sync.WaitGroup{}
	ds.In() <- newSimpleEventSleep("p7", wg, 8*time.Millisecond)
	ds.In() <- newSimpleEventSleep("p10", wg, 8*time.Millisecond)
	ds.In() <- newSimpleEventSleep("p9", wg, 8*time.Millisecond)
	// time.Sleep(8 * time.Millisecond)
	wg.Wait()

	scheduleNow(createSoloPath, 8*time.Millisecond)

	assert.Equal(t, 7, len(ds.streamInfos))
	assert.Equal(t, 2, len(ds.streamInfos[0].pathMap)) // p1, p4
	assert.Equal(t, 2, len(ds.streamInfos[1].pathMap)) // p2, p8
	assert.Equal(t, 2, len(ds.streamInfos[2].pathMap)) // p3, p6
	assert.Equal(t, 1, len(ds.streamInfos[3].pathMap)) // p5, Solo stream
	assert.Equal(t, 1, len(ds.streamInfos[4].pathMap)) // p7, Solo stream
	assert.Equal(t, 1, len(ds.streamInfos[5].pathMap)) // p10, Solo stream
	assert.Equal(t, 1, len(ds.streamInfos[6].pathMap)) // p9, Solo stream

	// Do nothing, and all paths are idle
	scheduleNow(removeSoloPath, 0)
	assert.Equal(t, 3, len(ds.streamInfos))
	assert.Equal(t, 6, len(ds.streamInfos[0].pathMap)) // p1, p4, p5, p7, p10, p9
	assert.Equal(t, 2, len(ds.streamInfos[1].pathMap)) // p2, p8
	assert.Equal(t, 2, len(ds.streamInfos[2].pathMap)) // p3, p6

	wg = &sync.WaitGroup{}
	ds.In() <- newSimpleEventSleep("p5", wg, 2*time.Millisecond)
	ds.In() <- newSimpleEventSleep("p7", wg, 2*time.Millisecond)
	ds.In() <- newSimpleEventSleep("p10", wg, 2*time.Millisecond)
	ds.In() <- newSimpleEventSleep("p9", wg, 2*time.Millisecond)
	// time.Sleep(10 * time.Millisecond)
	wg.Wait()

	scheduleNow(shuffleStreams, 8*time.Millisecond)
	assert.Equal(t, 3, len(ds.streamInfos))
	assert.Equal(t, 4, len(ds.streamInfos[0].pathMap)) // the paths are shuffled, we can't predict the exact paths anymore
	assert.Equal(t, 4, len(ds.streamInfos[1].pathMap))
	assert.Equal(t, 2, len(ds.streamInfos[2].pathMap))

	ds.Close()
}

type removePathHandler struct {
	ds DynamicStream[string, *simpleEvent, struct{}]
}

func (h *removePathHandler) Path(event *simpleEvent) string {
	return event.path
}

func (h *removePathHandler) Handle(dest struct{}, events ...*simpleEvent) (await bool) {
	event := events[0]
	h.ds.RemovePaths(event.path)
	event.wg.Done()
	return false
}

func TestDynamicStreamRemovePath(t *testing.T) {
	handler := &removePathHandler{}
	ds := newDynamicStreamImpl(handler, 1, 1*time.Hour, 1*time.Hour, 3)
	handler.ds = ds

	ds.Start()

	ds.AddPaths([]PathAndDest[string, struct{}]{
		{"p1", struct{}{}},
		{"p2", struct{}{}}}...)

	wg := &sync.WaitGroup{}
	wg.Add(1) // Only one event is processed
	ds.In() <- &simpleEvent{path: "p1", wg: wg}
	ds.In() <- &simpleEvent{path: "p1", wg: wg}

	// Make sure the two events are put in a stream already.
	time.Sleep(10 * time.Millisecond)
	wg.Wait()
}
