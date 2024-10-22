package dynstream

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/flowbehappy/tigate/pkg/apperror"
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

type simpleHandler struct {
	t *testing.T
}

func (h *simpleHandler) Path(event *simpleEvent) string {
	return event.path
}
func (h *simpleHandler) Handle(dest struct{}, events ...*simpleEvent) (await bool) {
	for _, event := range events {
		if h.t != nil {
			h.t.Log("Before Handle ", event.path)
		}
		if event.sleep > 0 {
			time.Sleep(event.sleep)
		}
		if event.wg != nil {
			event.wg.Done()
		}
		if h.t != nil {
			h.t.Log("After Handle ", event.path)
		}
	}
	return false
}

func (h *simpleHandler) GetSize(event *simpleEvent) int            { return 0 }
func (h *simpleHandler) GetArea(path string) int                   { return 0 }
func (h *simpleHandler) GetTimestamp(event *simpleEvent) Timestamp { return 0 }
func (h *simpleHandler) GetType(event *simpleEvent) EventType      { return 0 }

func (h *simpleHandler) OnDrop(event *simpleEvent) {
	if h.t != nil {
		h.t.Log("OnDrop ", event.path)
	}
}

func TestDynamicStreamBasic(t *testing.T) {
	handler := &simpleHandler{}
	option := NewOption()
	option.StreamCount = 1
	option.BatchCount = 2
	ds := NewDynamicStream(handler, option)
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

	ds.RemovePaths("path1", "path2", "path3")

	wg = &sync.WaitGroup{}
	ds.In() <- newSimpleEvent("path4", wg)
	wg.Wait()

	ds.Close()
}

func TestDynamicStreamAddRemovePaths(t *testing.T) {
	handler := &simpleHandler{}
	ds := NewDynamicStream(handler)
	ds.Start()
	errors := ds.AddPaths([]PathAndDest[string, struct{}]{
		{"path1", struct{}{}},
		{"path2", struct{}{}},
		{"path3", struct{}{}},
		{"path4", struct{}{}},
	}...)
	assert.Nil(t, errors)
	errors = ds.AddPaths([]PathAndDest[string, struct{}]{
		{"path1", struct{}{}},
		{"path3", struct{}{}},
		{"path5", struct{}{}},
	}...)
	assert.Equal(t, 3, len(errors))
	appError, ok := errors[0].(*apperror.AppError)
	assert.True(t, ok)
	assert.Equal(t, apperror.ErrorTypeDuplicate, appError.Type)
	appError, ok = errors[1].(*apperror.AppError)
	assert.True(t, ok)
	assert.Equal(t, apperror.ErrorTypeDuplicate, appError.Type)
	assert.Nil(t, errors[2])

	errors = ds.RemovePaths("path1", "path3")
	assert.Equal(t, 0, len(errors))

	errors = ds.RemovePaths("path2", "path3")
	assert.Equal(t, 2, len(errors))
	assert.Nil(t, errors[0])
	appError, ok = errors[1].(*apperror.AppError)
	assert.True(t, ok)
	assert.Equal(t, apperror.ErrorTypeNotExist, appError.Type)

	ds.Close()
}

// Note that this test is not deterministic because streams are running in separate goroutines.
// Maybe we should disable it in the test.
func TestDynamicStreamSchedule(t *testing.T) {
	t.Skip("Skipping TestDynamicStreamSchedule because it is not deterministic due to streams running in separate goroutines")

	handler := &simpleHandler{
		t: t,
	}
	option := NewOption()
	option.SchedulerInterval = 1 * time.Hour
	option.ReportInterval = 1 * time.Hour
	option.StreamCount = 3
	ds := newDynamicStreamImpl(handler, option)
	ds.Start()

	ds.AddPaths([]PathAndDest[string, struct{}]{
		{"p1", struct{}{}},
		{"p2", struct{}{}},
		{"p3", struct{}{}},
		{"p4", struct{}{}},
		{"p5", struct{}{}},
	}...)

	t.Log("=====1 ")

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

	t.Log("=====2 ")

	ds.reportAndSchedule(createSoloPath, 8*time.Millisecond)

	// path4 and path5 are very busy, so they should become solo streams
	assert.Equal(t, 5, len(ds.streamInfos))
	assert.Equal(t, 1, len(ds.streamInfos[0].pathMap)) // p1
	assert.Equal(t, 1, len(ds.streamInfos[1].pathMap)) // p2
	assert.Equal(t, 1, len(ds.streamInfos[2].pathMap)) // p3
	assert.Equal(t, 1, len(ds.streamInfos[3].pathMap)) // p4, Solo stream
	assert.Equal(t, 1, len(ds.streamInfos[4].pathMap)) // p5, Solo stream

	t.Log("=====3 ")

	wg = &sync.WaitGroup{}
	ds.In() <- newSimpleEvent("p1", wg)
	ds.In() <- newSimpleEvent("p2", wg)
	ds.In() <- newSimpleEvent("p3", wg)
	ds.In() <- newSimpleEvent("p4", wg)
	// ds.In() <- newSimpleEvent("p5", wg)
	ds.In() <- newSimpleEventSleep("p5", wg, 8*time.Millisecond)

	wg.Wait()

	t.Log("=====4 ")

	ds.reportAndSchedule(removeSoloPath, 8*time.Millisecond)

	t.Log("=====5 ")

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

	ds.reportAndSchedule(shuffleStreams, 8*time.Millisecond)
	t.Log("=====6 ")

	assert.Equal(t, 4, len(ds.streamInfos))
	assert.Equal(t, 4, len(ds.streamInfos[0].pathMap)) // p1, p4, p7, p10
	assert.Equal(t, 2, len(ds.streamInfos[1].pathMap)) // p2, p8
	assert.Equal(t, 3, len(ds.streamInfos[2].pathMap)) // p3, p6, p9
	assert.Equal(t, 1, len(ds.streamInfos[3].pathMap)) // p5, Solo stream

	wg = &sync.WaitGroup{}
	// Stream 1
	ds.In() <- newSimpleEventSleep("p7", wg, 8*time.Millisecond)
	ds.In() <- newSimpleEventSleep("p10", wg, 8*time.Millisecond)

	t.Log("=====7 ")

	// Stream 2
	ds.In() <- newSimpleEventSleep("p2", wg, 900*time.Microsecond)
	ds.In() <- newSimpleEventSleep("p8", wg, 900*time.Microsecond)

	// Stream 3
	ds.In() <- newSimpleEventSleep("p9", wg, 8*time.Millisecond)

	wg.Wait()

	ds.reportAndSchedule(createSoloPath, 10*time.Millisecond)
	t.Log("=====8 ")

	assert.Equal(t, 7, len(ds.streamInfos))
	assert.Equal(t, 2, len(ds.streamInfos[0].pathMap)) // p1, p4
	assert.Equal(t, 2, len(ds.streamInfos[1].pathMap)) // p2, p8
	assert.Equal(t, 2, len(ds.streamInfos[2].pathMap)) // p3, p6
	assert.Equal(t, 1, len(ds.streamInfos[3].pathMap)) // p5, Solo stream
	assert.Equal(t, 1, len(ds.streamInfos[4].pathMap)) // p7, Solo stream
	assert.Equal(t, 1, len(ds.streamInfos[5].pathMap)) // p10, Solo stream
	assert.Equal(t, 1, len(ds.streamInfos[6].pathMap)) // p9, Solo stream

	// Do nothing, and all paths are idle
	ds.reportAndSchedule(removeSoloPath, 0)

	t.Log("=====9 ")

	assert.Equal(t, 3, len(ds.streamInfos))
	assert.Equal(t, 6, len(ds.streamInfos[0].pathMap)) // p1, p4, p5, p7, p10, p9
	assert.Equal(t, 2, len(ds.streamInfos[1].pathMap)) // p2, p8
	assert.Equal(t, 2, len(ds.streamInfos[2].pathMap)) // p3, p6

	wg = &sync.WaitGroup{}
	ds.In() <- newSimpleEventSleep("p5", wg, 2*time.Millisecond)
	ds.In() <- newSimpleEventSleep("p7", wg, 2*time.Millisecond)
	ds.In() <- newSimpleEventSleep("p10", wg, 2*time.Millisecond)
	ds.In() <- newSimpleEventSleep("p9", wg, 2*time.Millisecond)

	wg.Wait()

	t.Log("=====10 ")

	ds.reportAndSchedule(shuffleStreams, 8*time.Millisecond)
	assert.Equal(t, 3, len(ds.streamInfos))
	assert.Equal(t, 4, len(ds.streamInfos[0].pathMap)) // the paths are shuffled, we can't predict the exact paths anymore
	assert.Equal(t, 4, len(ds.streamInfos[1].pathMap))
	assert.Equal(t, 2, len(ds.streamInfos[2].pathMap))

	ds.AddPath("p11", struct{}{})
	wg = &sync.WaitGroup{}
	ds.In() <- newSimpleEventSleep("p10", wg, 8*time.Millisecond)
	ds.In() <- newSimpleEventSleep("p11", wg, 8*time.Millisecond)
	wg.Wait()

	ds.reportAndSchedule(createSoloPath, 8*time.Millisecond)
	t.Log("=====11 ")

	assert.Equal(t, 5, len(ds.streamInfos))
	assert.Equal(t, 1, len(ds.streamInfos[3].pathMap)) // p10, Solo stream
	assert.Equal(t, 1, len(ds.streamInfos[4].pathMap)) // p11, Solo stream

	ds.RemovePath("p10")
	wg = &sync.WaitGroup{}
	ds.In() <- newSimpleEventSleep("p10", wg, 8*time.Millisecond) // This event is dropped by DS
	ds.In() <- newSimpleEventSleep("p11", wg, 8*time.Millisecond)
	wg.Done() // Manually finish the first event
	wg.Wait()

	t.Log("=====12 ")

	ds.reportAndSchedule(removeSoloPath, 8*time.Millisecond)

	assert.Equal(t, 4, len(ds.streamInfos))
	assert.Equal(t, 3+4+2, len(ds.streamInfos[0].pathMap)+len(ds.streamInfos[1].pathMap)+len(ds.streamInfos[2].pathMap))
	assert.Equal(t, 1, len(ds.streamInfos[3].pathMap)) // p11, Solo stream

	ds.Close()
}

type removePathHandler struct {
	ds DynamicStream[int, string, *simpleEvent, struct{}, *removePathHandler]
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

func (h *removePathHandler) GetSize(event *simpleEvent) int            { return 0 }
func (h *removePathHandler) GetArea(path string) int                   { return 0 }
func (h *removePathHandler) GetTimestamp(event *simpleEvent) Timestamp { return 0 }
func (h *removePathHandler) GetType(event *simpleEvent) EventType      { return 0 }
func (h *removePathHandler) OnDrop(event *simpleEvent)                 {}

func TestDynamicStreamRemovePath(t *testing.T) {
	handler := &removePathHandler{}
	option := NewOption()
	option.SchedulerInterval = 1 * time.Hour
	option.ReportInterval = 1 * time.Hour
	option.StreamCount = 3
	ds := NewDynamicStream(handler, option)
	handler.ds = ds

	ds.Start()

	ds.AddPaths([]PathAndDest[string, struct{}]{
		{"p1", struct{}{}},
		{"p2", struct{}{}}}...)

	wg := &sync.WaitGroup{}
	wg.Add(1) // Only one event is processed
	ds.In() <- &simpleEvent{path: "p1", wg: wg}
	ds.In() <- &simpleEvent{path: "p1", wg: wg}

	// The case is good if it doesn't panic
	// Sleep is to make sure events are actually processed
	time.Sleep(10 * time.Millisecond)

	wg.Wait()

	ds.Close()
}

type incEvent struct {
	path  string
	total *atomic.Int64
	inc   int64
	wg    *sync.WaitGroup
}

type incEventHandler struct{}

func (h *incEventHandler) OnDrop(event incEvent) {
	event.wg.Done()
}

func (h *incEventHandler) Path(event incEvent) string {
	return event.path
}
func (h *incEventHandler) Handle(dest struct{}, events ...incEvent) (await bool) {
	for _, event := range events {
		event.total.Add(event.inc)
		event.wg.Done()
	}
	return false
}

func (h *incEventHandler) GetSize(event incEvent) int            { return 0 }
func (h *incEventHandler) GetArea(path string) int               { return 0 }
func (h *incEventHandler) GetTimestamp(event incEvent) Timestamp { return 0 }
func (h *incEventHandler) GetType(event incEvent) EventType      { return 0 }

func TestDynamicStreamDrop(t *testing.T) {
	check := func(option Option) int64 {
		option.handleWait = &sync.WaitGroup{}
		option.handleWait.Add(1)

		handler := &incEventHandler{}
		ds := NewDynamicStream(handler, option)
		ds.Start()

		ds.AddPath("p1", struct{}{})
		total := &atomic.Int64{}

		eventCountDown := &sync.WaitGroup{}
		eventCountDown.Add(3)
		ds.In() <- incEvent{path: "p1", total: total, inc: 1, wg: eventCountDown}
		ds.In() <- incEvent{path: "p1", total: total, inc: 3, wg: eventCountDown}
		ds.In() <- incEvent{path: "p1", total: total, inc: 5, wg: eventCountDown}

		time.Sleep(10 * time.Millisecond) // Make sure all the events are in the pending queue or dropped
		option.handleWait.Done()
		eventCountDown.Wait()

		ds.Close()

		return total.Load()
	}

	option := NewOption()

	{
		assert.Equal(t, 9, check(option))
	}

	{
		option.MaxPendingLength = 1
		option.DropPolicy = DropLate
		assert.Equal(t, 1, check(option))
	}

	{
		option.MaxPendingLength = 2
		option.DropPolicy = DropLate
		assert.Equal(t, 4, check(option)) // 1 + 3
	}

	{
		option.MaxPendingLength = 2
		option.DropPolicy = DropEarly
		assert.Equal(t, 8, check(option)) // 3 + 5
	}
}

type testOrder struct {
	path      string
	id        int
	timestamp Timestamp

	wg *sync.WaitGroup
}

type testOrderHandler struct {
	res []int
}

func (h *testOrderHandler) Path(event *testOrder) string {
	return event.path
}
func (h *testOrderHandler) Handle(dest struct{}, events ...*testOrder) (await bool) {
	for _, event := range events {
		h.res = append(h.res, event.id)
		event.wg.Done()
	}
	return false
}

func (h *testOrderHandler) GetArea(path string) int                 { return 0 }
func (h *testOrderHandler) GetSize(event *testOrder) int            { return 0 }
func (h *testOrderHandler) GetTimestamp(event *testOrder) Timestamp { return event.timestamp }
func (h *testOrderHandler) GetType(event *testOrder) EventType      { return 0 }
func (h *testOrderHandler) OnDrop(event *testOrder)                 {}

func TestDynamicStreamOrder(t *testing.T) {
	handler := &testOrderHandler{}
	option := NewOption()
	option.SchedulerInterval = 1 * time.Hour
	option.ReportInterval = 1 * time.Hour
	option.StreamCount = 1

	hwg := &sync.WaitGroup{}
	option.handleWait = hwg
	hwg.Add(1)

	ds := NewDynamicStream(handler, option)
	ds.Start()

	ds.AddPath("p1", struct{}{})
	ds.AddPath("p2", struct{}{})
	ds.AddPath("p3", struct{}{})

	wg := &sync.WaitGroup{}
	wg.Add(6)
	ds.In() <- &testOrder{path: "p1", id: 1, timestamp: 1, wg: wg}
	ds.In() <- &testOrder{path: "p2", id: 2, timestamp: 3, wg: wg}
	ds.In() <- &testOrder{path: "p3", id: 3, timestamp: 6, wg: wg}

	ds.In() <- &testOrder{path: "p1", id: 4, timestamp: 2, wg: wg}
	ds.In() <- &testOrder{path: "p2", id: 5, timestamp: 4, wg: wg}
	ds.In() <- &testOrder{path: "p3", id: 6, timestamp: 5, wg: wg}

	time.Sleep(10 * time.Millisecond) // Make sure all the events are in the pending queue
	hwg.Done()
	wg.Wait()

	assert.Equal(t, []int{1, 4, 2, 5, 3, 6}, handler.res)
}
