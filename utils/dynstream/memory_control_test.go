package dynstream

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/ticdc/utils/deque"
	"github.com/pingcap/ticdc/utils/heap"
	"github.com/stretchr/testify/require"
)

// Helper function to create test components
func setupTestComponents() (*memControl[int, string, *mockEvent, any, *mockHandler], *pathInfo[int, string, *mockEvent, any, *mockHandler]) {
	mc := newMemControl[int, string, *mockEvent, any, *mockHandler]()

	area := 1

	sai := &streamAreaInfo[int, string, *mockEvent, any, *mockHandler]{
		area:          area,
		timestampHeap: heap.NewHeap[*timestampPathNode[int, string, *mockEvent, any, *mockHandler]](),
		queueTimeHeap: heap.NewHeap[*queuePathNode[int, string, *mockEvent, any, *mockHandler]](),
		pathSizeHeap:  heap.NewHeap[*pathSizeStat[int, string, *mockEvent, any, *mockHandler]](),
	}

	path := &pathInfo[int, string, *mockEvent, any, *mockHandler]{
		area:           area,
		path:           "test-path",
		dest:           "test-dest",
		streamAreaInfo: sai,
		pendingQueue:   deque.NewDeque[eventWrap[int, string, *mockEvent, any, *mockHandler]](32, 0),
	}

	return mc, path
}

func TestMemControlAddRemovePath(t *testing.T) {
	mc, path := setupTestComponents()
	settings := AreaSettings{
		MaxPendingSize:   1000,
		FeedbackInterval: time.Second,
	}
	feedbackChan := make(chan Feedback[int, string, any], 10)

	// Test adding path
	mc.addPathToArea(path, settings, feedbackChan)
	require.NotNil(t, path.areaMemStat)
	require.Equal(t, 1, path.areaMemStat.pathCount)

	// Test removing path
	mc.removePathFromArea(path)
	require.Equal(t, 0, path.areaMemStat.pathCount)
	require.Empty(t, mc.areaStatMap)
}

func TestAreaMemStatAppendEvent(t *testing.T) {
	// TODO: fix this test
	t.Skip("Skipping TestAreaMemStatAppendEvent because we don't merge periodic signals when append any more")

	mc, path1 := setupTestComponents()
	settings := AreaSettings{
		MaxPendingSize:   15,
		FeedbackInterval: time.Millisecond * 10,
	}
	feedbackChan := make(chan Feedback[int, string, any], 10)
	mc.addPathToArea(path1, settings, feedbackChan)

	handler := &mockHandler{}
	option := NewOption()
	option.EnableMemoryControl = true
	eventQueue := newEventQueue(option, handler)
	eventQueue.addPath(path1)

	// 1. Append normal event, it should be accepted
	normalEvent1 := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event:     &mockEvent{id: 1, path: "test-path"},
		timestamp: 1,
		eventSize: 10,
		queueTime: time.Now(),
	}
	path1.areaMemStat.appendEvent(path1, normalEvent1, handler, &eventQueue)
	require.Equal(t, int64(10), path1.areaMemStat.totalPendingSize.Load())

	// Append 2 periodic signals, and the second one will replace the first one
	periodicEvent := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event:     &mockEvent{id: 2, path: "test-path"},
		eventSize: 5,
		timestamp: 2,
		queueTime: time.Now(),
		eventType: EventType{Property: PeriodicSignal},
	}
	path1.areaMemStat.appendEvent(path1, periodicEvent, handler, &eventQueue)
	require.Equal(t, int64(15), path1.areaMemStat.totalPendingSize.Load())
	require.Equal(t, 2, path1.pendingQueue.Length())
	back, _ := path1.pendingQueue.BackRef()
	require.Equal(t, periodicEvent.timestamp, back.timestamp)
	periodicEvent2 := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event:     &mockEvent{id: 3, path: "test-path"},
		timestamp: 3,
		eventSize: 5,
		queueTime: time.Now(),
		eventType: EventType{Property: PeriodicSignal},
	}
	path1.areaMemStat.appendEvent(path1, periodicEvent2, handler, &eventQueue)
	// Size should remain the same as the signal was replaced
	require.Equal(t, int64(15), path1.areaMemStat.totalPendingSize.Load())
	// The pending queue should only have 2 events
	require.Equal(t, 2, path1.pendingQueue.Length())
	// The last event timestamp should be the latest
	back, _ = path1.pendingQueue.BackRef()
	require.Equal(t, periodicEvent2.timestamp, back.timestamp)

	// 3. Add a normal event, and it should be dropped, because the total pending size is exceed the max pending size
	normalEvent2 := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event:     &mockEvent{id: 4, path: "test-path"},
		eventSize: 20,
		queueTime: time.Now(),
		timestamp: 4,
	}
	path1.areaMemStat.appendEvent(path1, normalEvent2, handler, &eventQueue)
	require.Equal(t, int64(15), path1.areaMemStat.totalPendingSize.Load())
	require.Equal(t, 2, path1.pendingQueue.Length())
	back, _ = path1.pendingQueue.BackRef()
	// The last event should be the periodic event
	require.Equal(t, periodicEvent2.timestamp, back.timestamp)
	events := handler.drainDroppedEvents()
	require.Equal(t, 1, len(events))
	require.Equal(t, normalEvent2.event, events[0])

	// 4. Change the settings, enlarge the max pending size
	newSettings := AreaSettings{
		MaxPendingSize:   100,
		FeedbackInterval: time.Millisecond * 10,
	}
	mc.setAreaSettings(path1.area, newSettings)
	require.Equal(t, 100, path1.areaMemStat.settings.Load().MaxPendingSize)
	require.Equal(t, newSettings, *path1.areaMemStat.settings.Load())
	addr1 := fmt.Sprintf("%p", path1.areaMemStat.settings.Load())
	addr2 := fmt.Sprintf("%p", &newSettings)
	require.NotEqual(t, addr1, addr2)
	// 5. Add a normal event, and it should be accepted,
	//  because the total pending size is less than the max pending size
	normalEvent3 := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event:     &mockEvent{id: 5, path: "test-path"},
		eventSize: 20,
		queueTime: time.Now(),
		timestamp: 5,
	}
	path1.areaMemStat.appendEvent(path1, normalEvent3, handler, &eventQueue)
	require.Equal(t, int64(35), path1.areaMemStat.totalPendingSize.Load())
	require.Equal(t, 3, path1.pendingQueue.Length())
	back, _ = path1.pendingQueue.BackRef()
	require.Equal(t, normalEvent3.timestamp, back.timestamp)

	// 6. Add a new path, and append a large event to it, it will consume the remaining memory
	path2 := &pathInfo[int, string, *mockEvent, any, *mockHandler]{
		area:           1,
		path:           "test-path-2",
		streamAreaInfo: path1.streamAreaInfo,
		pendingQueue:   deque.NewDeque[eventWrap[int, string, *mockEvent, any, *mockHandler]](32, 0),
	}
	mc.addPathToArea(path2, newSettings, feedbackChan)
	eventQueue.addPath(path2)
	largeEvent := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event:     &mockEvent{id: 6, path: "test-path-2"},
		timestamp: 6,
		eventSize: int(newSettings.MaxPendingSize - int(path1.areaMemStat.totalPendingSize.Load())),
		queueTime: time.Now(),
	}
	path2.areaMemStat.appendEvent(path2, largeEvent, handler, &eventQueue)
	require.Equal(t, newSettings.MaxPendingSize, int(path2.areaMemStat.totalPendingSize.Load()))
	require.Equal(t, 2, path2.areaMemStat.pathCount)
	// There are 4 events in the eventQueue, [normalEvent1, periodicEvent2, normalEvent3, largeEvent]
	require.Equal(t, int64(4), eventQueue.totalPendingLength.Load())
	// The new path should be paused, because the pending size is reach the max pending size
	require.True(t, path2.paused)

	time.Sleep(2 * newSettings.FeedbackInterval)
	// 7. Add a normal event to path1, and the large event of path2 should be dropped
	// Because we will find the path with the largest pending size, and it's path2
	// So the large event of path2 will be dropped
	normalEvent4 := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event: &mockEvent{id: 7, path: "test-path"},
		// Make it the smallest timestamp,
		//so it will cause the large event of path2 to be dropped
		timestamp: 0,
		eventSize: 10,
		queueTime: time.Now(),
	}
	path1.areaMemStat.appendEvent(path1, normalEvent4, handler, &eventQueue)
	require.Equal(t, 45, int(path1.areaMemStat.totalPendingSize.Load()))
	require.Equal(t, 0, path2.pendingQueue.Length())
	droppedEvents := handler.drainDroppedEvents()
	require.Equal(t, 1, len(droppedEvents))
	require.Equal(t, largeEvent.event, droppedEvents[0])
	require.Equal(t, int64(4), eventQueue.totalPendingLength.Load())

	// 8. Add a signal event to path2, and it should be accepted, and its state should be resumed
	periodicEvent3 := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event:     &mockEvent{id: 8, path: "test-path-2"},
		timestamp: 7,
		eventSize: 5,
		queueTime: time.Now(),
	}
	path2.areaMemStat.appendEvent(path2, periodicEvent3, handler, &eventQueue)
	require.Equal(t, 1, path2.pendingQueue.Length())
	require.Equal(t, int64(5), eventQueue.totalPendingLength.Load())
	require.False(t, path2.paused)
}

func TestShouldPausePath(t *testing.T) {
	mc, path := setupTestComponents()

	// Add 100 path to the heap
	maxPendingSize := 0
	for i := 0; i < 100; i++ {
		newPath := &pathInfo[int, string, *mockEvent, any, *mockHandler]{
			path:        fmt.Sprintf("test-path-%d", i),
			pendingSize: i,
		}
		path.streamAreaInfo.pathSizeHeap.AddOrUpdate((*pathSizeStat[int, string, *mockEvent, any, *mockHandler])(newPath))
		maxPendingSize += i
	}

	settings := AreaSettings{
		MaxPendingSize:   maxPendingSize,
		FeedbackInterval: time.Second,
	}
	feedbackChan := make(chan Feedback[int, string, any], 10)
	mc.addPathToArea(path, settings, feedbackChan)

	tests := []struct {
		name          string
		pendingSize   int64
		heapIndex     int
		expectedPause bool
	}{
		{"No pause needed", int64(maxPendingSize / 2), 2, false},
		{"Need pause", int64(maxPendingSize * 4 / 5), 20, true},
		{"Critical level", int64(maxPendingSize), 2, true},
		{"Not in heap", int64(maxPendingSize / 2), 0, false},
		{"Not in heap but critical", int64(maxPendingSize), 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path.areaMemStat.totalPendingSize.Store(tt.pendingSize)
			path.sizeHeapIndex = tt.heapIndex
			result := path.areaMemStat.shouldPausePath(path)
			require.Equal(t, tt.expectedPause, result)
		})
	}
}

func TestSetAreaSettings(t *testing.T) {
	mc, path := setupTestComponents()
	// Case 1: Set the initial settings.
	initialSettings := AreaSettings{
		MaxPendingSize:   1000,
		FeedbackInterval: time.Second,
	}
	feedbackChan := make(chan Feedback[int, string, any], 10)
	mc.addPathToArea(path, initialSettings, feedbackChan)
	require.Equal(t, initialSettings, *path.areaMemStat.settings.Load())

	// Case 2: Set the new settings.
	newSettings := AreaSettings{
		MaxPendingSize:   2000,
		FeedbackInterval: 2 * time.Second,
	}
	mc.setAreaSettings(path.area, newSettings)
	require.Equal(t, newSettings, *path.areaMemStat.settings.Load())

	// Case 3: Set a invalid settings.
	invalidSettings := AreaSettings{
		MaxPendingSize:   0,
		FeedbackInterval: 0,
	}
	mc.setAreaSettings(path.area, invalidSettings)
	require.NotEqual(t, invalidSettings, *path.areaMemStat.settings.Load())
	require.Equal(t, DefaultFeedbackInterval, path.areaMemStat.settings.Load().FeedbackInterval)
	require.Equal(t, DefaultMaxPendingSize, path.areaMemStat.settings.Load().MaxPendingSize)
}

func TestIsPeriodicSignal(t *testing.T) {
	batchableEvent := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		eventType: EventType{Property: BatchableData},
	}
	periodicEvent := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		eventType: EventType{Property: PeriodicSignal},
	}
	nonBatchableEvent := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		eventType: EventType{Property: NonBatchable},
	}
	require.False(t, isPeriodicSignal(batchableEvent))
	require.True(t, isPeriodicSignal(periodicEvent))
	require.False(t, isPeriodicSignal(nonBatchableEvent))
}

func TestFindPausePathRatio(t *testing.T) {
	tests := []struct {
		name               string
		memoryUsageRatio   float64
		expectedPauseRatio float64
	}{
		{"Critical level", 0.96, 1.0},
		{"Severe level", 0.92, 0.8},
		{"Warning level", 0.87, 0.5},
		{"Caution level", 0.82, 0.2},
		{"Normal level", 0.75, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ratio := findPausePathRatio(tt.memoryUsageRatio)
			require.Equal(t, tt.expectedPauseRatio, ratio)
		})
	}
}
