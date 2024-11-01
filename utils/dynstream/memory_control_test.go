package dynstream

import (
	"fmt"
	"testing"
	"time"

	"github.com/flowbehappy/tigate/utils/deque"
	"github.com/flowbehappy/tigate/utils/heap"
	"github.com/stretchr/testify/require"
)

// Helper function to create test components
func setupTestComponents() (*memControl[int, string, *mockEvent, any, *mockHandler], *pathInfo[int, string, *mockEvent, any, *mockHandler]) {
	mc := newMemControl[int, string, *mockEvent, any, *mockHandler]()

	sai := &streamAreaInfo[int, string, *mockEvent, any, *mockHandler]{
		timestampHeap: heap.NewHeap[*timestampPathNode[int, string, *mockEvent, any, *mockHandler]](),
		queueTimeHeap: heap.NewHeap[*queuePathNode[int, string, *mockEvent, any, *mockHandler]](),
		pathSizeHeap:  heap.NewHeap[*pathSizeStat[int, string, *mockEvent, any, *mockHandler]](),
	}

	path := &pathInfo[int, string, *mockEvent, any, *mockHandler]{
		area:           1,
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
	mc, path := setupTestComponents()
	settings := AreaSettings{
		MaxPendingSize:   15,
		FeedbackInterval: time.Second,
	}
	feedbackChan := make(chan Feedback[int, string, any], 10)
	mc.addPathToArea(path, settings, feedbackChan)

	handler := &mockHandler{}
	option := NewOption()
	option.EnableMemoryControl = true
	eventQueue := newEventQueue(option, handler)

	// Append normal event
	normalEvent := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event:     &mockEvent{id: 1, path: "test-path"},
		timestamp: 1,
		eventSize: 10,
		queueTime: time.Now(),
	}
	path.areaMemStat.appendEvent(path, normalEvent, handler, &eventQueue)
	require.Equal(t, int64(10), path.areaMemStat.totalPendingSize.Load())

	// Append 2 periodic signals, and the second one will replace the first one
	periodicEvent := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event:     &mockEvent{id: 1, path: "test-path"},
		eventSize: 5,
		timestamp: 2,
		queueTime: time.Now(),
		eventType: EventType{Property: PeriodicSignal},
	}
	path.areaMemStat.appendEvent(path, periodicEvent, handler, &eventQueue)
	require.Equal(t, int64(15), path.areaMemStat.totalPendingSize.Load())
	require.Equal(t, 2, path.pendingQueue.Length())
	back, _ := path.pendingQueue.BackRef()
	require.Equal(t, periodicEvent.timestamp, back.timestamp)

	periodicEvent2 := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event:     &mockEvent{id: 2, path: "test-path"},
		timestamp: 3,
		eventSize: 5,
		queueTime: time.Now(),
		eventType: EventType{Property: PeriodicSignal},
	}
	path.areaMemStat.appendEvent(path, periodicEvent2, handler, &eventQueue)
	// Size should remain the same as the signal was replaced
	require.Equal(t, int64(15), path.areaMemStat.totalPendingSize.Load())
	// The pending queue should only have 2 events
	require.Equal(t, 2, path.pendingQueue.Length())
	// The last event timestamp should be the latest
	back, _ = path.pendingQueue.BackRef()
	require.Equal(t, periodicEvent2.timestamp, back.timestamp)

	// Add a normal event, and it should be dropped
	normalEvent2 := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event:     &mockEvent{id: 3, path: "test-path"},
		eventSize: 20,
		queueTime: time.Now(),
		timestamp: 4,
	}
	path.areaMemStat.appendEvent(path, normalEvent2, handler, &eventQueue)
	require.Equal(t, int64(15), path.areaMemStat.totalPendingSize.Load())
	require.Equal(t, 2, path.pendingQueue.Length())
	back, _ = path.pendingQueue.BackRef()
	// The last event should be the periodic event
	require.Equal(t, periodicEvent2.timestamp, back.timestamp)
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
	require.Equal(t, initialSettings, path.areaMemStat.settings)

	// Case 2: Set the new settings.
	newSettings := AreaSettings{
		MaxPendingSize:   2000,
		FeedbackInterval: 2 * time.Second,
	}
	mc.setAreaSettings(path.area, newSettings)
	require.Equal(t, newSettings, path.areaMemStat.settings)

	// Case 3: Set a invalid settings.
	invalidSettings := AreaSettings{
		MaxPendingSize:   0,
		FeedbackInterval: 0,
	}
	mc.setAreaSettings(path.area, invalidSettings)
	require.NotEqual(t, invalidSettings, path.areaMemStat.settings)
	require.Equal(t, DefaultFeedbackInterval, path.areaMemStat.settings.FeedbackInterval)
	require.Equal(t, DefaultMaxPendingSize, path.areaMemStat.settings.MaxPendingSize)
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
