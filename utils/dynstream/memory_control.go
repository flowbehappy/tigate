package dynstream

import (
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/zap"
)

var maxMemoryUsageMetric = metrics.DynamicStreamMemoryUsage.WithLabelValues("max")
var usedMemoryUsageMetric = metrics.DynamicStreamMemoryUsage.WithLabelValues("used")

// memoryPauseRule defines a mapping rule between memory usage ratio and path pause ratio
type memoryPauseRule struct {
	// alarmThreshold represents the memory usage ratio (used/max) that triggers the control
	// e.g., 0.95 means when memory usage reaches 95% of max allowed memory
	alarmThreshold float64

	// pausePathRatio represents the proportion of paths that should be paused
	// e.g., 1.0 means pause all paths, 0.5 means pause top 50% paths with largest pending size
	pausePathRatio float64
}

// rules defines the mapping rules for memory control:
// - When memory usage >= 95%, pause all paths (100%)
// - When memory usage >= 90%, pause top 80% paths
// - When memory usage >= 85%, pause top 50% paths
// - When memory usage >= 80%, pause top 20% paths
// - When memory usage < 80%, no paths will be paused
var rules = []memoryPauseRule{
	{0.95, 1.0}, // Critical level: pause all paths
	{0.90, 0.8}, // Severe level: pause most paths
	{0.85, 0.5}, // Warning level: pause half paths
	{0.80, 0.2}, // Caution level: pause few paths
}

// findPausePathRatio finds the pause path ratio based on the memory usage ratio.
func findPausePathRatio(memoryUsageRatio float64) float64 {
	for _, rule := range rules {
		if memoryUsageRatio >= rule.alarmThreshold {
			return rule.pausePathRatio
		}
	}
	return 0 // No paths need to be paused
}

// areaMemStat is used to store the memory statistics of an area.
// It is a global level struct, not stream level.
type areaMemStat[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	area A
	// Reverse reference to the memControl this area belongs to.
	memControl *memControl[A, P, T, D, H]

	settings     atomic.Pointer[AreaSettings]
	feedbackChan chan<- Feedback[A, P, D]

	pathCount        int
	totalPendingSize atomic.Int64
}

func newAreaMemStat[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](
	area A,
	memoryControl *memControl[A, P, T, D, H],
	settings AreaSettings,
	feedbackChan chan<- Feedback[A, P, D],
) *areaMemStat[A, P, T, D, H] {
	settings.fix()
	res := &areaMemStat[A, P, T, D, H]{
		area:         area,
		memControl:   memoryControl,
		feedbackChan: feedbackChan,
	}
	res.settings.Store(&settings)
	return res
}

// This method is called by streams' handleLoop concurrently.
// Although the method is called concurrently, we don't need a mutex here. Because we only change totalPendingSize,
// which is an atomic variable. Although the settings could be updated concurrently, we don't really care about the accuracy.
func (as *areaMemStat[A, P, T, D, H]) appendEvent(
	path *pathInfo[A, P, T, D, H],
	event eventWrap[A, P, T, D, H],
	handler H,
	eventQueue *eventQueue[A, P, T, D, H],
) {
	replaced := false
	// if isPeriodicSignal(event) {
	// 	back, ok := path.pendingQueue.BackRef()
	// 	if ok && isPeriodicSignal(*back) {
	// 		// Replace the repeated signal.
	// 		// Note that since the size of the repeated signal is the same, we don't need to update the pending size.
	// 		*back = event
	// 		replaced = true
	// 		eventQueue.updateHeapAfterUpdatePath(path)
	// 	}
	// }

	if !replaced {
		if as.shouldDropEvent(path, event, handler, eventQueue) {
			// Drop the event
			handler.OnDrop(event.event)
		} else {
			// Add the event to the pending queue.
			path.pendingQueue.PushBack(event)
			// Update the pending size.
			path.pendingSize += event.eventSize
			as.totalPendingSize.Add(int64(event.eventSize))
			// Update the heaps after adding the event in the queue.
			eventQueue.updateHeapAfterUpdatePath(path)
			eventQueue.totalPendingLength.Add(1)
		}
	}

	as.updatePathPauseState(path, event)
}

func (as *areaMemStat[A, P, T, D, H]) shouldDropEvent(
	path *pathInfo[A, P, T, D, H],
	event eventWrap[A, P, T, D, H],
	handler H,
	eventQueue *eventQueue[A, P, T, D, H],
) bool {
	if event.eventSize > as.settings.Load().MaxPendingSize {
		log.Warn("The event size exceeds the max pending size",
			zap.Any("area", as.area),
			zap.Any("path", path.path),
			zap.Int("eventSize", event.eventSize),
			zap.Int("maxPendingSize", as.settings.Load().MaxPendingSize))
		return true
	}

	exceedMaxPendingSize := func() bool {
		return int(as.totalPendingSize.Load())+event.eventSize > as.settings.Load().MaxPendingSize
	}

	// If the pending size does not exceed the max allowed size, or the path has no events and the event is a periodic event,
	// don't drop the event.
	// We use the periodic event to notify the handler about the status of the path, so we send it even if the mem quota is exceeded.
	// The periodic event consumes little memory and should be processed very fast.
	if !exceedMaxPendingSize() || (path.pendingQueue.Length() == 0 && event.eventType.Property == PeriodicSignal) {
		return false
	}

	// Drop the events of the largest pending size path to find a place for the new event.
LOOP:
	for exceedMaxPendingSize() {
		longestPath, ok := path.streamAreaInfo.pathSizeHeap.PeekTop()
		if !ok {
			log.Warn("pathSizeHeap is empty, but exceed MaxPendingSize, it should not happen",
				zap.Any("area", as.area), zap.Any("path", path.path))
			return true
		}
		front, ok := longestPath.pendingQueue.FrontRef()
		if !ok {
			log.Warn("pendingQueue is empty, but exceed MaxPendingSize, it should not happen",
				zap.Any("area", as.area), zap.Any("path", path.path))
			return true
		}
		if front.timestamp <= event.timestamp {
			// By default, drop the event.
			// And only NOT drop the event when the event's timestamp is smaller than the smallest event of about-to-drop path.
			return true
		}
		// If the longest path is the same as the current path, drop the event and return.
		if longestPath.path == path.path {
			return true
		}
		for longestPath.pendingQueue.Length() != 0 {
			back, _ := longestPath.pendingQueue.PopBack()
			handler.OnDrop(back.event)
			longestPath.pendingSize -= back.eventSize
			as.totalPendingSize.Add(int64(-back.eventSize))
			eventQueue.updateHeapAfterUpdatePath((*pathInfo[A, P, T, D, H])(longestPath))
			eventQueue.totalPendingLength.Add(-1)
			if !exceedMaxPendingSize() {
				break LOOP
			}
		}
	}

	return false
}

// updatePathPauseState determines the pause state of a path and sends feedback to handler if the state is changed.
// It needs to be called after a event is appended.
// Note: Our gaol is to fast pause, and lazy resume.
func (as *areaMemStat[A, P, T, D, H]) updatePathPauseState(path *pathInfo[A, P, T, D, H], event eventWrap[A, P, T, D, H]) {
	shouldPause := as.shouldPausePath(path)
	currentTime := event.queueTime

	sendFeedback := func(pause bool) {
		select {
		case as.feedbackChan <- Feedback[A, P, D]{
			Area:  path.area,
			Path:  path.path,
			Dest:  path.dest,
			Pause: pause,
		}:
		default:
			log.Warn("Feedback channel is full, drop the feedbacks",
				zap.Any("area", path.area),
				zap.Any("path", path.path),
				zap.Bool("pause", pause))
		}
		path.lastSendFeedbackTime = currentTime
	}

	prevPaused := path.paused

	// If the path is not paused previously but should be paused, we need to pause it.
	// And send pause feedback.
	if !prevPaused && shouldPause {
		path.paused = shouldPause
		path.lastSwitchPausedTime = currentTime
		sendFeedback(true)
		return
	}

	// Otherwise, only switch pause state after the switch interval (equals to feedback interval).
	if prevPaused != shouldPause && currentTime.Sub(path.lastSwitchPausedTime) >= as.settings.Load().FeedbackInterval {
		path.paused = shouldPause
		path.lastSwitchPausedTime = currentTime
	}

	// If the path's pause state is different from the event's pause state, send feedback after the feedback interval.
	if event.paused != path.paused && currentTime.Sub(path.lastSendFeedbackTime) >= as.settings.Load().FeedbackInterval {
		sendFeedback(path.paused)
	}
}

// shouldPausePath determines if a path should be paused based on memory usage.
// 1. Find the stopMaxIndex, which is the index of the path that should be paused in the heap.
// 2. If the path is not in the heap, it should be paused only if all the paths in the heap should be paused.
// 3. If the path is in the heap, it should be paused if its index in the heap is smaller than the stopMaxIndex.
func (as *areaMemStat[A, P, T, D, H]) shouldPausePath(path *pathInfo[A, P, T, D, H]) bool {
	memoryUsageRatio := float64(as.totalPendingSize.Load()) / float64(as.settings.Load().MaxPendingSize)
	pausePathRatio := findPausePathRatio(memoryUsageRatio)

	heapLength := path.streamAreaInfo.pathSizeHeap.Len()
	stopMaxIndex := int(float64(heapLength) * pausePathRatio)
	// Although heap indices don't guarantee exact ordering,
	// they provide a good approximation for our use case.
	// Since we're using a max heap, larger elements are closer to the root (index 0).
	shouldPause := false
	if path.sizeHeapIndex == 0 {
		// The path is not in the heap.
		// It should be paused only if all the paths in the heap should be paused.
		shouldPause = pausePathRatio == 1.0
	} else {
		shouldPause = (path.sizeHeapIndex - 1) < stopMaxIndex
	}
	return shouldPause
}

// A memControl is used to control the memory usage of the dynamic stream.
// It is a global level struct, not stream level.
type memControl[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	// Since this struct is global level, different streams may access it concurrently.
	mutex sync.Mutex

	areaStatMap map[A]*areaMemStat[A, P, T, D, H]
}

func newMemControl[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]]() *memControl[A, P, T, D, H] {
	return &memControl[A, P, T, D, H]{
		areaStatMap: make(map[A]*areaMemStat[A, P, T, D, H]),
	}
}

func (m *memControl[A, P, T, D, H]) setAreaSettings(area A, settings AreaSettings) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	// Update the settings
	if as, ok := m.areaStatMap[area]; ok {
		settings.fix()
		as.settings.Store(&settings)
	}
}

func (m *memControl[A, P, T, D, H]) addPathToArea(path *pathInfo[A, P, T, D, H], settings AreaSettings, feedbackChan chan<- Feedback[A, P, D]) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	area, ok := m.areaStatMap[path.area]
	if !ok {
		area = newAreaMemStat(path.area, m, settings, feedbackChan)
		m.areaStatMap[path.area] = area
	}

	path.areaMemStat = area
	area.pathCount++

	// Update the settings
	area.settings.Store(&settings)
}

// This method is called after the path is removed.
func (m *memControl[A, P, T, D, H]) removePathFromArea(path *pathInfo[A, P, T, D, H]) {
	area := path.areaMemStat
	area.totalPendingSize.Add(int64(-path.pendingSize))

	m.mutex.Lock()
	defer m.mutex.Unlock()

	area.pathCount--
	if area.pathCount == 0 {
		delete(m.areaStatMap, area.area)
	}
}

// FIXME/TODO: We use global metric here, which is not good for multiple streams.
func (m *memControl[A, P, T, D, H]) updateMetrics() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	usedMemory := int64(0)
	maxMemory := 0
	for _, area := range m.areaStatMap {
		usedMemory += area.totalPendingSize.Load()
		maxMemory += area.settings.Load().MaxPendingSize
	}
	maxMemoryUsageMetric.Set(float64(maxMemory))
	usedMemoryUsageMetric.Set(float64(usedMemory))
}

func isPeriodicSignal[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](event eventWrap[A, P, T, D, H]) bool {
	return event.eventType.Property == PeriodicSignal
}
