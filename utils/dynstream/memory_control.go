package dynstream

import (
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// If the memory usage of an area exceeds the threshold, the top N paths with the largest pending size will be paused.
var memoryAlarmThresholds = []float64{0.95, 0.9, 0.85, 0.8}
var pausePathRatios = []float64{1.0, 0.8, 0.5, 0.2}

func findPausePathRatio(ratio float64) float64 {
	for i, t := range memoryAlarmThresholds {
		if ratio >= t {
			return pausePathRatios[i]
		}
	}
	return 0
}

// areaMemStat is used to store the memory statistics of an area.
// It is a global level struct, not stream level.
type areaMemStat[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	area       A
	memControl *memControl[A, P, T, D, H]

	settings     AreaSettings
	feedbackChan chan<- Feedback[A, P, D]

	pathCount        int
	totalPendingSize atomic.Int64
}

// This method is called by streams' handleLoop concurrently.
// Although the method is called concurrently, we don't need a mutex here. Because we only change totalPendingSize,
// which is an atomic variable. Although the settings could be updated concurrently, we don't really care about the accuracy.
func (as *areaMemStat[A, P, T, D, H]) appendEvent(path *pathInfo[A, P, T, D, H], event eventWrap[A, P, T, D, H], handler H, eventQueue *eventQueue[A, P, T, D, H]) {
	replaced := false
	if event.eventType.Property == PeriodicSignal {
		front, ok := path.pendingQueue.FrontRef()
		if ok && front.eventType.Property == PeriodicSignal {
			// Replace the repeated signal.
			// Note that since the size of the repeated signal is the same, we don't need to update the pending size.
			*front = event
			replaced = true
			eventQueue.updateHeapAfterUpdatePath(path)
		}
	}

	dropped := false
	if !replaced && int(as.totalPendingSize.Load())+event.eventSize > as.settings.MaxPendingSize {
		// Drop the event if the total pending size exceeds the limit, and
		// 1. The path has pending events, or
		// 2. The event's timestamp is not the smallest among all the paths in the area.
		// The second point is important. Because other paths maybe just waiting for this event to process before moving forward.
		if path.pendingQueue.Length() != 0 {
			dropped = true
			// Drop the event after the pending size exceeds the limit.
			if event.eventType.Property != PeriodicSignal {
				handler.OnDrop(event.event)
			}
		} else if top, ok := path.streamAreaInfo.timestampHeap.PeekTop(); ok && event.timestamp > top.frontTimestamp {
			dropped = true
			// Drop the event after the pending size exceeds the limit.
			if event.eventType.Property != PeriodicSignal {
				handler.OnDrop(event.event)
			}
		}
	}

	if !replaced && !dropped {
		// Add the event to the pending queue.
		path.pendingQueue.PushBack(event)

		// Update the pending size.
		path.pendingSize += event.eventSize
		as.totalPendingSize.Add(int64(event.eventSize))

		// Update the heaps after adding the event in the queue.
		// Including the pathSizeHeap
		eventQueue.updateHeapAfterUpdatePath(path)
		eventQueue.totalPendingLength++
	}

	pendingSize := int(as.totalPendingSize.Load())
	pausePathRatio := findPausePathRatio(float64(pendingSize) / float64(as.settings.MaxPendingSize))

	heapLength := path.streamAreaInfo.pathSizeHeap.Len()
	stopMaxIndex := int(float64(heapLength) * pausePathRatio)
	// In a heap, although the index of an element in the underlying array is not strictly indicating the order,
	// it is still roughly correct. We don't need to be very precise here.
	// The heap is a max heap, so the elements with larger size are in the front.
	shoudlPausePath := false
	if path.sizeHeapIndex == 0 {
		// The path is not in the heap.
		// It should be paused only if all the paths in the heap should be paused.
		shoudlPausePath = pausePathRatio == 1.0
	} else {
		shoudlPausePath = (path.sizeHeapIndex - 1) < stopMaxIndex
	}

	prevPaused := path.paused
	path.paused = shoudlPausePath

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
		path.lastSendFeedbackTime = event.queueTime
	}

	if as.settings.FeedbackInterval > 0 {
		if !prevPaused && shoudlPausePath {
			// Pause should be done immediately. And send pause feedback immediately no matter what the upstream is paused or not.
			path.paused = true
			path.lastSwitchPausedTime = event.queueTime
			sendFeedback(true)
		} else {
			if prevPaused != shoudlPausePath && event.queueTime.Sub(path.lastSwitchPausedTime) >= as.settings.FeedbackInterval {
				// Only switch pause after the switch interval (equals to feedback interval).
				path.paused = shoudlPausePath
				path.lastSwitchPausedTime = event.queueTime
			}
			if event.paused != path.paused && event.queueTime.Sub(path.lastSendFeedbackTime) >= as.settings.FeedbackInterval {
				// Only send feedback after the feedback interval.
				sendFeedback(path.paused)
			}
		}
	}
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
		as.settings = settings
	}
}

func (m *memControl[A, P, T, D, H]) addPathToArea(path *pathInfo[A, P, T, D, H], settings AreaSettings, feedbackChan chan<- Feedback[A, P, D]) {
	if settings.MaxPendingSize == 0 {
		log.Panic("AreaSettings.MaxPendingSize should not be 0")
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()

	area, ok := m.areaStatMap[path.area]
	if !ok {
		area = &areaMemStat[A, P, T, D, H]{
			area:         path.area,
			memControl:   m,
			settings:     settings,
			feedbackChan: feedbackChan,
		}
		m.areaStatMap[path.area] = area
	}

	path.areaMemStat = area
	area.pathCount++

	// Update the settings
	area.settings = settings
}

// This mehotd is called after the path is removed.
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
