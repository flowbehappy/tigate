package dynstream

import (
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/flowbehappy/tigate/utils/heap"
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

type pathSizeStat[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] pathInfo[A, P, T, D, H]

func (p *pathSizeStat[A, P, T, D, H]) SetHeapIndex(index int) {
	(*pathInfo[A, P, T, D, H])(p).sizeHeapIndex = index
}

func (p *pathSizeStat[A, P, T, D, H]) GetHeapIndex() int {
	return (*pathInfo[A, P, T, D, H])(p).sizeHeapIndex
}

func (p *pathSizeStat[A, P, T, D, H]) LessThan(other *pathSizeStat[A, P, T, D, H]) bool {
	// The heap is in descending order.
	return p.pendingSize > other.pendingSize
}

// AreaStat is used to store the statistics of an area.
// It is a global level struct, not stream level.
type globalAreaStat[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	area      A
	pathCount int

	mutex sync.Mutex

	pathSizeHeap     heap.Heap[*pathSizeStat[A, P, T, D, H]]
	totalPendingSize atomic.Int64
	pausePathRatio   float64

	settings AreaSettings
}

func (as *globalAreaStat[A, P, T, D, H]) addPathToArea(path *pathInfo[A, P, T, D, H]) {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	path.globalAreaStat = as
	as.pathCount++
	as.pathSizeHeap.AddOrUpdate((*pathSizeStat[A, P, T, D, H])(path))
}

// Return true if the area is empty.
func (as *globalAreaStat[A, P, T, D, H]) removePathFromArea(path *pathInfo[A, P, T, D, H]) bool {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	as.pathSizeHeap.Remove((*pathSizeStat[A, P, T, D, H])(path))
	as.pathCount--
	path.globalAreaStat = nil
	return as.pathCount == 0
}

func (as *globalAreaStat[A, P, T, D, H]) appendEvent(area A, path *pathInfo[A, P, T, D, H], event eventWrap[A, P, T, D, H], handler H) {
	if event.eventType != RepeatedSignal &&
		as.settings.MaxPendingSize > 0 &&
		int(as.totalPendingSize.Load())+event.size > as.settings.MaxPendingSize {
		// Drop the event after the pending size exceeds the limit.
		// Note that we don't drop the repeated signal. Because the repeated signal is small
		// and it is important for the handler to push the other events forward.
		handler.OnDrop(event.event)
		return
	}

	replaced := false
	if event.eventType == RepeatedSignal {
		front, ok := path.pendingQueue.FrontRef()
		if ok && front.eventType == RepeatedSignal {
			// Replace the repeated signal.
			// Note that since the size of the repeated signal is the same, we don't need to update the pending size.
			*front = event
			replaced = true
		}
	}

	oldPaused := path.paused
	pathShouldPause := false
	{
		m.mutex.Lock()
		defer m.mutex.Unlock()

		if !replaced {
			path.pendingQueue.PushBack(event)
			path.pendingSize += event.size
			curSize := int(as.totalPendingSize.Add(int64(event.size)))

			as.pathSizeHeap.AddOrUpdate((*pathSizeStat[A, P, T, D, H])(path))
			as.pausePathRatio = findPausePathRatio(float64(curSize) / float64(as.settings.MaxPendingSize))
		}

		stopMaxIndex := int(float64(as.pathSizeHeap.Len()) * as.pausePathRatio)
		// In a heap, although the index of an element in the underlying array is not strictly indicating the order,
		// it is still roughly correct. We don't need to be very precise here.
		// The heap is a max heap, so the elements with larger size are in the front.
		pathShouldPause = path.sizeHeapIndex < stopMaxIndex
	}

	path.paused = pathShouldPause

	if as.settings.FeedbackInterval != 0 {
		if (!oldPaused && path.paused) ||
			(event.paused != path.paused && event.queueTime.Sub(path.lastSendFeedbackTime) >= as.settings.FeedbackInterval) {
			// If is just paused, send feedback immediately.
			// Otherwise, send feedback after the feedback interval.
			select {
			case m.feedbackCh <- Feedback[A, P, D]{
				Area:  path.area,
				Path:  path.path,
				Dest:  path.dest,
				Pause: path.paused,
			}:
			default:
				log.Warn("Feedback channel is full, drop the feedbacks",
					zap.Any("area", path.area),
					zap.Any("path", path.path),
					zap.Bool("pause", path.paused))
			}
			path.lastSendFeedbackTime = event.queueTime
		}
	}
}

// A memoryControl is used to control the memory usage of the dynamic stream.
// It is a global level struct, not stream level.
type memoryControl[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	// Since this struct is global level, different streams may access it concurrently.
	// The operations on the areaStatMap should be protected by the mutex.
	mutex sync.Mutex

	areaStatMap map[A]*globalAreaStat[A, P, T, D, H]
	feedbackCh  chan<- Feedback[A, P, D]
}

func newMemoryControl[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](feedbackCh chan<- Feedback[A, P, D]) *memoryControl[A, P, T, D, H] {
	return &memoryControl[A, P, T, D, H]{
		areaStatMap: make(map[A]*globalAreaStat[A, P, T, D, H]),
		feedbackCh:  feedbackCh,
	}
}

func (m *memoryControl[A, P, T, D, H]) getAndCreateAreaStat(area A) *globalAreaStat[A, P, T, D, H] {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	as, ok := m.areaStatMap[area]
	if !ok {
		as = &globalAreaStat[A, P, T, D, H]{
			area:         area,
			pathSizeHeap: heap.NewHeap[*pathSizeStat[A, P, T, D, H]](),
		}
		m.areaStatMap[area] = as
	}
	return as
}

func (m *memoryControl[A, P, T, D, H]) setAreaSettings(area A, settings AreaSettings) {
	as := m.getAndCreateAreaStat(area)
	as.settings = settings
}

func (m *memoryControl[A, P, T, D, H]) getAreaSettings(area A) AreaSettings {
	as := m.getAndCreateAreaStat(area)
	return as.settings
}

func (m *memoryControl[A, P, T, D, H]) ensurePathInArea(path *pathInfo[A, P, T, D, H]) {
	if path.globalAreaStat == nil {
		if path.pendingQueue.Length() > 0 {
			panic("pending events before setting area stat")
		}
		as := m.getAndCreateAreaStat(path.area)
		as.addPathToArea(path)
	}
}

// This mehotd is called after the path is removed.
func (m *memoryControl[A, P, T, D, H]) removePathFromArea(path *pathInfo[A, P, T, D, H]) {
	if path.globalAreaStat == nil {
		return
	}
	as := path.globalAreaStat
	if as.removePathFromArea(path) {
		m.mutex.Lock()
		defer m.mutex.Unlock()
		delete(m.areaStatMap, as.area)
	}
}

func (m *memoryControl[A, P, T, D, H]) appendEvent(area A, path *pathInfo[A, P, T, D, H], event eventWrap[A, P, T, D, H], handler H) {
	m.ensurePathInArea(path)
	path.globalAreaStat.appendEvent(area, path, event, handler)
}
