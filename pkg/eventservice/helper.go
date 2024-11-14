package eventservice

import (
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/utils/dynstream"
)

type dispatcherEventsHandler struct {
}

func (h *dispatcherEventsHandler) Path(task scanTask) common.DispatcherID {
	return task.dispatcherStat.info.GetID()
}

// Handle implements the dynstream.Handler interface.
// If the event is processed successfully, it should return false.
// If the event is processed asynchronously, it should return true. The later events of the path are blocked
// until a wake signal is sent to DynamicStream's Wake channel.
func (h *dispatcherEventsHandler) Handle(broker *eventBroker, tasks ...scanTask) bool {
	if len(tasks) != 1 {
		log.Panic("only one task is allowed")
	}
	startTime := time.Now()
	defer func() {
		metricEventBrokerHandleDuration.Observe(float64(time.Since(startTime).Milliseconds()))
	}()
	task := tasks[0]
	needScan, _ := broker.checkNeedScan(task)
	if !needScan {
		task.handle()
		return false
	}
	// The dispatcher has new events. We need to push the task to the task pool.
	return broker.taskPool.pushTask(task)
}

func (h *dispatcherEventsHandler) GetType(event scanTask) dynstream.EventType {
	// scanTask is only a signal to trigger the scan.
	// We make it a PeriodicSignal to make the new scan task squeeze out the old one.
	return dynstream.EventType{DataGroup: 0, Property: dynstream.PeriodicSignal}
}

func (h *dispatcherEventsHandler) GetSize(event scanTask) int { return 0 }
func (h *dispatcherEventsHandler) GetArea(path common.DispatcherID, dest *eventBroker) common.GID {
	d, ok := dest.getDispatcher(path)
	if !ok {
		return common.GID{}
	}
	return d.info.GetChangefeedID().ID()
}
func (h *dispatcherEventsHandler) GetTimestamp(event scanTask) dynstream.Timestamp { return 0 }
func (h *dispatcherEventsHandler) IsPaused(event scanTask) bool                    { return false }
func (h *dispatcherEventsHandler) OnDrop(event scanTask)                           {}

// mergeChannel is a channel that merges dispatcher stats by dispatcher ID.
// It ensures that only one stat for each dispatcher is sent to the channel.
type mergeChannel struct {
	ch    chan *dispatcherStat
	cache struct {
		mu sync.RWMutex
		m  map[common.DispatcherID]int
	}
}

func (mc *mergeChannel) isInChannel(key common.DispatcherID) bool {
	mc.cache.mu.RLock()
	defer mc.cache.mu.RUnlock()
	v, ok := mc.cache.m[key]
	return ok && v > 0
}

// TrySend attempts to send a dispatcher stat to the channel.
// Returns false if the element already exists, true if successfully sent.
func (mc *mergeChannel) TrySend(stat *dispatcherStat) bool {
	// Use dispatcher ID as key for better performance
	key := stat.info.GetID()
	// Check if the element already exists in the map
	if mc.isInChannel(key) {
		// Element already exists, skip sending
		return false
	}
	// Store the element in map first, using ID as key and stat as value
	// Then send to channel
	mc.cache.mu.Lock()
	mc.cache.m[key] = 1
	mc.cache.mu.Unlock()
	select {
	case mc.ch <- stat:
		return true
	default:
		mc.cache.mu.Lock()
		mc.cache.m[key] = 0
		mc.cache.mu.Unlock()
		return false
	}
}

// Receive gets an element from the channel and removes it from the map
func (mc *mergeChannel) Receive() *dispatcherStat {
	stat := <-mc.ch
	mc.cache.mu.Lock()
	mc.cache.m[stat.info.GetID()] = 0
	mc.cache.mu.Unlock()
	return stat
}

// NewMergeChannel creates a new mergeChannel with the specified capacity
func NewMergeChannel(capacity int) *mergeChannel {
	res := &mergeChannel{
		ch: make(chan *dispatcherStat, capacity),
	}
	res.cache.m = make(map[common.DispatcherID]int, capacity)
	return res
}
