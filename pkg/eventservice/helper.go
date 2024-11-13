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

type mergeChannel struct {
	ch chan *dispatcherStat
	m  sync.Map
}

// TrySend attempts to send a dispatcher stat to the channel.
// Returns false if the element already exists, true if successfully sent.
func (mc *mergeChannel) TrySend(stat *dispatcherStat) bool {
	// Use dispatcher ID as key for better performance
	key := stat.info.GetID()
	// Check if the element already exists in the map
	_, exists := mc.m.Load(key)
	if exists {
		// Element already exists, skip sending
		return false
	}
	// Store the element in map first, using ID as key and stat as value
	mc.m.Store(key, stat)
	// Then send to channel
	mc.ch <- stat
	return true
}

// Receive gets an element from the channel and removes it from the map
func (mc *mergeChannel) Receive() chan *dispatcherStat {
	return mc.ch
}

func (mc *mergeChannel) Remove(stat *dispatcherStat) {
	mc.m.Delete(stat.info.GetID())
}

// NewMergeChannel creates a new mergeChannel with the specified capacity
func NewMergeChannel(capacity int) *mergeChannel {
	return &mergeChannel{
		ch: make(chan *dispatcherStat, capacity),
	}
}
