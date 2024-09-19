package eventservice

import (
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/pingcap/log"
	"go.uber.org/zap"
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
	task := tasks[0]

	dataRange, needScan := task.dispatcherStat.getDataRange()
	if !needScan {
		return false
	}

	remoteID := node.ID(task.dispatcherStat.info.GetServerID())
	dispatcherID := task.dispatcherStat.info.GetID()

	ddlEvents, endTs, err := broker.schemaStore.FetchTableDDLEvents(dataRange.Span.TableID, dataRange.StartTs, dataRange.EndTs)
	if err != nil {
		log.Panic("get ddl events failed", zap.Error(err))
	}
	if endTs < dataRange.EndTs {
		dataRange.EndTs = endTs
	}
	if dataRange.EndTs <= dataRange.StartTs {
		return false
	}

	// 1. Fastpath: the dispatcher has no new events. In such case, we don't need to scan the event store.
	// We just send the watermark to the dispatcher.
	if dataRange.StartTs >= task.dispatcherStat.spanSubscription.maxEventCommitTs.Load() {
		for _, e := range ddlEvents {
			broker.sendDDL(remoteID, e, task.dispatcherStat)
		}
		broker.sendWatermark(remoteID, dispatcherID, dataRange.EndTs, task.dispatcherStat.metricEventServiceSendResolvedTsCount)
		task.dispatcherStat.watermark.Store(dataRange.EndTs)
		return false
	}

	// 2. The dispatcher has new events. We need to push the task to the task pool.
	broker.taskPool.pushTask(task)
	return true
}
