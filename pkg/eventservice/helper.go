package eventservice

import (
	"context"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
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
	ctx := context.Background()
	needScan, _, _ := broker.checkNeedScan(ctx, task)
	if !needScan {
		return false
	}
	// The dispatcher has new events. We need to push the task to the task pool.
	return broker.taskPool.pushTask(task)
}
