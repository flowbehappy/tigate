// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package dispatchermanager

import (
	"time"

	"github.com/flowbehappy/tigate/downstreamadapter/dispatcher"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/google/uuid"
	"github.com/ngaut/log"
	"go.uber.org/zap"
)

/*
HeartbeatSendTask is responsible for collecting heartbeat info periodically from
all dispatchers in the event dispatcher manager and sending them to the HeartbeatRequestQueue.
Each event dispatcher manager corresponds a HeartbeatSendTask.
*/
type HeartbeatSendTask struct {
	ticker                 *time.Ticker
	eventDispatcherManager *EventDispatcherManager
	taskStatus             threadpool.TaskStatus
}

func newHeartBeatSendTask(m *EventDispatcherManager) *HeartbeatSendTask {
	return &HeartbeatSendTask{
		ticker:                 time.NewTicker(50 * time.Millisecond),
		eventDispatcherManager: m,
		taskStatus:             threadpool.Running,
	}
}

func (t *HeartbeatSendTask) GetStatus() threadpool.TaskStatus {
	return t.taskStatus
}

func (t *HeartbeatSendTask) SetStatus(taskStatus threadpool.TaskStatus) {
	t.taskStatus = taskStatus
}

func (t *HeartbeatSendTask) Execute(timeout time.Duration) threadpool.TaskStatus {
	message := t.eventDispatcherManager.CollectHeartbeatInfo()
	t.eventDispatcherManager.HeartbeatRequestQueue.Enqueue(message)
	return threadpool.Waiting

}

func (t *HeartbeatSendTask) Await() threadpool.TaskStatus {
	select {
	case <-t.ticker.C:
		return threadpool.Running
	default:
		return threadpool.Waiting
	}
}

func (t *HeartbeatSendTask) Release() {
	t.ticker.Stop()
}

// 将从 queue 里面收到的信息分发给各个 dispatcher，每个 dispatcherManager 有一个这个task
/*
HeartbeatRecvTask is responsible for dispatching heartbeat response to each dispatchers
in the event dispatcher manager from HeartbeatResponseQueue.
Each event dispatcher manager corresponds a HeartbeatRecvTask.
*/
type HeartbeatRecvTask struct {
	eventDispatcherManager *EventDispatcherManager
	taskStatus             threadpool.TaskStatus
}

func newHeartbeatRecvTask(m *EventDispatcherManager) *HeartbeatRecvTask {
	return &HeartbeatRecvTask{
		eventDispatcherManager: m,
	}
}

func (t *HeartbeatRecvTask) GetStatus() threadpool.TaskStatus {
	return t.taskStatus
}

func (t *HeartbeatRecvTask) SetStatus(taskStatus threadpool.TaskStatus) {
	t.taskStatus = taskStatus
}

func (t *HeartbeatRecvTask) Await() threadpool.TaskStatus {
}

func (t *HeartbeatRecvTask) Release() {
}

func (t *HeartbeatRecvTask) Execute(timeout time.Duration) threadpool.TaskStatus {
	for {
		heartbeatResponse := t.eventDispatcherManager.HeartbeatResponseQueue.Dequeue()
		tableProgressInfo := heartbeatResponse.Info
		for _, info := range tableProgressInfo {
			dispatcherId, err := uuid.Parse(info.DispatcherID)
			if err != nil {
				log.Error("invalid dispatcher id", zap.Any("info.DispatcherID", info.DispatcherID), zap.Error(err))
				continue
			}
			// if dispatcherId == dispatcher.TableTriggerEventDispatcherId {
			// 	dispatcherItem := t.eventDispatcherManager.TableTriggerEventDispatcher
			// 	var message dispatcher.HeartBeatResponseMessage
			// 	for _, progress := range info.TableProgresses {
			// 		message.OtherTableProgress = append(message.OtherTableProgress, &dispatcher.TableSpanProgress{
			// 			Span: &common.TableSpan{
			// 				TableID:  progress.Span.TableID,
			// 				StartKey: progress.Span.StartKey,
			// 				EndKey:   progress.Span.EndKey,
			// 			},
			// 			IsBlocked:    progress.IsBlocked,
			// 			BlockTs:      progress.BlockTs,
			// 			CheckpointTs: progress.CheckpointTs,
			// 		})
			// 	}
			// 	message.Action = dispatcher.Action(info.Action)
			// 	dispatcherItem.HeartbeatChan <- &message
			// 	continue
			// }
			if dispatcherItem, ok := t.eventDispatcherManager.DispatcherMap[common.DispatcherID(dispatcherId)]; ok {
				var message dispatcher.HeartBeatResponseMessage
				for _, progress := range info.TableProgresses {
					message.OtherTableProgress = append(message.OtherTableProgress, &dispatcher.TableSpanProgress{
						Span: &common.TableSpan{
							TableID:  progress.Span.TableID,
							StartKey: progress.Span.StartKey,
							EndKey:   progress.Span.EndKey,
						},
						IsBlocked:    progress.IsBlocked,
						BlockTs:      progress.BlockTs,
						CheckpointTs: progress.CheckpointTs,
					})
				}
				message.Action = dispatcher.Action(info.Action)
				dispatcherItem.HeartbeatChan <- &message
			}

		}
	}
}
