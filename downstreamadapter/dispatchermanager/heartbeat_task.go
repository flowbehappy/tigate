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
	"github.com/flowbehappy/tigate/utils/threadpool"
)

/*
HeartbeatSendTask is responsible for collecting heartbeat info periodically from
all dispatchers in the event dispatcher manager and sending them to the HeartbeatRequestQueue.
Each event dispatcher manager corresponds a HeartbeatSendTask.
*/
type HeartBeatSendTask struct {
	eventDispatcherManager *EventDispatcherManager
	counter                int // use to decide whether we need to collect complete table progress info
	taskHandle             *threadpool.TaskHandle
}

/*
func newHeartBeatSendTask(m *EventDispatcherManager) *HeartBeatSendTask {
	task := &HeartBeatSendTask{
		eventDispatcherManager: m,
		counter:                0,
	}
	task.taskHandle = threadpool.GetTaskSchedulerInstance().HeartbeatTaskScheduler.Submit(task, threadpool.CPUTask, time.Now().Add(1*time.Second))
	return task
}

func (t *HeartBeatSendTask) Execute() (threadpool.TaskStatus, time.Time) {
	t.counter = (t.counter + 1) % 10
	needCompleteStatus := t.counter == 0
	message := t.eventDispatcherManager.CollectHeartbeatInfo(needCompleteStatus)
	t.eventDispatcherManager.GetHeartbeatRequestQueue().Enqueue(&HeartBeatRequestWithTargetID{TargetID: t.eventDispatcherManager.GetMaintainerID(), Request: message})
	return threadpool.CPUTask, time.Now().Add(time.Second * 1)

}

func (t *HeartBeatSendTask) Cancel() {
	t.taskHandle.Cancel()
}*/

// 将从 queue 里面收到的信息分发给各个 dispatcher，每个 dispatcherManager 有一个这个task
/*
HeartbeatRecvTask is responsible for dispatching heartbeat response to each dispatchers
in the event dispatcher manager from HeartbeatResponseQueue.
Each event dispatcher manager corresponds a HeartbeatRecvTask.
*/

/*
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

// func (t *HeartbeatRecvTask) Await() threadpool.TaskStatus {
// }

func (t *HeartbeatRecvTask) Release() {
}

func (t *HeartbeatRecvTask) Execute() (threadpool.TaskStatus, time.Time) {
	for {
		heartbeatResponse := t.eventDispatcherManager.HeartbeatResponseQueue.Dequeue()
		tableProgressInfo := heartbeatResponse.Info
		for _, info := range tableProgressInfo {
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
			tableSpan := common.TableSpan{TableSpan: info.Span}
			dispatcherItem := t.eventDispatcherManager.GetDispatcherMap().Get(&tableSpan)
			if dispatcherItem != nil {
				var message dispatcher.HeartBeatResponseMessage
				for _, progress := range info.TableProgresses {
					message.OtherTableProgress = append(message.OtherTableProgress, &dispatcher.TableSpanProgress{
						Span: &common.TableSpan{
							TableSpan: progress.Span,
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

func (t *HeartbeatRecvTask) Cancel() {
}
*/
