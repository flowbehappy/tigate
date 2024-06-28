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
	"github.com/flowbehappy/tigate/utils/threadpool"
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

func (t *HeartbeatSendTask) Execute(timeout time.Duration) threadpool.TaskStatus {
	select {
	case <-t.ticker.C:
		message := t.eventDispatcherManager.CollectHeartbeatInfo()
		t.eventDispatcherManager.HeartbeatRequestQueue.Enqueue(message)
		return threadpool.Running
	default:
		return threadpool.Running
	}
}

func (t *HeartbeatSendTask) Await() threadpool.TaskStatus {
	//
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
func (t *HeartbeatRecvTask) Await() threadpool.TaskStatus {
}

func (t *HeartbeatRecvTask) Release() {
}

func (t *HeartbeatRecvTask) Execute(timeout time.Duration) threadpool.TaskStatus {
	for {
		heartbeatResponse := t.eventDispatcherManager.HeartbeatResponseQueue.Dequeue()
		tableProgressInfo := heartbeatResponse.Info
		for _, info := range tableProgressInfo {
			dispatcherId := info.DispatcherID
			if dispatcherId == dispatcher.TableTriggerEventDispatcherId {
				dispatcherItem := t.eventDispatcherManager.TableTriggerEventDispatcher
				var message dispatcher.HeartBeatResponseMessage
				for _, progress := range info.TableProgresses {
					message.OtherTableProgress = append(message.OtherTableProgress, &dispatcher.TableSpanProgress{
						Span:         progress.Span,
						IsBlocked:    progress.IsBlocked,
						BlockTs:      progress.BlockTs,
						CheckpointTs: progress.CheckpointTs,
					})
				}
				message.Action = dispatcher.Action(info.Action)
				dispatcherItem.HeartbeatChan <- &message
				continue
			}
			if dispatcherItem, ok := t.eventDispatcherManager.DispatcherMap[dispatcherId]; ok {
				var message dispatcher.HeartBeatResponseMessage
				for _, progress := range info.TableProgresses {
					message.OtherTableProgress = append(message.OtherTableProgress, &dispatcher.TableSpanProgress{
						Span:         progress.Span,
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
