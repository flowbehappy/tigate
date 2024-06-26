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
	"new_arch/downstreamadapter/dispatcher"
	"new_arch/heartbeatpb"
	"new_arch/utils/threadpool"
	"time"
)

// 一个 manager 对应一个 send task，主要用于把这个 manager 收集到的 heart beat 扔到 queue 里面去
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
	// 从 channel 里拿数据，然后分发给各个 dispatcher
	for {
		heartbeatResponse := t.eventDispatcherManager.HeartbeatResponseQueue.Dequeue()
		tableProgressInfo := heartbeatResponse.Info
		for _, info := range tableProgressInfo {
			dispatcherId := info.DispatcherID
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

type HeartbeatResponseQueue struct {
	queue chan *heartbeatpb.HeartBeatResponse
}

func NewHeartbeatResponseQueue() *HeartbeatResponseQueue {
	return &HeartbeatResponseQueue{
		queue: make(chan *heartbeatpb.HeartBeatResponse, 1000), // 带缓冲的 channel
	}
}

// Enqueue 向队列中添加消息
func (q *HeartbeatResponseQueue) Enqueue(response *heartbeatpb.HeartBeatResponse) {
	q.queue <- response
}

// Dequeue 从队列中移除并返回一条消息
func (q *HeartbeatResponseQueue) Dequeue() *heartbeatpb.HeartBeatResponse {
	return <-q.queue
}

// Close 关闭队列的 channel
func (q *HeartbeatResponseQueue) Close() {
	close(q.queue)
}
