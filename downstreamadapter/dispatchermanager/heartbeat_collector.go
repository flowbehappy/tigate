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

// 这个 grpc 应该也要做成 共享 stream 的感觉，然后开 goroutine，专门作为收发东西。
// send task 还是要存在的，定期收集
// recv 就不需要了，goroutine 里面收到以后直接扔到 channel 里，后面有需要再拆开好了

// 一个 dispatcher manager 需要一起通信么？要考虑打散么
type HearbeatCollector struct {
}

// 负责持续发消息的 task
type HeartbeatSendTask struct {
	ticker                 *time.Ticker
	eventDispatcherManager *EventDispatcherManager
}

func newHeartBeatSendTask() *HeartbeatSendTask {
	return &HeartbeatSendTask{
		ticker: time.NewTicker(50 * time.Millisecond),
	}
}

func (t *HeartbeatSendTask) execute(timeout time.Duration) threadpool.TaskStatus {
	select {
	case <-t.ticker.C:
		// gather info
		message := t.eventDispatcherManager.CollectHeartbeatInfo()
		// 然后把 message 可以扔到通道里去
		return threadpool.Running
	default:
		return threadpool.Running
	}
}

func (t *HeartbeatSendTask) await(timeout time.Duration) threadpool.TaskStatus {
	//
}

func (t *HeartbeatSendTask) release() {
	t.ticker.Stop()
}

// 将收到的信息分发给各个 dispatcher
type HeartbeatRecvTask struct {
	eventDispatcherManager *EventDispatcherManager
	queue                  *HeartbeatResponseQueue
	//chan
}

func newHeartbeatRecvTask() *HeartbeatRecvTask {

}

func (t *HeartbeatRecvTask) execute(timeout time.Duration) threadpool.TaskStatus {
	// 从 channel 里拿数据，然后分发给各个 dispatcher
	for {
		heartbeatResponse := t.queue.Dequeue()
		tableProgressInfo := heartbeatResponse.Info
		for _, info := range tableProgressInfo {
			dispatcherId := info.DispatcherID
			if dispatcherItem, ok := t.eventDispatcherManager.dispatcherMap[dispatcherId]; ok {
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
