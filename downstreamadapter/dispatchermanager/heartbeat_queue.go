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

import "github.com/flowbehappy/tigate/heartbeatpb"

type HeartbeatRequestQueue struct {
	queue chan *heartbeatpb.HeartBeatRequest
}

func NewHeartbeatRequestQueue() *HeartbeatRequestQueue {
	return &HeartbeatRequestQueue{
		queue: make(chan *heartbeatpb.HeartBeatRequest, 1000), // 带缓冲的 channel
	}
}

// Enqueue 向队列中添加消息
func (q *HeartbeatRequestQueue) Enqueue(request *heartbeatpb.HeartBeatRequest) {
	q.queue <- request
}

// Dequeue 从队列中移除并返回一条消息
func (q *HeartbeatRequestQueue) Dequeue() *heartbeatpb.HeartBeatRequest {
	return <-q.queue
}

// Close 关闭队列的 channel
func (q *HeartbeatRequestQueue) Close() {
	close(q.queue)
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
