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
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/messaging"
)

/*
HeartbeatRequestQueue is a channel for all event dispatcher managers to send heartbeat requests to HeartBeatCollector
*/

type HeartBeatRequestWithTargetID struct {
	TargetID messaging.ServerId
	Request  *heartbeatpb.HeartBeatRequest
}

type HeartbeatRequestQueue struct {
	queue chan *HeartBeatRequestWithTargetID
}

func NewHeartbeatRequestQueue() *HeartbeatRequestQueue {
	return &HeartbeatRequestQueue{
		queue: make(chan *HeartBeatRequestWithTargetID, 100000), // 大小后面再说
	}
}

func (q *HeartbeatRequestQueue) Enqueue(request *HeartBeatRequestWithTargetID) {
	q.queue <- request
}

func (q *HeartbeatRequestQueue) Dequeue() *HeartBeatRequestWithTargetID {
	return <-q.queue
}

func (q *HeartbeatRequestQueue) Close() {
	close(q.queue)
}

/*
HeartbeatResponseQueue is a channel for HeartBeatCollector to send heartbeat response to all event dispatcher managers.
*/
type HeartbeatResponseQueue struct {
	queue chan *heartbeatpb.HeartBeatResponse
}

func NewHeartbeatResponseQueue() *HeartbeatResponseQueue {
	return &HeartbeatResponseQueue{
		queue: make(chan *heartbeatpb.HeartBeatResponse, 1000), // 带缓冲的 channel
	}
}

func (q *HeartbeatResponseQueue) Enqueue(response *heartbeatpb.HeartBeatResponse) {
	q.queue <- response
}

func (q *HeartbeatResponseQueue) Dequeue() *heartbeatpb.HeartBeatResponse {
	return <-q.queue
}

func (q *HeartbeatResponseQueue) Close() {
	close(q.queue)
}
