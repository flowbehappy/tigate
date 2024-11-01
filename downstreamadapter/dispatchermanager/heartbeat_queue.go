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
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/node"
)

/*
HeartbeatRequestQueue is a channel for all event dispatcher managers to send heartbeat requests to HeartBeatCollector
*/

type HeartBeatRequestWithTargetID struct {
	TargetID node.ID
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

type BlockStatusRequestWithTargetID struct {
	TargetID node.ID
	Request  *heartbeatpb.BlockStatusRequest
}

type BlockStatusRequestQueue struct {
	queue chan *BlockStatusRequestWithTargetID
}

func NewBlockStatusRequestQueue() *BlockStatusRequestQueue {
	return &BlockStatusRequestQueue{
		queue: make(chan *BlockStatusRequestWithTargetID, 10000),
	}
}

func (q *BlockStatusRequestQueue) Enqueue(request *BlockStatusRequestWithTargetID) {
	q.queue <- request
}

func (q *BlockStatusRequestQueue) Dequeue() *BlockStatusRequestWithTargetID {
	return <-q.queue
}

func (q *BlockStatusRequestQueue) Close() {
	close(q.queue)
}
