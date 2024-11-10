// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package dispatchermanager

import (
	"sync"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/pingcap/ticdc/utils/threadpool"
)

type HeartBeatTask struct {
	taskHandle *threadpool.TaskHandle
	manager    *EventDispatcherManager
	counter    int
}

func newHeartBeatTask(manager *EventDispatcherManager) *HeartBeatTask {
	taskScheduler := GetHeartBeatTaskScheduler()
	t := &HeartBeatTask{
		manager: manager,
		counter: 0,
	}
	t.taskHandle = taskScheduler.Submit(t, time.Now().Add(time.Second*1))
	return t
}

func (t *HeartBeatTask) Execute() time.Time {
	if t.manager.closed.Load() {
		return time.Time{}
	}
	t.counter = (t.counter + 1) % 10
	needCompleteStatus := t.counter == 0
	message := t.manager.CollectHeartbeatInfo(needCompleteStatus)
	t.manager.heartbeatRequestQueue.Enqueue(&HeartBeatRequestWithTargetID{TargetID: t.manager.GetMaintainerID(), Request: message})
	return time.Now().Add(time.Second * 1)
}

func (t *HeartBeatTask) Cancel() {
	t.taskHandle.Cancel()
}

var heartBeatTaskSchedulerOnce sync.Once
var heartBeatTaskScheduler threadpool.ThreadPool

func GetHeartBeatTaskScheduler() threadpool.ThreadPool {
	if heartBeatTaskScheduler == nil {
		heartBeatTaskSchedulerOnce.Do(func() {
			heartBeatTaskScheduler = threadpool.NewThreadPoolDefault()
		})
	}
	return heartBeatTaskScheduler
}

func SetHeartBeatTaskScheduler(taskScheduler threadpool.ThreadPool) {
	heartBeatTaskScheduler = taskScheduler
}

type SchedulerDispatcherRequest struct {
	*heartbeatpb.ScheduleDispatcherRequest
}

func NewSchedulerDispatcherRequest(req *heartbeatpb.ScheduleDispatcherRequest) SchedulerDispatcherRequest {
	return SchedulerDispatcherRequest{req}
}

var schedulerDispatcherRequestDynamicStream dynstream.DynamicStream[int, common.ChangeFeedID, SchedulerDispatcherRequest, *EventDispatcherManager, *SchedulerDispatcherRequestHandler]
var schedulerDispatcherRequestDynamicStreamOnce sync.Once

func GetSchedulerDispatcherRequestDynamicStream() dynstream.DynamicStream[int, common.ChangeFeedID, SchedulerDispatcherRequest, *EventDispatcherManager, *SchedulerDispatcherRequestHandler] {
	if schedulerDispatcherRequestDynamicStream == nil {
		schedulerDispatcherRequestDynamicStreamOnce.Do(func() {
			option := dynstream.NewOption()
			option.BatchCount = 128
			schedulerDispatcherRequestDynamicStream = dynstream.NewDynamicStream(&SchedulerDispatcherRequestHandler{}, option)
			schedulerDispatcherRequestDynamicStream.Start()
		})
	}
	return schedulerDispatcherRequestDynamicStream
}

func SetSchedulerDispatcherRequestDynamicStream(dynamicStream dynstream.DynamicStream[int, common.ChangeFeedID, SchedulerDispatcherRequest, *EventDispatcherManager, *SchedulerDispatcherRequestHandler]) {
	schedulerDispatcherRequestDynamicStream = dynamicStream
}

type HeartBeatResponse struct {
	*heartbeatpb.HeartBeatResponse
}

func NewHeartBeatResponse(resp *heartbeatpb.HeartBeatResponse) HeartBeatResponse {
	return HeartBeatResponse{resp}
}

var heartBeatResponseDynamicStream dynstream.DynamicStream[int, common.ChangeFeedID, HeartBeatResponse, *EventDispatcherManager, *HeartBeatResponseHandler]
var heartBeatResponseDynamicStreamOnce sync.Once

func GetHeartBeatResponseDynamicStream() dynstream.DynamicStream[int, common.ChangeFeedID, HeartBeatResponse, *EventDispatcherManager, *HeartBeatResponseHandler] {
	if heartBeatResponseDynamicStream == nil {
		heartBeatResponseDynamicStreamOnce.Do(func() {
			heartBeatResponseDynamicStream = dynstream.NewDynamicStream(&HeartBeatResponseHandler{dispatcher.GetDispatcherStatusDynamicStream()})
			heartBeatResponseDynamicStream.Start()
		})
	}
	return heartBeatResponseDynamicStream
}

func SetHeartBeatResponseDynamicStream(dynamicStream dynstream.DynamicStream[int, common.ChangeFeedID, HeartBeatResponse, *EventDispatcherManager, *HeartBeatResponseHandler]) {
	heartBeatResponseDynamicStream = dynamicStream
}

type CheckpointTsMessage struct {
	*heartbeatpb.CheckpointTsMessage
}

func NewCheckpointTsMessage(msg *heartbeatpb.CheckpointTsMessage) CheckpointTsMessage {
	return CheckpointTsMessage{msg}
}

var checkpointTsMessageDynamicStream dynstream.DynamicStream[int, common.ChangeFeedID, CheckpointTsMessage, *EventDispatcherManager, *CheckpointTsMessageHandler]
var checkpointTsMessageDynamicStreamOnce sync.Once

func GetCheckpointTsMessageDynamicStream() dynstream.DynamicStream[int, common.ChangeFeedID, CheckpointTsMessage, *EventDispatcherManager, *CheckpointTsMessageHandler] {
	if checkpointTsMessageDynamicStream == nil {
		checkpointTsMessageDynamicStreamOnce.Do(func() {
			checkpointTsMessageDynamicStream = dynstream.NewDynamicStream(&CheckpointTsMessageHandler{})
			checkpointTsMessageDynamicStream.Start()
		})
	}
	return checkpointTsMessageDynamicStream
}
