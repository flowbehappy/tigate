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

	"github.com/flowbehappy/tigate/downstreamadapter/dispatcher"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/utils/dynstream"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/tiflow/cdc/model"
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
	t.taskHandle = taskScheduler.Submit(t, threadpool.CPUTask, time.Now().Add(time.Second*1))
	return t
}

func (t *HeartBeatTask) Execute() (threadpool.TaskStatus, time.Time) {
	if t.manager.closed.Load() {
		return threadpool.Done, time.Time{}
	}
	t.counter = (t.counter + 1) % 10
	needCompleteStatus := t.counter == 0
	message := t.manager.CollectHeartbeatInfo(needCompleteStatus)
	t.manager.GetHeartbeatRequestQueue().Enqueue(&HeartBeatRequestWithTargetID{TargetID: t.manager.GetMaintainerID(), Request: message})
	return threadpool.CPUTask, time.Now().Add(time.Second * 1)
}

func (t *HeartBeatTask) Cancel() {
	t.taskHandle.Cancel()
}

var heartBeatTaskSchedulerOnce sync.Once
var heartBeatTaskScheduler *threadpool.TaskScheduler

func GetHeartBeatTaskScheduler() *threadpool.TaskScheduler {
	if heartBeatTaskScheduler == nil {
		heartBeatTaskSchedulerOnce.Do(func() {
			heartBeatTaskScheduler = threadpool.NewTaskSchedulerDefault("HeartBeatTaskScheduler")
		})
	}
	return heartBeatTaskScheduler
}

func SetHeartBeatTaskScheduler(taskScheduler *threadpool.TaskScheduler) {
	heartBeatTaskScheduler = taskScheduler
}

var schedulerDispatcherRequestDynamicStream dynstream.DynamicStream[model.ChangeFeedID, *heartbeatpb.ScheduleDispatcherRequest, *EventDispatcherManager]
var schedulerDispatcherRequestDynamicStreamOnce sync.Once

func GetSchedulerDispatcherRequestDynamicStream() dynstream.DynamicStream[model.ChangeFeedID, *heartbeatpb.ScheduleDispatcherRequest, *EventDispatcherManager] {
	if schedulerDispatcherRequestDynamicStream == nil {
		schedulerDispatcherRequestDynamicStreamOnce.Do(func() {
			schedulerDispatcherRequestDynamicStream = dynstream.NewDynamicStreamDefault(&SchedulerDispatcherRequestHandler{})
			schedulerDispatcherRequestDynamicStream.Start()
		})
	}
	return schedulerDispatcherRequestDynamicStream
}

func SetSchedulerDispatcherRequestDynamicStream(dynamicStream dynstream.DynamicStream[model.ChangeFeedID, *heartbeatpb.ScheduleDispatcherRequest, *EventDispatcherManager]) {
	schedulerDispatcherRequestDynamicStream = dynamicStream
}

var heartBeatResponseDynamicStream dynstream.DynamicStream[model.ChangeFeedID, *heartbeatpb.HeartBeatResponse, *EventDispatcherManager]
var heartBeatResponseDynamicStreamOnce sync.Once

func GetHeartBeatResponseDynamicStream() dynstream.DynamicStream[model.ChangeFeedID, *heartbeatpb.HeartBeatResponse, *EventDispatcherManager] {
	if heartBeatResponseDynamicStream == nil {
		heartBeatResponseDynamicStreamOnce.Do(func() {
			heartBeatResponseDynamicStream = dynstream.NewDynamicStreamDefault(&HeartBeatResponseHandler{dispatcher.GetDispatcherStatusDynamicStream()})
			heartBeatResponseDynamicStream.Start()
		})
	}
	return heartBeatResponseDynamicStream
}

func SetHeartBeatResponseDynamicStream(dynamicStream dynstream.DynamicStream[model.ChangeFeedID, *heartbeatpb.HeartBeatResponse, *EventDispatcherManager]) {
	heartBeatResponseDynamicStream = dynamicStream
}
