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
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/pingcap/ticdc/utils/threadpool"
)

type HeartBeatTask struct {
	taskHandle *threadpool.TaskHandle
	manager    *EventDispatcherManager
	// Used to determine when to collect complete status
	statusTick int
}

func newHeartBeatTask(manager *EventDispatcherManager) *HeartBeatTask {
	taskScheduler := GetHeartBeatTaskScheduler()
	t := &HeartBeatTask{
		manager:    manager,
		statusTick: 0,
	}
	t.taskHandle = taskScheduler.Submit(t, time.Now().Add(time.Second*1))
	return t
}

func (t *HeartBeatTask) Execute() time.Time {
	if t.manager.closed.Load() {
		return time.Time{}
	}
	executeInterval := time.Millisecond * 200
	completeStatusInterval := int(time.Second * 10 / executeInterval)
	t.statusTick++
	needCompleteStatus := (t.statusTick)%completeStatusInterval == 0
	message := t.manager.aggregateDispatcherHeartbeats(needCompleteStatus)
	t.manager.heartbeatRequestQueue.Enqueue(&HeartBeatRequestWithTargetID{TargetID: t.manager.GetMaintainerID(), Request: message})
	return time.Now().Add(executeInterval)
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

var schedulerDispatcherRequestDynamicStream dynstream.DynamicStream[int, common.GID, SchedulerDispatcherRequest, *EventDispatcherManager, *SchedulerDispatcherRequestHandler]
var schedulerDispatcherRequestDynamicStreamOnce sync.Once

func GetSchedulerDispatcherRequestDynamicStream() dynstream.DynamicStream[int, common.GID, SchedulerDispatcherRequest, *EventDispatcherManager, *SchedulerDispatcherRequestHandler] {
	if schedulerDispatcherRequestDynamicStream == nil {
		schedulerDispatcherRequestDynamicStreamOnce.Do(func() {
			option := dynstream.NewOption()
			option.BatchCount = 128
			schedulerDispatcherRequestDynamicStream = dynstream.NewParallelDynamicStream(
				func(id common.GID) uint64 { return id.FastHash() },
				&SchedulerDispatcherRequestHandler{}, option)
			schedulerDispatcherRequestDynamicStream.Start()
		})
	}
	return schedulerDispatcherRequestDynamicStream
}

func SetSchedulerDispatcherRequestDynamicStream(dynamicStream dynstream.DynamicStream[int, common.GID, SchedulerDispatcherRequest, *EventDispatcherManager, *SchedulerDispatcherRequestHandler]) {
	schedulerDispatcherRequestDynamicStream = dynamicStream
}

type HeartBeatResponse struct {
	*heartbeatpb.HeartBeatResponse
}

func NewHeartBeatResponse(resp *heartbeatpb.HeartBeatResponse) HeartBeatResponse {
	return HeartBeatResponse{resp}
}

var heartBeatResponseDynamicStream dynstream.DynamicStream[int, common.GID, HeartBeatResponse, *EventDispatcherManager, *HeartBeatResponseHandler]
var heartBeatResponseDynamicStreamOnce sync.Once

func GetHeartBeatResponseDynamicStream() dynstream.DynamicStream[int, common.GID, HeartBeatResponse, *EventDispatcherManager, *HeartBeatResponseHandler] {
	if heartBeatResponseDynamicStream == nil {
		heartBeatResponseDynamicStreamOnce.Do(func() {
			heartBeatResponseDynamicStream = dynstream.NewParallelDynamicStream(
				func(id common.GID) uint64 { return id.FastHash() },
				&HeartBeatResponseHandler{dispatcher.GetDispatcherStatusDynamicStream()})
			heartBeatResponseDynamicStream.Start()
		})
	}
	return heartBeatResponseDynamicStream
}

func SetHeartBeatResponseDynamicStream(dynamicStream dynstream.DynamicStream[int, common.GID, HeartBeatResponse, *EventDispatcherManager, *HeartBeatResponseHandler]) {
	heartBeatResponseDynamicStream = dynamicStream
}

type CheckpointTsMessage struct {
	*heartbeatpb.CheckpointTsMessage
}

func NewCheckpointTsMessage(msg *heartbeatpb.CheckpointTsMessage) CheckpointTsMessage {
	return CheckpointTsMessage{msg}
}

var checkpointTsMessageDynamicStream dynstream.DynamicStream[int, common.GID, CheckpointTsMessage, *EventDispatcherManager, *CheckpointTsMessageHandler]
var checkpointTsMessageDynamicStreamOnce sync.Once

func GetCheckpointTsMessageDynamicStream() dynstream.DynamicStream[int, common.GID, CheckpointTsMessage, *EventDispatcherManager, *CheckpointTsMessageHandler] {
	if checkpointTsMessageDynamicStream == nil {
		checkpointTsMessageDynamicStreamOnce.Do(func() {
			checkpointTsMessageDynamicStream = dynstream.NewParallelDynamicStream(
				func(id common.GID) uint64 { return id.FastHash() },
				&CheckpointTsMessageHandler{})
			checkpointTsMessageDynamicStream.Start()
		})
	}
	return checkpointTsMessageDynamicStream
}

type DispatcherMap struct {
	m     sync.Map
	mutex sync.Mutex // mutex is protected when the seq is need to get or set, and make the seq and map change atomic.
	seq   uint64     // sequence number is increasing when changed
}

func newDispatcherMap() *DispatcherMap {
	return &DispatcherMap{
		m:   sync.Map{},
		seq: 0,
	}
}

func (d *DispatcherMap) Len() int {
	var len = 0
	d.m.Range(func(_, _ interface{}) bool {
		len++
		return true
	})
	return len
}

func (d *DispatcherMap) Get(id common.DispatcherID) (*dispatcher.Dispatcher, bool) {
	dispatcherItem, ok := d.m.Load(id)
	if ok {
		return dispatcherItem.(*dispatcher.Dispatcher), ok
	}
	return nil, false
}

func (d *DispatcherMap) GetSeq() uint64 {
	d.mutex.Lock()
	d.mutex.Unlock()
	return d.seq
}

func (d *DispatcherMap) Delete(id common.DispatcherID) {
	d.mutex.Lock()
	d.mutex.Unlock()
	d.m.Delete(id)
	d.seq++
}

func (d *DispatcherMap) Set(id common.DispatcherID, dispatcher *dispatcher.Dispatcher) uint64 {
	d.mutex.Lock()
	d.mutex.Unlock()
	d.m.Store(id, dispatcher)
	d.seq++

	return d.seq
}

func (d *DispatcherMap) ForEach(fn func(id common.DispatcherID, dispatcher *dispatcher.Dispatcher)) uint64 {
	d.mutex.Lock()
	d.mutex.Unlock()
	d.m.Range(func(key, value interface{}) bool {
		fn(key.(common.DispatcherID), value.(*dispatcher.Dispatcher))
		return true
	})
	return d.seq
}

func toFilterConfigPB(filter *config.FilterConfig) *eventpb.FilterConfig {
	filterConfig := &eventpb.FilterConfig{
		Rules:            filter.Rules,
		IgnoreTxnStartTs: filter.IgnoreTxnStartTs,
		EventFilters:     make([]*eventpb.EventFilterRule, len(filter.EventFilters)),
	}

	for _, eventFilter := range filter.EventFilters {
		ignoreEvent := make([]string, len(eventFilter.IgnoreEvent))
		for _, event := range eventFilter.IgnoreEvent {
			ignoreEvent = append(ignoreEvent, string(event))
		}
		filterConfig.EventFilters = append(filterConfig.EventFilters, &eventpb.EventFilterRule{
			Matcher:                  eventFilter.Matcher,
			IgnoreEvent:              ignoreEvent,
			IgnoreSql:                eventFilter.IgnoreSQL,
			IgnoreInsertValueExpr:    eventFilter.IgnoreInsertValueExpr,
			IgnoreUpdateNewValueExpr: eventFilter.IgnoreUpdateNewValueExpr,
			IgnoreUpdateOldValueExpr: eventFilter.IgnoreUpdateOldValueExpr,
			IgnoreDeleteValueExpr:    eventFilter.IgnoreDeleteValueExpr,
		})
	}

	return filterConfig
}

type TableSpanStatusWithSeq struct {
	*heartbeatpb.TableSpanStatus
	StartTs uint64
	Seq     uint64
}

type Watermark struct {
	mutex sync.Mutex
	*heartbeatpb.Watermark
}

func NewWatermark(ts uint64) Watermark {
	return Watermark{
		Watermark: &heartbeatpb.Watermark{
			CheckpointTs: ts,
			ResolvedTs:   ts,
		},
	}
}

func (w *Watermark) Get() *heartbeatpb.Watermark {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.Watermark
}

func (w *Watermark) Set(watermark *heartbeatpb.Watermark) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.Watermark = watermark
}
