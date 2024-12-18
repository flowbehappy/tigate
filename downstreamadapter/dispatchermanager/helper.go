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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/pingcap/ticdc/utils/threadpool"
	"go.uber.org/zap"
)

type DispatcherMap struct {
	m sync.Map
	// mutex is used to protect the seq.
	// Here we don't need to make seq changes always atmoic with the m changed.
	// Our target is :
	// The seq get from ForEach should be smaller than the seq get from Set
	// when ForEach is not access the new dispatcher just Set.
	// So we add seq after the dispatcher is add in the m for Set, and get the seq before do range for ForRange.
	mutex sync.Mutex
	// sequence number is increasing when dispatcher is added.
	//
	// Seq is used to prevent the fallback of changefeed's checkpointTs.
	// When some new dispatcher(table) is being added, the maintainer will block the forward of changefeed's checkpointTs
	// until the maintainer receive the message that the new dispatcher's component status change to working
	// Besides, there is no strict order of the heartbeat message and the table status messages, which is means
	// it can happen that when dispatcher A is created, event dispatcher manager may first send a table status message
	// to show the new dispatcher is working, and then send a heartbeat message of the current watermark,
	// which is calculated without the new disaptcher.
	// When the checkpoinTs of the watermark is large than the startTs of the new dispatcher,
	// the watermark of next heartbeat, which calculated with the new dispatcher can be less than the previous watermark.
	// Then it can cause the fallback of changefeed's checkpointTs.
	// To avoid fallback, we add a seq number in each heartbeat message(including table span messages)
	// When a table is added the seq number will be increase,
	// and when the maintainer receive the outdate seq, it will know the heartbeat message is outdate and ignore it.
	// In this way, even the above case happens, the changefeed's checkpointTs will not fallback.
	seq uint64
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
	defer d.mutex.Unlock()
	return d.seq
}

func (d *DispatcherMap) Delete(id common.DispatcherID) {
	d.m.Delete(id)
}

func (d *DispatcherMap) Set(id common.DispatcherID, dispatcher *dispatcher.Dispatcher) uint64 {
	d.m.Store(id, dispatcher)
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.seq++
	return d.seq
}

func (d *DispatcherMap) ForEach(fn func(id common.DispatcherID, dispatcher *dispatcher.Dispatcher)) uint64 {
	var seq uint64
	d.mutex.Lock()
	seq = d.seq
	d.mutex.Unlock()
	d.m.Range(func(key, value interface{}) bool {
		fn(key.(common.DispatcherID), value.(*dispatcher.Dispatcher))
		return true
	})
	return seq
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

// HeartbeatTask is a perioic task to collect the heartbeat status from event dispatcher manager and push to heartbeatRequestQueue
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

// schedulerDispatcherRequestDynamicStream is responsible for create or remove the dispatchers.
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

type SchedulerDispatcherRequest struct {
	*heartbeatpb.ScheduleDispatcherRequest
}

func NewSchedulerDispatcherRequest(req *heartbeatpb.ScheduleDispatcherRequest) SchedulerDispatcherRequest {
	return SchedulerDispatcherRequest{req}
}

type SchedulerDispatcherRequestHandler struct {
}

func (h *SchedulerDispatcherRequestHandler) Path(scheduleDispatcherRequest SchedulerDispatcherRequest) common.GID {
	return common.NewChangefeedGIDFromPB(scheduleDispatcherRequest.ChangefeedID)
}

func (h *SchedulerDispatcherRequestHandler) Handle(eventDispatcherManager *EventDispatcherManager, reqs ...SchedulerDispatcherRequest) bool {
	// If req is about remove dispatcher, then there will only be one request in reqs.
	infos := make([]dispatcherCreateInfo, 0, len(reqs))
	for _, req := range reqs {
		if req.ScheduleDispatcherRequest == nil {
			log.Warn("scheduleDispatcherRequest is nil, skip")
			continue
		}
		config := req.Config
		dispatcherID := common.NewDispatcherIDFromPB(config.DispatcherID)
		switch req.ScheduleAction {
		case heartbeatpb.ScheduleAction_Create:
			infos = append(infos, dispatcherCreateInfo{
				Id:          dispatcherID,
				TableSpan:   config.Span,
				StartTs:     config.StartTs,
				SchemaID:    config.SchemaID,
				CurrentPDTs: config.CurrentPdTs,
			})
		case heartbeatpb.ScheduleAction_Remove:
			if len(reqs) != 1 {
				log.Error("invalid remove dispatcher request count in one batch", zap.Int("count", len(reqs)))
			}
			eventDispatcherManager.removeDispatcher(dispatcherID)
		}
	}
	if len(infos) > 0 {
		err := eventDispatcherManager.newDispatchers(infos)
		if err != nil {
			select {
			case eventDispatcherManager.errCh <- err:
			default:
				log.Error("error channel is full, discard error",
					zap.Any("ChangefeedID", eventDispatcherManager.changefeedID.String()),
					zap.Error(err))
			}
		}
	}
	return false
}

func (h *SchedulerDispatcherRequestHandler) GetSize(event SchedulerDispatcherRequest) int { return 0 }
func (h *SchedulerDispatcherRequestHandler) IsPaused(event SchedulerDispatcherRequest) bool {
	return false
}
func (h *SchedulerDispatcherRequestHandler) GetArea(path common.GID, dest *EventDispatcherManager) int {
	return 0
}
func (h *SchedulerDispatcherRequestHandler) GetTimestamp(event SchedulerDispatcherRequest) dynstream.Timestamp {
	return 0
}
func (h *SchedulerDispatcherRequestHandler) GetType(event SchedulerDispatcherRequest) dynstream.EventType {
	// we do batch for create dispatcher now.
	switch event.ScheduleAction {
	case heartbeatpb.ScheduleAction_Create:
		return dynstream.EventType{DataGroup: 1, Property: dynstream.BatchableData}
	case heartbeatpb.ScheduleAction_Remove:
		return dynstream.EventType{DataGroup: 2, Property: dynstream.NonBatchable}
	default:
		log.Panic("unknown schedule action", zap.Int("action", int(event.ScheduleAction)))
	}
	return dynstream.DefaultEventType
}

func (h *SchedulerDispatcherRequestHandler) OnDrop(event SchedulerDispatcherRequest) {}

// heartBeatResponseDynamicStream is responsible for send heartBeatResponse to the related dispatchers.
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

type HeartBeatResponse struct {
	*heartbeatpb.HeartBeatResponse
}

func NewHeartBeatResponse(resp *heartbeatpb.HeartBeatResponse) HeartBeatResponse {
	return HeartBeatResponse{resp}
}

func SetHeartBeatResponseDynamicStream(dynamicStream dynstream.DynamicStream[int, common.GID, HeartBeatResponse, *EventDispatcherManager, *HeartBeatResponseHandler]) {
	heartBeatResponseDynamicStream = dynamicStream
}

type HeartBeatResponseHandler struct {
	dispatcherStatusDynamicStream dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherStatusWithID, *dispatcher.Dispatcher, *dispatcher.DispatcherStatusHandler]
}

func NewHeartBeatResponseHandler() HeartBeatResponseHandler {
	return HeartBeatResponseHandler{dispatcherStatusDynamicStream: dispatcher.GetDispatcherStatusDynamicStream()}
}

func (h *HeartBeatResponseHandler) Path(HeartbeatResponse HeartBeatResponse) common.GID {
	return common.NewChangefeedGIDFromPB(HeartbeatResponse.ChangefeedID)
}

func (h *HeartBeatResponseHandler) Handle(eventDispatcherManager *EventDispatcherManager, resps ...HeartBeatResponse) bool {
	if len(resps) != 1 {
		// TODO: Support batch
		panic("invalid response count")
	}
	heartbeatResponse := resps[0]
	dispatcherStatuses := heartbeatResponse.GetDispatcherStatuses()
	for _, dispatcherStatus := range dispatcherStatuses {
		influencedDispatchersType := dispatcherStatus.InfluencedDispatchers.InfluenceType
		switch influencedDispatchersType {
		case heartbeatpb.InfluenceType_Normal:
			for _, dispatcherID := range dispatcherStatus.InfluencedDispatchers.DispatcherIDs {
				dispId := common.NewDispatcherIDFromPB(dispatcherID)
				h.dispatcherStatusDynamicStream.Push(
					dispId,
					dispatcher.NewDispatcherStatusWithID(dispatcherStatus, dispId))
			}
		case heartbeatpb.InfluenceType_DB:
			schemaID := dispatcherStatus.InfluencedDispatchers.SchemaID
			excludeDispatcherID := common.NewDispatcherIDFromPB(dispatcherStatus.InfluencedDispatchers.ExcludeDispatcherId)
			dispatcherIds := eventDispatcherManager.GetAllDispatchers(schemaID)
			for _, id := range dispatcherIds {
				if id != excludeDispatcherID {
					h.dispatcherStatusDynamicStream.Push(id, dispatcher.NewDispatcherStatusWithID(dispatcherStatus, id))
				}
			}
		case heartbeatpb.InfluenceType_All:
			excludeDispatcherID := common.NewDispatcherIDFromPB(dispatcherStatus.InfluencedDispatchers.ExcludeDispatcherId)
			eventDispatcherManager.GetDispatcherMap().ForEach(func(id common.DispatcherID, _ *dispatcher.Dispatcher) {
				if id != excludeDispatcherID {
					h.dispatcherStatusDynamicStream.Push(id, dispatcher.NewDispatcherStatusWithID(dispatcherStatus, id))
				}
			})
		}
	}
	return false
}

func (h *HeartBeatResponseHandler) GetSize(event HeartBeatResponse) int   { return 0 }
func (h *HeartBeatResponseHandler) IsPaused(event HeartBeatResponse) bool { return false }
func (h *HeartBeatResponseHandler) GetArea(path common.GID, dest *EventDispatcherManager) int {
	return 0
}
func (h *HeartBeatResponseHandler) GetTimestamp(event HeartBeatResponse) dynstream.Timestamp {
	return 0
}
func (h *HeartBeatResponseHandler) GetType(event HeartBeatResponse) dynstream.EventType {
	return dynstream.DefaultEventType
}
func (h *HeartBeatResponseHandler) OnDrop(event HeartBeatResponse) {}

// checkpointTsMessageDynamicStream is responsible for push checkpointTsMessage to the corresponding table trigger event dispatcher.
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

type CheckpointTsMessage struct {
	*heartbeatpb.CheckpointTsMessage
}

func NewCheckpointTsMessage(msg *heartbeatpb.CheckpointTsMessage) CheckpointTsMessage {
	return CheckpointTsMessage{msg}
}

type CheckpointTsMessageHandler struct{}

func NewCheckpointTsMessageHandler() CheckpointTsMessageHandler {
	return CheckpointTsMessageHandler{}
}

func (h *CheckpointTsMessageHandler) Path(checkpointTsMessage CheckpointTsMessage) common.GID {
	return common.NewChangefeedGIDFromPB(checkpointTsMessage.ChangefeedID)
}

func (h *CheckpointTsMessageHandler) Handle(eventDispatcherManager *EventDispatcherManager, messages ...CheckpointTsMessage) bool {
	if len(messages) != 1 {
		// TODO: Support batch
		panic("invalid message count")
	}
	checkpointTsMessage := messages[0]
	if eventDispatcherManager.tableTriggerEventDispatcher != nil && eventDispatcherManager.sink.SinkType() != common.MysqlSinkType {
		tableTriggerEventDispatcher := eventDispatcherManager.tableTriggerEventDispatcher
		tableTriggerEventDispatcher.HandleCheckpointTs(checkpointTsMessage.CheckpointTs)
	}
	return false
}

func (h *CheckpointTsMessageHandler) GetSize(event CheckpointTsMessage) int   { return 0 }
func (h *CheckpointTsMessageHandler) IsPaused(event CheckpointTsMessage) bool { return false }
func (h *CheckpointTsMessageHandler) GetArea(path common.GID, dest *EventDispatcherManager) int {
	return 0
}
func (h *CheckpointTsMessageHandler) GetTimestamp(event CheckpointTsMessage) dynstream.Timestamp {
	return 0
}
func (h *CheckpointTsMessageHandler) GetType(event CheckpointTsMessage) dynstream.EventType {
	return dynstream.DefaultEventType
}
func (h *CheckpointTsMessageHandler) OnDrop(event CheckpointTsMessage) {}
