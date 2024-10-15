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
	"context"
	"sync"

	"github.com/flowbehappy/tigate/pkg/node"

	"github.com/flowbehappy/tigate/downstreamadapter/dispatcher"
	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/flowbehappy/tigate/utils/dynstream"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

/*
HeartBeatCollect is responsible for sending heartbeat requests and receiving heartbeat responses by messageCenter
HeartBeatCollector is an instance-level component. It will deal with all the heartbeat messages from all dispatchers in all dispatcher managers.
*/
type HeartBeatCollector struct {
	wg   sync.WaitGroup
	from node.ID

	heartBeatReqQueue   *HeartbeatRequestQueue
	blockStatusReqQueue *BlockStatusRequestQueue

	heartBeatResponseDynamicStream          dynstream.DynamicStream[model.ChangeFeedID, HeartBeatResponse, *EventDispatcherManager]
	schedulerDispatcherRequestDynamicStream dynstream.DynamicStream[model.ChangeFeedID, SchedulerDispatcherRequest, *EventDispatcherManager]
	checkpointTsMessageDynamicStream        dynstream.DynamicStream[model.ChangeFeedID, CheckpointTsMessage, *EventDispatcherManager]

	mc messaging.MessageCenter
}

func NewHeartBeatCollector(serverId node.ID) *HeartBeatCollector {
	heartBeatCollector := HeartBeatCollector{
		from:                                    serverId,
		heartBeatReqQueue:                       NewHeartbeatRequestQueue(),
		blockStatusReqQueue:                     NewBlockStatusRequestQueue(),
		heartBeatResponseDynamicStream:          GetHeartBeatResponseDynamicStream(),
		schedulerDispatcherRequestDynamicStream: GetSchedulerDispatcherRequestDynamicStream(),
		checkpointTsMessageDynamicStream:        GetCheckpointTsMessageDynamicStream(),
		mc:                                      appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
	}
	heartBeatCollector.mc.RegisterHandler(messaging.HeartbeatCollectorTopic, heartBeatCollector.RecvMessages)

	heartBeatCollector.wg.Add(2)
	go func() {
		defer heartBeatCollector.wg.Done()
		heartBeatCollector.sendHeartBeatMessages()
	}()

	go func() {
		defer heartBeatCollector.wg.Done()
		heartBeatCollector.sendBlockStatusMessages()
	}()

	return &heartBeatCollector
}

func (c *HeartBeatCollector) RegisterEventDispatcherManager(m *EventDispatcherManager) error {
	m.SetHeartbeatRequestQueue(c.heartBeatReqQueue)
	m.SetBlockStatusRequestQueue(c.blockStatusReqQueue)
	err := c.heartBeatResponseDynamicStream.AddPath(m.changefeedID, m)
	if err != nil {
		log.Error("heartBeatResponseDynamicStream Failed to add path", zap.Any("ChangefeedID", m.changefeedID))
		return err
	}
	err = c.schedulerDispatcherRequestDynamicStream.AddPath(m.changefeedID, m)
	if err != nil {
		log.Error("schedulerDispatcherRequestDynamicStream Failed to add path", zap.Any("ChangefeedID", m.changefeedID))
		return err
	}
	return nil
}

func (c *HeartBeatCollector) sendHeartBeatMessages() {
	for {
		heartBeatRequestWithTargetID := c.heartBeatReqQueue.Dequeue()
		err := c.mc.SendCommand(
			messaging.NewSingleTargetMessage(
				heartBeatRequestWithTargetID.TargetID,
				messaging.MaintainerManagerTopic,
				heartBeatRequestWithTargetID.Request,
			))
		if err != nil {
			log.Error("failed to send heartbeat request message", zap.Error(err))
		}
	}
}

func (c *HeartBeatCollector) sendBlockStatusMessages() {
	for {
		blockStatusRequestWithTargetID := c.blockStatusReqQueue.Dequeue()
		err := c.mc.SendCommand(
			messaging.NewSingleTargetMessage(
				blockStatusRequestWithTargetID.TargetID,
				messaging.MaintainerManagerTopic,
				blockStatusRequestWithTargetID.Request,
			))
		if err != nil {
			log.Error("failed to send block status request message", zap.Error(err))
		}
	}
}

func (c *HeartBeatCollector) RecvMessages(_ context.Context, msg *messaging.TargetMessage) error {
	switch msg.Type {
	case messaging.TypeHeartBeatResponse:
		heartbeatResponse := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
		heartBeatResponseDynamicStream := GetHeartBeatResponseDynamicStream()
		heartBeatResponseDynamicStream.In() <- *NewHeartBeatResponse(heartbeatResponse)
	case messaging.TypeScheduleDispatcherRequest:
		schedulerDispatcherRequest := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
		c.schedulerDispatcherRequestDynamicStream.In() <- *NewSchedulerDispatcherRequest(schedulerDispatcherRequest)
		// TODO: check metrics
		metrics.HandleDispatcherRequsetCounter.WithLabelValues("default", schedulerDispatcherRequest.ChangefeedID, "receive").Inc()
	case messaging.TypeCheckpointTsMessage:
		checkpointTsMessage := msg.Message[0].(*heartbeatpb.CheckpointTsMessage)
		c.checkpointTsMessageDynamicStream.In() <- *NewCheckpointTsMessage(checkpointTsMessage)
	default:
		log.Panic("unknown message type", zap.Any("message", msg.Message))
	}
	return nil
}

func (c *HeartBeatCollector) Close() {
	c.mc.DeRegisterHandler(messaging.HeartbeatCollectorTopic)
}

type SchedulerDispatcherRequestHandler struct {
}

func (h *SchedulerDispatcherRequestHandler) Path(scheduleDispatcherRequest SchedulerDispatcherRequest) model.ChangeFeedID {
	return model.DefaultChangeFeedID(scheduleDispatcherRequest.ChangefeedID)
}

func (h *SchedulerDispatcherRequestHandler) Handle(eventDispatcherManager *EventDispatcherManager, reqs ...SchedulerDispatcherRequest) bool {
	if len(reqs) != 1 {
		// TODO: Support batch
		panic("invalid request count")
	}
	scheduleDispatcherRequest := reqs[0]
	if scheduleDispatcherRequest.ScheduleDispatcherRequest == nil {
		log.Warn("scheduleDispatcherRequest is nil, skip")
		return false
	}
	scheduleAction := scheduleDispatcherRequest.ScheduleAction
	config := scheduleDispatcherRequest.Config

	dispatcherID := common.NewDispatcherIDFromPB(config.DispatcherID)
	switch scheduleAction {
	case heartbeatpb.ScheduleAction_Create:
		eventDispatcherManager.NewDispatcher(dispatcherID, config.Span, config.StartTs, config.SchemaID)
	case heartbeatpb.ScheduleAction_Remove:
		eventDispatcherManager.RemoveDispatcher(dispatcherID)
	}
	return false
}

type HeartBeatResponseHandler struct {
	dispatcherStatusDynamicStream dynstream.DynamicStream[common.DispatcherID, dispatcher.DispatcherStatusWithID, *dispatcher.Dispatcher]
}

func NewHeartBeatResponseHandler() HeartBeatResponseHandler {
	return HeartBeatResponseHandler{dispatcherStatusDynamicStream: dispatcher.GetDispatcherStatusDynamicStream()}
}

func (h *HeartBeatResponseHandler) Path(HeartbeatResponse HeartBeatResponse) model.ChangeFeedID {
	return model.DefaultChangeFeedID(HeartbeatResponse.ChangefeedID)
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
				h.dispatcherStatusDynamicStream.In() <- dispatcher.NewDispatcherStatusWithID(dispatcherStatus, common.NewDispatcherIDFromPB(dispatcherID))
			}
		case heartbeatpb.InfluenceType_DB:
			schemaID := dispatcherStatus.InfluencedDispatchers.SchemaID
			excludeDispatcherID := common.NewDispatcherIDFromPB(dispatcherStatus.InfluencedDispatchers.ExcludeDispatcherId)
			dispatcherIds := eventDispatcherManager.GetAllDispatchers(schemaID)
			for _, id := range dispatcherIds {
				if id != excludeDispatcherID {
					h.dispatcherStatusDynamicStream.In() <- dispatcher.NewDispatcherStatusWithID(dispatcherStatus, id)
				}
			}
		case heartbeatpb.InfluenceType_All:
			excludeDispatcherID := common.NewDispatcherIDFromPB(dispatcherStatus.InfluencedDispatchers.ExcludeDispatcherId)
			eventDispatcherManager.GetDispatcherMap().ForEach(func(id common.DispatcherID, _ *dispatcher.Dispatcher) {
				if id != excludeDispatcherID {
					h.dispatcherStatusDynamicStream.In() <- dispatcher.NewDispatcherStatusWithID(dispatcherStatus, id)
				}
			})
		}
	}
	return false
}

type CheckpointTsMessageHandler struct{}

func NewCheckpointTsMessageHandler() CheckpointTsMessageHandler {
	return CheckpointTsMessageHandler{}
}

func (h *CheckpointTsMessageHandler) Path(checkpointTsMessage CheckpointTsMessage) model.ChangeFeedID {
	return model.DefaultChangeFeedID(checkpointTsMessage.ChangefeedID)
}

func (h *CheckpointTsMessageHandler) Handle(eventDispatcherManager *EventDispatcherManager, messages ...CheckpointTsMessage) bool {
	if len(messages) != 1 {
		// TODO: Support batch
		panic("invalid message count")
	}
	checkpointTsMessage := messages[0]
	if eventDispatcherManager.tableTriggerEventDispatcher != nil && eventDispatcherManager.sink.SinkType() != sink.MysqlSinkType {
		tableTriggerEventDispatcher := eventDispatcherManager.tableTriggerEventDispatcher
		tableTriggerEventDispatcher.HandleCheckpointTs(checkpointTsMessage.CheckpointTs)
	}
	return false
}
