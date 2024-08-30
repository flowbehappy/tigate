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

	"github.com/flowbehappy/tigate/downstreamadapter/dispatcher"
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
	from messaging.ServerId

	reqQueue *HeartbeatRequestQueue

	heartBeatResponseDynamicStream          dynstream.DynamicStream[model.ChangeFeedID, *heartbeatpb.HeartBeatResponse, *EventDispatcherManager]
	schedulerDispatcherRequestDynamicStream dynstream.DynamicStream[model.ChangeFeedID, *heartbeatpb.ScheduleDispatcherRequest, *EventDispatcherManager]
}

func NewHeartBeatCollector(serverId messaging.ServerId) *HeartBeatCollector {
	heartBeatCollector := HeartBeatCollector{
		from:                                    serverId,
		reqQueue:                                NewHeartbeatRequestQueue(),
		heartBeatResponseDynamicStream:          GetHeartBeatResponseDynamicStream(),
		schedulerDispatcherRequestDynamicStream: GetSchedulerDispatcherRequestDynamicStream(),
	}
	appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).RegisterHandler(messaging.HeartbeatCollectorTopic, heartBeatCollector.RecvMessages)

	heartBeatCollector.wg.Add(1)
	go heartBeatCollector.SendHeartBeatMessages()

	return &heartBeatCollector
}

func (c *HeartBeatCollector) RegisterEventDispatcherManager(m *EventDispatcherManager) error {
	m.SetHeartbeatRequestQueue(c.reqQueue)
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

func (c *HeartBeatCollector) SendHeartBeatMessages() {
	defer c.wg.Done()
	for {
		heartBeatRequestWithTargetID := c.reqQueue.Dequeue()
		err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendCommand(
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

func (c *HeartBeatCollector) RecvMessages(ctx context.Context, msg *messaging.TargetMessage) error {
	switch msg.Type {
	case messaging.TypeHeartBeatResponse:
		heartbeatResponse := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
		heartBeatResponseDynamicStream := GetHeartBeatResponseDynamicStream()
		heartBeatResponseDynamicStream.In() <- heartbeatResponse
	case messaging.TypeScheduleDispatcherRequest:
		scheduleDispatcherRequest := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
		c.schedulerDispatcherRequestDynamicStream.In() <- scheduleDispatcherRequest
		// TODO: check metrics
		metrics.HandleDispatcherRequsetCounter.WithLabelValues("default", scheduleDispatcherRequest.ChangefeedID, "receive").Inc()
	default:
		log.Panic("unknown message type", zap.Any("message", msg.Message))
	}
	return nil
}

func (c *HeartBeatCollector) Close() {
	appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).
		DeRegisterHandler(messaging.HeartbeatCollectorTopic)
}

type SchedulerDispatcherRequestHandler struct {
}

func (h *SchedulerDispatcherRequestHandler) Path(scheduleDispatcherRequest *heartbeatpb.ScheduleDispatcherRequest) model.ChangeFeedID {
	return model.DefaultChangeFeedID(scheduleDispatcherRequest.ChangefeedID)
}

func (h *SchedulerDispatcherRequestHandler) Handle(scheduleDispatcherRequest *heartbeatpb.ScheduleDispatcherRequest, eventDispatcherManager *EventDispatcherManager) bool {
	if scheduleDispatcherRequest == nil {
		log.Warn("scheduleDispatcherRequest is nil, skip")
		return false
	}
	scheduleAction := scheduleDispatcherRequest.ScheduleAction
	config := scheduleDispatcherRequest.Config
	if scheduleAction == heartbeatpb.ScheduleAction_Create {
		eventDispatcherManager.NewDispatcher(common.NewDispatcherIDFromPB(config.DispatcherID), &common.TableSpan{TableSpan: config.Span}, config.StartTs)
	} else if scheduleAction == heartbeatpb.ScheduleAction_Remove {
		eventDispatcherManager.RemoveDispatcher(common.NewDispatcherIDFromPB(config.DispatcherID))
	}
	return false
}

type HeartBeatResponseHandler struct {
	dispatcherStatusDynamicStream dynstream.DynamicStream[common.DispatcherID, dispatcher.DispatcherStatusWithID, *dispatcher.Dispatcher]
}

func NewHeartBeatResponseHandler() HeartBeatResponseHandler {
	return HeartBeatResponseHandler{dispatcherStatusDynamicStream: dispatcher.GetDispatcherStatusDynamicStream()}
}

func (h *HeartBeatResponseHandler) Path(HeartbeatResponse *heartbeatpb.HeartBeatResponse) model.ChangeFeedID {
	return model.DefaultChangeFeedID(HeartbeatResponse.ChangefeedID)
}

func (h *HeartBeatResponseHandler) Handle(heartbeatResponse *heartbeatpb.HeartBeatResponse, eventDispatcherManager *EventDispatcherManager) bool {
	dispatcherStatuses := heartbeatResponse.GetDispatcherStatuses()
	for _, dispatcherStatus := range dispatcherStatuses {
		influencedDispatchersType := dispatcherStatus.InfluencedDispatchers.InfluenceType
		if influencedDispatchersType == heartbeatpb.InfluenceType_Normal {
			for _, dispatcherID := range dispatcherStatus.InfluencedDispatchers.DispatcherIDs {
				h.dispatcherStatusDynamicStream.In() <- dispatcher.NewDispatcherStatusWithID(dispatcherStatus, common.NewDispatcherIDFromPB(dispatcherID))
			}
		} else if influencedDispatchersType == heartbeatpb.InfluenceType_DB {
			// 找出 db 对应的所有 id 扔进去, 记得查看 exclude_dispatcher_id
		} else if influencedDispatchersType == heartbeatpb.InfluenceType_All {
			// 遍历所有 dispatcher 扔进去, 记得查看 exclude_dispatcher_id
		}
	}

	return false
}
