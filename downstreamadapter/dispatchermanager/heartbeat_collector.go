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

	requestQueue *HeartbeatRequestQueue

	dispatcherRequestCh []chan *heartbeatpb.ScheduleDispatcherRequest

	heartBeatResponseDynamicStream          dynstream.DynamicStream[model.ChangeFeedID, *heartbeatpb.HeartBeatResponse, *EventDispatcherManager]
	schedulerDispatcherRequestDynamicStream dynstream.DynamicStream[model.ChangeFeedID, *heartbeatpb.ScheduleDispatcherRequest, *EventDispatcherManager]
}

func NewHeartBeatCollector(serverId messaging.ServerId) *HeartBeatCollector {
	heartBeatCollector := HeartBeatCollector{
		from:                                    serverId,
		requestQueue:                            NewHeartbeatRequestQueue(),
		heartBeatResponseDynamicStream:          appcontext.GetService[dynstream.DynamicStream[model.ChangeFeedID, *heartbeatpb.HeartBeatResponse, *EventDispatcherManager]](appcontext.HeartBeatResponseDynamicStream),
		schedulerDispatcherRequestDynamicStream: appcontext.GetService[dynstream.DynamicStream[model.ChangeFeedID, *heartbeatpb.ScheduleDispatcherRequest, *EventDispatcherManager]](appcontext.SchedulerDispatcherRequestDynamicStream),
	}
	appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).RegisterHandler(messaging.HeartbeatCollectorTopic, heartBeatCollector.RecvMessages)

	heartBeatCollector.wg.Add(1)
	go heartBeatCollector.SendHeartBeatMessages()

	return &heartBeatCollector
}

func (c *HeartBeatCollector) RegisterEventDispatcherManager(m *EventDispatcherManager) error {
	m.SetHeartbeatRequestQueue(c.requestQueue)
	c.heartBeatResponseDynamicStream.AddPath(dynstream.PathAndDest[model.ChangeFeedID, *EventDispatcherManager]{Path: m.changefeedID, Dest: m})
	c.schedulerDispatcherRequestDynamicStream.AddPath(dynstream.PathAndDest[model.ChangeFeedID, *EventDispatcherManager]{Path: m.changefeedID, Dest: m})
	return nil
}

func (c *HeartBeatCollector) SendHeartBeatMessages() {
	for {
		heartBeatRequestWithTargetID := c.requestQueue.Dequeue()
		err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendEvent(&messaging.TargetMessage{
			To:      heartBeatRequestWithTargetID.TargetID,
			Topic:   messaging.MaintainerManagerTopic,
			Type:    messaging.TypeHeartBeatRequest,
			Message: heartBeatRequestWithTargetID.Request,
		})
		if err != nil {
			log.Error("failed to send heartbeat request message", zap.Error(err))
		}
	}
}

func (c *HeartBeatCollector) RecvMessages(ctx context.Context, msg *messaging.TargetMessage) error {
	switch msg.Type {
	case messaging.TypeHeartBeatResponse:
		heartbeatResponse := msg.Message.(*heartbeatpb.HeartBeatResponse)
		heartBeatResponseDynamicStream := appcontext.GetService[dynstream.DynamicStream[model.ChangeFeedID, *heartbeatpb.HeartBeatResponse, *EventDispatcherManager]](appcontext.HeartBeatResponseDynamicStream)
		heartBeatResponseDynamicStream.In() <- heartbeatResponse
	case messaging.TypeScheduleDispatcherRequest:
		scheduleDispatcherRequest := msg.Message.(*heartbeatpb.ScheduleDispatcherRequest)
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
	scheduleAction := scheduleDispatcherRequest.ScheduleAction
	config := scheduleDispatcherRequest.Config
	if scheduleAction == heartbeatpb.ScheduleAction_Create {
		eventDispatcherManager.NewDispatcher(&common.TableSpan{TableSpan: config.Span}, config.StartTs)
	} else if scheduleAction == heartbeatpb.ScheduleAction_Remove {
		eventDispatcherManager.RemoveDispatcher(&common.TableSpan{TableSpan: config.Span})
	}
	return false
}

type HeartBeatResponseHandler struct {
	dispatcherStatusDynamicStream dynstream.DynamicStream[common.DispatcherID, dispatcher.DispatcherStatusWithDispatcherID, *dispatcher.Dispatcher]
}

func NewHeartBeatResponseHandler(dispatcherStatusDynamicStream dynstream.DynamicStream[common.DispatcherID, dispatcher.DispatcherStatusWithDispatcherID, *dispatcher.Dispatcher]) HeartBeatResponseHandler {
	return HeartBeatResponseHandler{dispatcherStatusDynamicStream: dispatcherStatusDynamicStream}
}

func (h *HeartBeatResponseHandler) Path(HeartbeatResponse *heartbeatpb.HeartBeatResponse) model.ChangeFeedID {
	return model.DefaultChangeFeedID(HeartbeatResponse.ChangefeedID)
}

func (h *HeartBeatResponseHandler) Handle(heartbeatResponse *heartbeatpb.HeartBeatResponse, eventDispatcherManager *EventDispatcherManager) bool {
	dispatcherStatuses := heartbeatResponse.GetDispatcherStatuses()
	for _, dispatcherStatus := range dispatcherStatuses {
		tableSpan := dispatcherStatus.Span
		dispatcherItem, ok := eventDispatcherManager.dispatcherMap.Get(&common.TableSpan{TableSpan: tableSpan})
		if !ok {
			log.Error("dispatcher not found", zap.Any("tableSpan", tableSpan))
			continue
		}

		h.dispatcherStatusDynamicStream.In() <- *dispatcher.NewDispatcherStatusWithDispatcherID(dispatcherStatus, dispatcherItem.GetId())
	}

	return false
}
