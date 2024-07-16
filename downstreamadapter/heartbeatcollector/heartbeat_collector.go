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

package heartbeatcollector

import (
	"fmt"
	"sync"

	"github.com/flowbehappy/tigate/coordinator"
	"github.com/flowbehappy/tigate/downstreamadapter/dispatchermanager"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/apperror"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/ngaut/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

const heartbeatResponseTopic = "HeartBeatResponse"
const heartbeatRequestTopic = "HeartBeatRequest"
const schedulerDispatcherTopic = "SchedulerDispatcherRequest"

/*
HeartBeatCollect is responsible for sending heartbeat requests and receiving heartbeat responses by messageCenter
HeartBeatCollector is an instance-level component. It will deal with all the heartbeat messages from all dispatchers in all dispatcher managers.
*/
type HeartBeatCollector struct {
	wg     sync.WaitGroup
	from   messaging.ServerId
	target messaging.ServerId

	eventDispatcherManagerMutex sync.RWMutex
	eventDispatcherManagerMap   map[model.ChangeFeedID]*dispatchermanager.EventDispatcherManager // changefeedID -> EventDispatcherManager

	responseChanMapMutex sync.RWMutex
	reponseChanMap       map[model.ChangeFeedID]*dispatchermanager.HeartbeatResponseQueue //changefeedID -> HeartbeatResponseQueue
	requestQueue         *dispatchermanager.HeartbeatRequestQueue
}

func NewHeartBeatCollector(serverId messaging.ServerId) *HeartBeatCollector {
	heartBeatCollector := HeartBeatCollector{
		from:           serverId,
		requestQueue:   dispatchermanager.NewHeartbeatRequestQueue(),
		reponseChanMap: make(map[model.ChangeFeedID]*dispatchermanager.HeartbeatResponseQueue),
	}
	context.GetService[messaging.MessageCenter](context.MessageCenter).RegisterHandler(heartbeatResponseTopic, heartBeatCollector.RecvHeartBeatResponseMessages)
	context.GetService[messaging.MessageCenter](context.MessageCenter).RegisterHandler(schedulerDispatcherTopic, heartBeatCollector.RecvSchedulerDispatcherRequestMessages)
	heartBeatCollector.wg.Add(1)
	go heartBeatCollector.SendHeartBeatMessages()

	return &heartBeatCollector

}

func (c *HeartBeatCollector) RegisterEventDispatcherManager(m *dispatchermanager.EventDispatcherManager) error {
	m.HeartbeatRequestQueue = c.requestQueue

	c.eventDispatcherManagerMutex.Lock()
	c.responseChanMapMutex.Lock()

	defer c.eventDispatcherManagerMutex.Unlock()
	defer c.responseChanMapMutex.Unlock()

	c.reponseChanMap[m.ChangefeedID] = m.HeartbeatResponseQueue
	c.eventDispatcherManagerMap[m.ChangefeedID] = m

	return nil
}

func (c *HeartBeatCollector) SendHeartBeatMessages() {
	for {
		heartBeatRequestWithTargetID := c.requestQueue.Dequeue()
		err := context.GetService[messaging.MessageCenter](context.MessageCenter).SendEvent(&messaging.TargetMessage{
			To:      heartBeatRequestWithTargetID.TargetID,
			Topic:   heartbeatRequestTopic,
			Type:    messaging.TypeHeartBeatRequest,
			Message: heartBeatRequestWithTargetID.Request,
		})
		if err != nil {
			log.Error("failed to send heartbeat request message", zap.Error(err))
		}
	}
}

func (c *HeartBeatCollector) RecvHeartBeatResponseMessages(msg *messaging.TargetMessage) error {
	heartbeatResponse, ok := msg.Message.(*heartbeatpb.HeartBeatResponse)
	if !ok {
		log.Error("invalid heartbeat response message", zap.Any("msg", msg))
		return apperror.AppError{Type: apperror.ErrorTypeInvalidMessage, Reason: fmt.Sprintf("invalid heartbeat response message")}
	}
	changefeedID := model.DefaultChangeFeedID(heartbeatResponse.ChangefeedID)

	c.responseChanMapMutex.RLock()
	defer c.responseChanMapMutex.RUnlock()
	if queue, ok := c.reponseChanMap[changefeedID]; ok {
		queue.Enqueue(heartbeatResponse)
	}
	return nil
}

func (c *HeartBeatCollector) RecvSchedulerDispatcherRequestMessages(msg *messaging.TargetMessage) error {
	scheduleDispatcherRequest := msg.Message.(*heartbeatpb.ScheduleDispatcherRequest)
	changefeedID := model.DefaultChangeFeedID(scheduleDispatcherRequest.ChangefeedID)

	c.eventDispatcherManagerMutex.RLock()
	defer c.eventDispatcherManagerMutex.RUnlock()

	eventDispatcherManager, ok := c.eventDispatcherManagerMap[changefeedID]
	if !ok {
		// Maybe the message is received before the event dispatcher manager is registered, so just ingore it
		log.Warn("invalid changefeedID in scheduler dispatcher request message", zap.Any("changefeedID", changefeedID))
		return nil
	}
	scheduleAction := scheduleDispatcherRequest.ScheduleAction
	config := scheduleDispatcherRequest.Config
	if scheduleAction == heartbeatpb.ScheduleAction_Create {
		if scheduleDispatcherRequest.IsSecondary {
			eventDispatcherManager.NewTableEventDispatcher(&common.TableSpan{TableSpan: config.Span}, config.StartTs)
		} else {
			eventDispatcherManager.TableSpanStatusesChan <- &heartbeatpb.TableSpanStatus{
				Span:            config.Span,
				ComponentStatus: int32(coordinator.ComponentStatusPrepared),
			}
			// 提前触发任务
		}
	} else if scheduleAction == heartbeatpb.ScheduleAction_Remove {
		eventDispatcherManager.RemoveTableEventDispatcher(&common.TableSpan{TableSpan: config.Span})
	}
	return nil
}

func (c *HeartBeatCollector) Close() {
	// todo
}
