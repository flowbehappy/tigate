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
	"fmt"
	"sync"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/apperror"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

const handleDispatcherRequestConcurrency = 16

type ResponseChanMap struct {
	responseChanMapMutex sync.RWMutex
	m                    map[model.ChangeFeedID]*HeartbeatResponseQueue //changefeedID -> HeartbeatResponseQueue
}

func NewResponseChanMap() *ResponseChanMap {
	return &ResponseChanMap{
		m: make(map[model.ChangeFeedID]*HeartbeatResponseQueue),
	}
}

func (responseChanMap *ResponseChanMap) Get(changeFeedID model.ChangeFeedID) (heartbeatResponseQueue *HeartbeatResponseQueue, ok bool) {
	responseChanMap.responseChanMapMutex.RLock()
	defer responseChanMap.responseChanMapMutex.RUnlock()
	heartbeatResponseQueue, ok = responseChanMap.m[changeFeedID]
	return
}

func (responseChanMap *ResponseChanMap) Set(changeFeedID model.ChangeFeedID, heartbeatResponseQueue *HeartbeatResponseQueue) {
	responseChanMap.responseChanMapMutex.Lock()
	defer responseChanMap.responseChanMapMutex.Unlock()

	responseChanMap.m[changeFeedID] = heartbeatResponseQueue
}

type EventDispatcherManagerMap struct {
	eventDispatcherManagerMutex sync.RWMutex
	eventDispatcherManagerMap   map[model.ChangeFeedID]*EventDispatcherManager // changefeedID -> EventDispatcherManager
}

func NewEventDispatcherManagerMap() *EventDispatcherManagerMap {
	return &EventDispatcherManagerMap{
		eventDispatcherManagerMap: make(map[model.ChangeFeedID]*EventDispatcherManager),
	}
}

func (eventDispatcherManagerMap *EventDispatcherManagerMap) Get(changeFeedID model.ChangeFeedID) (eventDispatcherManager *EventDispatcherManager, ok bool) {
	eventDispatcherManagerMap.eventDispatcherManagerMutex.RLock()
	defer eventDispatcherManagerMap.eventDispatcherManagerMutex.RUnlock()
	eventDispatcherManager, ok = eventDispatcherManagerMap.eventDispatcherManagerMap[changeFeedID]
	return
}

func (eventDispatcherManagerMap *EventDispatcherManagerMap) Set(changeFeedID model.ChangeFeedID, eventDispatcherManager *EventDispatcherManager) {
	eventDispatcherManagerMap.eventDispatcherManagerMutex.Lock()
	defer eventDispatcherManagerMap.eventDispatcherManagerMutex.Unlock()

	eventDispatcherManagerMap.eventDispatcherManagerMap[changeFeedID] = eventDispatcherManager
}

/*
HeartBeatCollect is responsible for sending heartbeat requests and receiving heartbeat responses by messageCenter
HeartBeatCollector is an instance-level component. It will deal with all the heartbeat messages from all dispatchers in all dispatcher managers.
*/
type HeartBeatCollector struct {
	wg   sync.WaitGroup
	from messaging.ServerId

	eventDispatcherManagerMap *EventDispatcherManagerMap
	responseChanMap           *ResponseChanMap

	requestQueue *HeartbeatRequestQueue

	dispatcherRequestCh []chan *heartbeatpb.ScheduleDispatcherRequest
}

func NewHeartBeatCollector(serverId messaging.ServerId) *HeartBeatCollector {
	heartBeatCollector := HeartBeatCollector{
		from:                      serverId,
		requestQueue:              NewHeartbeatRequestQueue(),
		responseChanMap:           NewResponseChanMap(),
		eventDispatcherManagerMap: NewEventDispatcherManagerMap(),
		dispatcherRequestCh:       make([]chan *heartbeatpb.ScheduleDispatcherRequest, handleDispatcherRequestConcurrency),
	}
	appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).RegisterHandler(messaging.HeartbeatCollectorTopic, heartBeatCollector.RecvHeartBeatResponseMessages)
	appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).
		RegisterHandler(messaging.HeartbeatCollectorTopic, heartBeatCollector.RecvSchedulerDispatcherRequestMessages)
	heartBeatCollector.wg.Add(1)
	go heartBeatCollector.SendHeartBeatMessages()

	for i := 0; i < handleDispatcherRequestConcurrency; i++ {
		idx := i
		heartBeatCollector.dispatcherRequestCh[idx] = make(chan *heartbeatpb.ScheduleDispatcherRequest, 1024)
		heartBeatCollector.wg.Add(1)
		go func() {
			defer heartBeatCollector.wg.Done()
			for req := range heartBeatCollector.dispatcherRequestCh[idx] {
				err := heartBeatCollector.handleDispatcherRequestMessages(req)
				if err != nil {
					metrics.HandleDispatcherRequsetCounter.WithLabelValues("default", req.ChangefeedID, "error").Inc()
				}
				metrics.HandleDispatcherRequsetCounter.WithLabelValues("default", req.ChangefeedID, "success").Inc()
			}
		}()
	}

	return &heartBeatCollector
}

func (c *HeartBeatCollector) RegisterEventDispatcherManager(m *EventDispatcherManager) error {
	m.SetHeartbeatRequestQueue(c.requestQueue)

	c.responseChanMap.Set(m.GetChangeFeedID(), m.GetHeartbeatResponseQueue())
	c.eventDispatcherManagerMap.Set(m.GetChangeFeedID(), m)

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

func (c *HeartBeatCollector) RecvHeartBeatResponseMessages(ctx context.Context, msg *messaging.TargetMessage) error {
	heartbeatResponse, ok := msg.Message.(*heartbeatpb.HeartBeatResponse)
	if !ok {
		log.Error("invalid heartbeat response message", zap.Any("msg", msg))
		return apperror.AppError{Type: apperror.ErrorTypeInvalidMessage, Reason: fmt.Sprintf("invalid heartbeat response message")}
	}
	changefeedID := model.DefaultChangeFeedID(heartbeatResponse.ChangefeedID)

	queue, ok := c.responseChanMap.Get(changefeedID)
	if ok {
		queue.Enqueue(heartbeatResponse)
	}
	return nil
}

func (c *HeartBeatCollector) RecvSchedulerDispatcherRequestMessages(ctx context.Context, msg *messaging.TargetMessage) error {
	scheduleDispatcherRequest := msg.Message.(*heartbeatpb.ScheduleDispatcherRequest)

	idx := int(scheduleDispatcherRequest.Config.Span.TableID) % handleDispatcherRequestConcurrency
	select {
	case c.dispatcherRequestCh[idx] <- scheduleDispatcherRequest:
		metrics.HandleDispatcherRequsetCounter.WithLabelValues("default", scheduleDispatcherRequest.ChangefeedID, "receive").Inc()
	default:
		metrics.HandleDispatcherRequsetCounter.WithLabelValues("default", scheduleDispatcherRequest.ChangefeedID, "discard").Inc()
	}
	return nil
}

func (c *HeartBeatCollector) handleDispatcherRequestMessages(req *heartbeatpb.ScheduleDispatcherRequest) error {
	// start := time.Now()
	changefeedID := model.DefaultChangeFeedID(req.ChangefeedID)

	eventDispatcherManager, ok := c.eventDispatcherManagerMap.Get(changefeedID)
	if !ok {
		// Maybe the message is received before the event dispatcher manager is registered, so just ingore it
		log.Warn("invalid changefeedID in scheduler dispatcher request message", zap.String("changefeedID", changefeedID.String()))
		return nil
	}
	scheduleAction := req.ScheduleAction
	config := req.Config
	if scheduleAction == heartbeatpb.ScheduleAction_Create {
		eventDispatcherManager.NewDispatcher(&common.TableSpan{TableSpan: config.Span}, config.StartTs)
	} else if scheduleAction == heartbeatpb.ScheduleAction_Remove {
		eventDispatcherManager.RemoveDispatcher(&common.TableSpan{TableSpan: config.Span})
	}

	// log.Info("RecvSchedulerDispatcherRequestMessages handle dispatch msg", zap.Any("tableSpan", config.Span),
	// zap.Int64("cost(ns)", time.Since(start).Nanoseconds()), zap.Time("start", start))
	return nil
}

func (c *HeartBeatCollector) Close() {
	appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).
		DeRegisterHandler(messaging.HeartbeatCollectorTopic)
}
