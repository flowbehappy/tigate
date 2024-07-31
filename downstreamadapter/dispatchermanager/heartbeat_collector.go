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

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
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

	eventDispatcherManagerMutex sync.RWMutex
	eventDispatcherManagerMap   map[model.ChangeFeedID]*EventDispatcherManager // changefeedID -> EventDispatcherManager

	responseChanMapMutex sync.RWMutex
	responseChanMap      map[model.ChangeFeedID]*HeartbeatResponseQueue //changefeedID -> HeartbeatResponseQueue
	requestQueue         *HeartbeatRequestQueue

	dispatcherRequestCh chan *heartbeatpb.ScheduleDispatcherRequest
}

func NewHeartBeatCollector(serverId messaging.ServerId) *HeartBeatCollector {
	heartBeatCollector := HeartBeatCollector{
		from:                      serverId,
		requestQueue:              NewHeartbeatRequestQueue(),
		responseChanMap:           make(map[model.ChangeFeedID]*HeartbeatResponseQueue),
		eventDispatcherManagerMap: make(map[model.ChangeFeedID]*EventDispatcherManager),
		dispatcherRequestCh:       make(chan *heartbeatpb.ScheduleDispatcherRequest, 102400),
	}
	//context.GetService[messaging.MessageCenter](context.MessageCenter).RegisterHandler(heartbeatResponseTopic, heartBeatCollector.RecvHeartBeatResponseMessages)
	appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).
		RegisterHandler(messaging.SchedulerDispatcherTopic, heartBeatCollector.RecvSchedulerDispatcherRequestMessages)
	heartBeatCollector.wg.Add(1)
	go heartBeatCollector.SendHeartBeatMessages()

	for i := 0; i < 1; i++ {
		heartBeatCollector.wg.Add(1)
		go func() {
			defer heartBeatCollector.wg.Done()
			for {
				select {
				case req := <-heartBeatCollector.dispatcherRequestCh:
					err := heartBeatCollector.handleDispatcherRequestMessages(req)
					if err != nil {
						metrics.HandleDispatcherRequsetCounter.WithLabelValues("default", req.ChangefeedID, "error").Inc()
					}
					metrics.HandleDispatcherRequsetCounter.WithLabelValues("default", req.ChangefeedID, "success").Inc()
				}
			}
		}()
	}

	return &heartBeatCollector
}

func (c *HeartBeatCollector) RegisterEventDispatcherManager(m *EventDispatcherManager) error {
	m.SetHeartbeatRequestQueue(c.requestQueue)

	c.eventDispatcherManagerMutex.Lock()
	c.responseChanMapMutex.Lock()

	defer c.eventDispatcherManagerMutex.Unlock()
	defer c.responseChanMapMutex.Unlock()

	//c.reponseChanMap[m.GetChangeFeedID()] = m.HeartbeatResponseQueue
	c.eventDispatcherManagerMap[m.GetChangeFeedID()] = m

	return nil
}

func (c *HeartBeatCollector) SendHeartBeatMessages() {
	for {
		heartBeatRequestWithTargetID := c.requestQueue.Dequeue()
		err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendEvent(&messaging.TargetMessage{
			To:      heartBeatRequestWithTargetID.TargetID,
			Topic:   messaging.DispatcherHeartBeatRequestTopic,
			Type:    messaging.TypeHeartBeatRequest,
			Message: heartBeatRequestWithTargetID.Request,
		})
		if err != nil {
			log.Error("failed to send heartbeat request message", zap.Error(err))
		}
	}
}

/*
func (c *HeartBeatCollector) RecvHeartBeatResponseMessages(msg *messaging.TargetMessage) error {
	heartbeatResponse, ok := msg.Message.(*heartbeatpb.HeartBeatResponse)
	if !ok {
		log.Error("invalid heartbeat response message", zap.Any("msg", msg))
		return apperror.AppError{Type: apperror.ErrorTypeInvalidMessage, Reason: fmt.Sprintf("invalid heartbeat response message")}
	}
	changefeedID := model.DefaultChangeFeedID(heartbeatResponse.ChangefeedID)

	c.responseChanMapMutex.RLock()
	defer c.responseChanMapMutex.RUnlock()
	if queue, ok := c.responseChanMap[changefeedID]; ok {
		queue.Enqueue(heartbeatResponse)
	}
	return nil
}*/

func (c *HeartBeatCollector) RecvSchedulerDispatcherRequestMessages(ctx context.Context, msg *messaging.TargetMessage) error {
	scheduleDispatcherRequest := msg.Message.(*heartbeatpb.ScheduleDispatcherRequest)

	select {
	case c.dispatcherRequestCh <- scheduleDispatcherRequest:
		metrics.HandleDispatcherRequsetCounter.WithLabelValues("default", scheduleDispatcherRequest.ChangefeedID, "receive").Inc()
	default:
		metrics.HandleDispatcherRequsetCounter.WithLabelValues("default", scheduleDispatcherRequest.ChangefeedID, "discard").Inc()
	}
	return nil
}

func (c *HeartBeatCollector) handleDispatcherRequestMessages(req *heartbeatpb.ScheduleDispatcherRequest) error {
	// start := time.Now()
	changefeedID := model.DefaultChangeFeedID(req.ChangefeedID)

	c.eventDispatcherManagerMutex.RLock()
	defer c.eventDispatcherManagerMutex.RUnlock()

	eventDispatcherManager, ok := c.eventDispatcherManagerMap[changefeedID]
	if !ok {
		// Maybe the message is received before the event dispatcher manager is registered, so just ingore it
		log.Warn("invalid changefeedID in scheduler dispatcher request message", zap.String("changefeedID", changefeedID.String()))
		return nil
	}
	scheduleAction := req.ScheduleAction
	config := req.Config
	if scheduleAction == heartbeatpb.ScheduleAction_Create {
		// TODO: 后续需要优化这段逻辑，perpared 这种调度状态需要多发 message 回去
		if !req.IsSecondary {
			eventDispatcherManager.NewTableEventDispatcher(&common.TableSpan{TableSpan: config.Span}, config.StartTs)
		} else {
			// eventDispatcherManager.GetTableSpanStatusesChan() <- &heartbeatpb.TableSpanStatus{
			// 	Span:            config.Span,
			// 	ComponentStatus: heartbeatpb.ComponentState_Prepared,
			// }
		}
	} else if scheduleAction == heartbeatpb.ScheduleAction_Remove {
		eventDispatcherManager.RemoveTableEventDispatcher(&common.TableSpan{TableSpan: config.Span})
	}

	// log.Info("RecvSchedulerDispatcherRequestMessages handle dispatch msg", zap.Any("tableSpan", config.Span),
	// zap.Int64("cost(ns)", time.Since(start).Nanoseconds()), zap.Time("start", start))
	return nil
}

func (c *HeartBeatCollector) Close() {
	// todo
}
