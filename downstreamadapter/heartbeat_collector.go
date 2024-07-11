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

package downstreamadapter

import (
	"fmt"
	"sync"

	"github.com/flowbehappy/tigate/downstreamadapter/dispatchermanager"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/apperror"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/ngaut/log"
	"go.uber.org/zap"
)

const heartbeatResponseTopic = "HeartBeatResponse"
const heartbeatRequestTopic = "HeartBeatRequest"

/*
HeartBeatCollect is responsible for sending heartbeat requests and receiving heartbeat responses by messageCenter
*/
type HeartBeatCollector struct {
	messageCenter  messaging.MessageCenter
	wg             sync.WaitGroup
	target         messaging.ServerId
	reponseChanMap map[uint64]*dispatchermanager.HeartbeatResponseQueue
	requestQueue   *dispatchermanager.HeartbeatRequestQueue
}

func newHeartBeatCollector(messageCenter messaging.MessageCenter, serverId messaging.ServerId) *HeartBeatCollector {
	heartBeatCollector := HeartBeatCollector{
		messageCenter:  messageCenter,
		target:         serverId,
		requestQueue:   dispatchermanager.NewHeartbeatRequestQueue(),
		reponseChanMap: make(map[uint64]*dispatchermanager.HeartbeatResponseQueue),
	}
	heartBeatCollector.messageCenter.RegisterHandler(heartbeatResponseTopic, heartBeatCollector.RecvHeartBeatResponseMessages)
	heartBeatCollector.wg.Add(1)
	go heartBeatCollector.SendHeartBeatMessages()

	return &heartBeatCollector

}

func (c *HeartBeatCollector) RegisterEventDispatcherManager(m *dispatchermanager.EventDispatcherManager) error {
	m.HeartbeatRequestQueue = c.requestQueue
	c.reponseChanMap[m.Id] = m.HeartbeatResponseQueue

	return nil
}

func (c *HeartBeatCollector) SendHeartBeatMessages() {
	for {
		heartBeatRequest := c.requestQueue.Dequeue()
		err := c.messageCenter.SendEvent(&messaging.TargetMessage{
			To:      c.target,
			Topic:   heartbeatRequestTopic,
			Type:    messaging.TypeHeartBeatRequest,
			Message: heartBeatRequest,
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
	managerId := heartbeatResponse.EventDispatcherManagerID
	if queue, ok := c.reponseChanMap[managerId]; ok {
		queue.Enqueue(heartbeatResponse)
	}
	return nil
}

func (c *HeartBeatCollector) Close() {
	// todo
}
