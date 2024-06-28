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

package node

import (
	"sync"

	"github.com/flowbehappy/tigate/downstreamadapter/dispatchermanager"
	"github.com/flowbehappy/tigate/utils/conn"

	"github.com/pingcap/tiflow/pkg/security"
)

/*
HeartBeatCollect is responsible for sending heartbeat requests and receiving heartbeat responses through grpc.
Multiple event dispatcher managers share the same grpc client to reuse connections.
Each grpc client corresponses 2 goroutines, one for sending heartbeat requests and the other for receiving heartbeat responses.
*/
type HeartBeatCollector struct {
	grpcPool                         *conn.HeartbeatConnAndClientPool          // pool 要重写过
	clients                          map[*conn.HeartbeatConnAndClient][]uint64 // client --> eventdispatchermanagerIDList
	clientMaxDispatcherManagerNumber int
	addr                             string // 可能要支持多个 addr
	wg                               *sync.WaitGroup
	reponseChanMap                   map[uint64]*dispatchermanager.HeartbeatResponseQueue
	requestQueue                     *dispatchermanager.HeartbeatRequestQueue
}

func newHeartBeatCollector(maintainerAddr string, clientMaxDispatcherManagerNumber int) *HeartBeatCollector {
	return &HeartBeatCollector{
		addr:                             maintainerAddr,
		clientMaxDispatcherManagerNumber: clientMaxDispatcherManagerNumber,
		grpcPool:                         conn.NewHeartbeatConnAndClientPool(&security.Credential{}, 1000),
		requestQueue:                     dispatchermanager.NewHeartbeatRequestQueue(),
		reponseChanMap:                   make(map[uint64]*dispatchermanager.HeartbeatResponseQueue),
	}
}

func (c *HeartBeatCollector) RegisterEventDispatcherManager(m *dispatchermanager.EventDispatcherManager) error {
	m.HeartbeatRequestQueue = c.requestQueue
	c.reponseChanMap[m.Id] = m.HeartbeatResponseQueue

	flag := false
	for client, managerIDLists := range c.clients { // 这边先用个遍历，后面看看有什么更合适的结构
		if len(managerIDLists) < c.clientMaxDispatcherManagerNumber {
			c.clients[client] = append(c.clients[client], m.Id)
			flag = true
			break
		}
	}

	if !flag {
		newClient, _ := c.grpcPool.Connect(c.addr)
		c.clients[newClient] = append(c.clients[newClient], m.Id)
		c.wg.Add(1)
		c.wg.Add(1)
		go c.SendMessages(newClient)
		go c.RecvMessages(newClient)
	}

	return nil
}

func (c *HeartBeatCollector) SendMessages(cc *conn.HeartbeatConnAndClient) {
	client := cc.Client
	for {
		request := c.requestQueue.Dequeue()
		// 发出去
		client.Send(request)
	}
}

func (c *HeartBeatCollector) RecvMessages(cc *conn.HeartbeatConnAndClient) {
	client := cc.Client
	for {
		heartbeatResponse, err := client.Recv() // 分发 heartbeat response
		managerId := heartbeatResponse.EventDispatcherManagerID
		if queue, ok := c.reponseChanMap[managerId]; ok {
			queue.Enqueue(heartbeatResponse)
		}
	}
}
