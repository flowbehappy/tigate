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
	"new_arch/utils/conn"
	"sync"
)

// 单独的 goroutine 用于收发 heartbeat 消息
type HeartBeatCollector struct {
	grpcPool                  *conn.ConnAndClientPool          // pool 要重写过
	clients                   map[*conn.ConnAndClient][]uint64 // client --> eventdispatchermanagerIDList
	clientMaxDispatcherNumber uint64
	addr                      string
	wg                        *sync.WaitGroup
	// channel 的函数
}

func (c *HeartBeatCollector) RegisterEventDispatcherManager(m *EventDispatcherManager) error {
	flag := false
	for client, managerIDLists := range c.clients { // 这边先用个遍历，后面看看有什么更合适的结构
		if len(managerIDLists) < c.clientMaxDispatcherNumber {
			c.clients[client] = append(c.clients[client], m.ID)
			flag = true
			break
		}
	}

	if !flag {
		newClient, _ := c.grpcPool.Connect(c.addr)
		c.clients[newClient] = append(c.clients[newClient], m.ID)
		c.wg.Add(1)
		c.wg.Add(1)
		go c.sendMessages(newClient)
		go c.RecvMessages(newClient)
	}
}

func (c *HeartBeatCollector) sendMessages(cc *conn.ConnAndClient) {
	client := cc.client
	for {
		select {
		case message := <-c.messageChan:
			// 发出去
		}
	}
}

func (c *HeartBeatCollector) sendMessages(cc *conn.ConnAndClient) {
	client := cc.client
	for {
		heartbeatResponse, err := client.Recv() // 分发 heartbeat response
		// 分发 heartbeat response
	}
}
