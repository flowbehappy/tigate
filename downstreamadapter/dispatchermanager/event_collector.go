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
	"new_arch/downstreamadapter/dispatcher"
	"new_arch/utils/conn"

	"github.com/ngaut/log"
	"github.com/pingcap/tiflow/pkg/security"
)

// 负责从 logService 中拉取所有 event，解码分发给各个 dispatcher
type EventCollector struct {
	grpcPool                  *conn.ConnAndClientPool      // 用于获取 client
	masterClient              *conn.ConnAndTableAddrClient // 专门用于跟 log master 通信
	clientMaxDispatcherNumber int
	clients                   map[*conn.ConnAndClient][]uint64           // client --> dispatcherIDList
	addrMap                   map[string][]*conn.ConnAndClient           // addr --> client
	dispatcherMap             map[uint64]dispatcher.TableEventDispatcher // dispatcher_id --> dispatcher
}

func newEventCollector(masterAddr string) *EventCollector {
	eventCollector := EventCollector{
		grpcPool: conn.NewConnAndClientPool(&security.Credential{}, 100), // todo:
	}
	eventCollector.masterClient, _ = eventCollector.grpcPool.NewConnAndTableAddrClient(masterAddr)
	return &eventCollector
}

func (c *EventCollector) RegisterDispatcher(dispatcher *dispatcher.TableEventDispatcher) error {
	addr := c.getAddr(dispatcher)

	clients := c.addrMap[addr] // 获得目前现有的所有跟这个 addr 相关的 clients

	flag := false // 标记是否选了合适的 client

	for _, client := range clients { // 这边先用个遍历，后面看看有什么更合适的结构
		dispatcherIDLists := c.clients[client]
		if len(dispatcherIDLists) < c.clientMaxDispatcherNumber {
			c.clients[client] = append(c.clients[client], dispatcher.ID)
			flag = true
			break
		}
	}
	if !flag {
		newClient, _ := c.grpcPool.Connect(addr)
		c.addrMap[addr] = append(c.addrMap[addr], newClient)
		c.clients[newClient] = append(c.clients[newClient], dispatcher.ID)
		go c.run(newClient)
	}
}

func (c *EventCollector) run(cc *conn.ConnAndClient) {
	client := cc.client
	for {
		eventResponse, err := client.Recv() // 这边收到 event 要分发掉 -- decode 的说的是这个自带的么
		if err != nil {
			// 抛出错误？
		}
		dispatcherId := eventResponse.DispatcherId

		if dispatcher, ok := c.dispatcherMap[dispatcherId]; ok {
			dispatcher.ResolvedTs = eventResponse.ResolvedTs // todo:枷锁
			for _, event := range eventResponse.Events {
				// deal with event
				dispatcher.Ch <- event // 换成一个函数
			}
		}
		//
	}
}

// 这个要想一下，做成单线程，还是收发拆开
func (c *EventCollector) getAddr(dispatcher *dispatcher.TableEventDispatcher) string {
	client := c.masterClient.client
	// make the msg
	err := client.Send(msg)
	if err != nil {
		// 抛出错误
	}

	for {
		addrResponse, err := client.Recv() // 这个会直接阻塞么？
		dispatcherId := addrResponse.DispatcherId
		if dispatcherId != dispatcher.ID {
			log.Error("wrong dispatcher")
			continue
		}
		addr := addrResponse.Addr
		return addr
	}

}
