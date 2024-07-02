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

	"github.com/flowbehappy/tigate/downstreamadapter/dispatcher"
	"github.com/flowbehappy/tigate/eventpb"
	"github.com/flowbehappy/tigate/utils/conn"

	"github.com/ngaut/log"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/tikv/client-go/v2/oracle"
)

/*
EventCollector is responsible for collecting the events from log service and dispatching them to different dispatchers.
Multiple dispatchers can share one grpc client. Each grpc client corresponds to one goroutine for continuously receiving events.
Besides, EventCollector also generate SyncPoint Event for dispatchers when necessary.
*/
type EventCollector struct {
	grpcPool                  *conn.EventFeedConnAndClientPool // 用于获取 client
	masterClient              *conn.TableAddrConnAndClient     // 专门用于跟 log master 通信
	clientMaxDispatcherNumber int
	clients                   map[*conn.EventFeedConnAndClient][]uint64 // client --> dispatcherIDList
	addrMap                   map[string][]*conn.EventFeedConnAndClient // addr --> client
	dispatcherMap             map[uint64]dispatcher.Dispatcher          // dispatcher_id --> dispatcher
	dispatcherClientMap       map[uint64]*conn.EventFeedConnAndClient   // dispatcher_id --> client
	wg                        *sync.WaitGroup
}

func newEventCollector(masterAddr string) *EventCollector {
	eventCollector := EventCollector{
		grpcPool: conn.NewEventFeedConnAndClientPool(&security.Credential{}, 100), // todo:
	}
	eventCollector.masterClient, _ = conn.NewConnAndTableAddrClient(masterAddr, &security.Credential{})
	return &eventCollector
}

func (c *EventCollector) RegisterDispatcher(d dispatcher.Dispatcher, startTs uint64, filter *Filter) error {
	id := d.GetId()
	addr := c.getAddr(d)

	clients := c.addrMap[addr] // 获得目前现有的所有跟这个 addr 相关的 clients

	flag := false // 标记是否选了合适的 client

	for _, client := range clients { // 这边先用个遍历，后面看看有什么更合适的结构
		dispatcherIDLists := c.clients[client]
		if len(dispatcherIDLists) < c.clientMaxDispatcherNumber {
			c.clients[client] = append(c.clients[client], id)
			c.dispatcherClientMap[id] = client
			flag = true
			break
		}
	}
	if !flag {
		newClient, _ := c.grpcPool.Connect(addr)
		c.addrMap[addr] = append(c.addrMap[addr], newClient)
		c.clients[newClient] = append(c.clients[newClient], id)
		c.dispatcherClientMap[id] = newClient

		c.wg.Add(1)
		go c.run(newClient)
	}

	// 要加一个 发送对应注册信息的 request
	request := eventpb.EventRequest{
		DispatcherId: id,
		StartTs:      startTs,
		TableSpan:    d.GetTableSpan(),
		Filter:       filter,
		Ratio:        1,
		Remove:       false,
	}

	err := c.dispatcherClientMap[id].Client.Send(&request)
	if err != nil {
		//
	}

	c.dispatcherMap[d.GetId()] = d

	return nil
}

func (c *EventCollector) run(cc *conn.EventFeedConnAndClient) {
	client := cc.Client
	for {
		eventResponse, err := client.Recv() // 这边收到 event 要分发掉 -- decode 的说的是这个自带的么
		if err != nil {
			// 抛出错误？
		}
		dispatcherId := eventResponse.DispatcherId

		if dispatcherItem, ok := c.dispatcherMap[dispatcherId]; ok {
			// check whether need to update speed ratio
			ok, ratio := dispatcherItem.GetMemoryUsage().UpdatedSpeedRatio(eventResponse.Ratio)
			if ok {
				request := eventpb.EventRequest{
					DispatcherId: dispatcherId,
					TableSpan:    dispatcherItem.GetTableSpan(),
					Ratio:        ratio,
					Remove:       false,
				}
				// 这个开销大么，在这里等合适么？看看要不要拆一下
				err := client.Send(&request)
				if err != nil {
					//
				}
			}

			if dispatcherId == dispatcher.TableTriggerEventDispatcherId {
				for _, event := range eventResponse.Events {
					dispatcherItem.GetMemoryUsage().Add(event.CommitTs(), event.MemoryCost())
					dispatcherItem.GetEventChan() <- event // 换成一个函数
				}
				dispatcherItem.UpdateResolvedTs(eventResponse.ResolvedTs) // todo:枷锁
				continue
			}

			for _, event := range eventResponse.Events {
				syncPointInfo := dispatcherItem.GetSyncPointInfo()
				// 在这里加 sync point？ 这个性能会有明显影响么,这个要测过
				if syncPointInfo.EnableSyncPoint && event.CommitTs() > syncPointInfo.NextSyncPointTs {
					dispatcherItem.GetEventChan() <- Event{} //构造 Sync Point Event
					syncPointInfo.NextSyncPointTs = oracle.GoTimeToTS(
						oracle.GetTimeFromTS(syncPointInfo.NextSyncPointTs).
							Add(syncPointInfo.SyncPointInterval))
				}

				// deal with event
				dispatcherItem.GetMemoryUsage().Add(event.CommitTs(), event.MemoryCost())
				dispatcherItem.GetEventChan() <- event // 换成一个函数
			}
			dispatcherItem.UpdateResolvedTs(eventResponse.ResolvedTs) // todo:枷锁
		}
		//
	}
}

// 这个要想一下，做成单线程，还是收发拆开
func (c *EventCollector) getAddr(d dispatcher.Dispatcher) string {
	client := c.masterClient.Client
	// make the msg
	err := client.Send(msg)
	if err != nil {
		// 抛出错误
	}

	for {
		addrResponse, err := client.Recv() // 这个会直接阻塞么？
		dispatcherId := addrResponse.DispatcherId
		if dispatcherId != d.GetId() {
			log.Error("wrong dispatcher")
			continue
		}
		addr := addrResponse.Addr
		return addr
	}

}
