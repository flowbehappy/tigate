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

package eventcollector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/downstreamadapter/dispatcher"
	"github.com/flowbehappy/tigate/eventpb"
	"github.com/flowbehappy/tigate/pkg/apperror"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/chann"
	"go.uber.org/zap"
)

type DispatcherMap struct {
	mutex sync.RWMutex
	m     map[common.DispatcherID]dispatcher.Dispatcher // dispatcher_id --> dispatcher
}

func newDispatcherMap() *DispatcherMap {
	return &DispatcherMap{
		m: make(map[common.DispatcherID]dispatcher.Dispatcher),
	}
}

func (m *DispatcherMap) Get(dispatcherId common.DispatcherID) (dispatcher.Dispatcher, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	d, ok := m.m[dispatcherId]
	return d, ok
}

func (m *DispatcherMap) Set(dispatcherId common.DispatcherID, d dispatcher.Dispatcher) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.m[dispatcherId] = d
}

func (m *DispatcherMap) Delete(dispatcherId common.DispatcherID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.m, dispatcherId)
}

type RegisterInfo struct {
	dispatcher dispatcher.Dispatcher
	startTs    uint64
}

const recvEventConcurrency = 16

/*
EventCollector is responsible for collecting the events from event service and dispatching them to different dispatchers.
Besides, EventCollector also generate SyncPoint Event for dispatchers when necessary.
EventCollector is an instance-level component.
*/
type EventCollector struct {
	serverId          messaging.ServerId
	dispatcherMap     *DispatcherMap
	globalMemoryQuota int64
	wg                sync.WaitGroup

	registerMessageChan *chann.DrainableChann[*RegisterInfo] // for temp

	messageChan []chan *common.TxnEvent
}

func NewEventCollector(globalMemoryQuota int64, serverId messaging.ServerId) *EventCollector {
	eventCollector := EventCollector{
		serverId:            serverId,
		globalMemoryQuota:   globalMemoryQuota,
		dispatcherMap:       newDispatcherMap(),
		registerMessageChan: chann.NewAutoDrainChann[*RegisterInfo](),
		messageChan:         make([]chan *common.TxnEvent, recvEventConcurrency),
	}
	appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).RegisterHandler(messaging.EventFeedTopic, eventCollector.RecvEventsMessage)

	eventCollector.wg.Add(1)
	go func(c *EventCollector) {
		defer eventCollector.wg.Done()
		for {
			registerInfo := <-eventCollector.registerMessageChan.Out()
			d := registerInfo.dispatcher
			startTs := registerInfo.startTs
			err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendEvent(&messaging.TargetMessage{
				To:    c.serverId, // demo 中 每个节点都有自己的 eventService
				Topic: messaging.EventServiceTopic,
				Type:  messaging.TypeRegisterDispatcherRequest,
				Message: messaging.RegisterDispatcherRequest{RegisterDispatcherRequest: &eventpb.RegisterDispatcherRequest{
					DispatcherId: uuid.UUID(d.GetId()).String(),
					TableSpan:    d.GetTableSpan().TableSpan,
					Remove:       false,
					StartTs:      startTs,
					ServerId:     c.serverId.String(),
				}},
			})
			if err != nil {
				log.Error("failed to send register dispatcher request message", zap.Error(err))
				c.registerMessageChan.In() <- &RegisterInfo{
					dispatcher: d,
					startTs:    startTs,
				}
			}
			time.Sleep(10 * time.Millisecond) // for test
		}
	}(&eventCollector)

	for i := 0; i < recvEventConcurrency; i++ {
		idx := i
		eventCollector.messageChan[idx] = make(chan *common.TxnEvent, 1024)
		eventCollector.wg.Add(1)
		go func(c *EventCollector, idx int) {
			defer c.wg.Done()
			for {
				txnEvent := <-c.messageChan[idx] // 阻塞等待
				dispatcherID := txnEvent.DispatcherID
				if dispatcherItem, ok := c.dispatcherMap.Get(common.DispatcherID(uuid.MustParse(dispatcherID))); ok {
					if txnEvent.IsDMLEvent() {
						dispatcherItem.PushTxnEvent(txnEvent)
					} else {
						dispatcherItem.UpdateResolvedTs(txnEvent.ResolvedTs)
					}
				}
			}
		}(&eventCollector, idx)
	}

	return &eventCollector
}

func (c *EventCollector) RegisterDispatcher(d dispatcher.Dispatcher, startTs uint64) error {
	err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendEvent(&messaging.TargetMessage{
		To:    c.serverId, // demo 中 每个节点都有自己的 eventService
		Topic: messaging.EventServiceTopic,
		Type:  messaging.TypeRegisterDispatcherRequest,
		Message: messaging.RegisterDispatcherRequest{RegisterDispatcherRequest: &eventpb.RegisterDispatcherRequest{
			DispatcherId: uuid.UUID(d.GetId()).String(),
			TableSpan:    d.GetTableSpan().TableSpan,
			Remove:       false,
			StartTs:      startTs,
			ServerId:     c.serverId.String(),
		}},
	})
	if err != nil {
		log.Error("failed to send register dispatcher request message", zap.Error(err))
		c.registerMessageChan.In() <- &RegisterInfo{
			dispatcher: d,
			startTs:    startTs,
		}
		return err
	}
	c.dispatcherMap.Set(d.GetId(), d)
	return nil
}

func (c *EventCollector) RemoveDispatcher(d dispatcher.Dispatcher) error {
	err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendEvent(&messaging.TargetMessage{
		To:    c.serverId,
		Topic: messaging.EventServiceTopic,
		Type:  messaging.TypeRegisterDispatcherRequest,
		Message: messaging.RegisterDispatcherRequest{RegisterDispatcherRequest: &eventpb.RegisterDispatcherRequest{
			DispatcherId: uuid.UUID(d.GetId()).String(),
			Remove:       true,
			ServerId:     c.serverId.String(),
			TableSpan:    d.GetTableSpan().TableSpan,
		},
		},
	})
	if err != nil {
		log.Error("failed to send register dispatcher request message", zap.Error(err))
		return err
	}
	c.dispatcherMap.Delete(common.DispatcherID(d.GetId()))
	return nil
}

func (c *EventCollector) RecvEventsMessage(ctx context.Context, msg *messaging.TargetMessage) error {
	/*
		if dispatcher.GetGlobalMemoryUsage().UsedBytes > c.globalMemoryQuota {
			// 卡一段时间,怎么拍啊？
			log.Info("downstream adapter is out of memory, waiting for 30 seconds")
			time.Sleep(30 * time.Second)
			continue
		}
	*/

	txnEvent, ok := msg.Message.(*common.TxnEvent)
	if !ok {
		log.Error("invalid event feed message", zap.Any("msg", msg))
		return apperror.AppError{Type: apperror.ErrorTypeInvalidMessage, Reason: fmt.Sprintf("invalid heartbeat response message")}
	}

	idx := uuid.MustParse(txnEvent.DispatcherID).ID() % recvEventConcurrency
	c.messageChan[idx] <- txnEvent

	return nil
}

// log.Info("Recv TxnEvent", zap.Any("dispatcherID", dispatcherID), zap.Any("event is dml event", txnEvent.IsDMLEvent()))
// if txnEvent.IsDMLEvent() {
// 	rowEvent := txnEvent.GetRows()[0]
// 	log.Info("Recv TxnEvent", zap.Any("dispatcherID", dispatcherID), zap.Any("event info", rowEvent.CommitTs), zap.Any("table name", rowEvent.TableInfo.TableName))
// }

//if dispatcherItem, ok := c.dispatcherMap.Get(common.DispatcherID(uuid.MustParse(dispatcherID))); ok {
// check whether need to update speed ratio
//ok, ratio := dispatcherItem.GetMemoryUsage().UpdatedSpeedRatio(eventResponse.Ratio)
// if ok {
// 	request := eventpb.EventRequest{
// 		DispatcherId: dispatcherId,
// 		TableSpan:    dispatcherItem.GetTableSpan(),
// 		Ratio:        ratio,
// 		Remove:       false,
// 	}
// 	// 这个开销大么，在这里等合适么？看看要不要拆一下
// 	err := client.Send(&request)
// 	if err != nil {
// 		//
// 	}
// }

// if dispatcherId == dispatcher.TableTriggerEventDispatcherId {
// 	for _, event := range eventResponse.Events {
// 		dispatcherItem.GetMemoryUsage().Add(event.CommitTs(), event.MemoryCost())
// 		dispatcherItem.GetEventChan() <- event // 换成一个函数
// 	}
// 	dispatcherItem.UpdateResolvedTs(eventResponse.ResolvedTs) // todo:枷锁
// 	continue
// }
//for _, txnEvent := range eventFeeds.TxnEvents {
// TODO: message 改过以后重写，先串起来。
// if txnEvent.IsDMLEvent() {
// 	dispatcherItem.PushTxnEvent(txnEvent)
// } else {
// 	dispatcherItem.UpdateResolvedTs(txnEvent.ResolvedTs)
// }

// dispatcherItem.UpdateResolvedTs(eventFeeds.ResolvedTs)
/*
	syncPointInfo := dispatcherItem.GetSyncPointInfo()
	// 在这里加 sync point？ 这个性能会有明显影响么,这个要测过
	if syncPointInfo.EnableSyncPoint && event.CommitTs() > syncPointInfo.NextSyncPointTs {
		dispatcherItem.GetEventChan() <- Event{} //构造 Sync Point Event
		syncPointInfo.NextSyncPointTs = oracle.GoTimeToTS(
			oracle.GetTimeFromTS(syncPointInfo.NextSyncPointTs).
				Add(syncPointInfo.SyncPointInterval))
	}
*/

// // deal with event
// dispatcherItem.GetMemoryUsage().Add(event.CommitTs(), event.MemoryCost())
// dispatcherItem.GetEventChan() <- event // 换成一个函数
//}

// }
// return nil
// }
