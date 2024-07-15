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

	"github.com/flowbehappy/tigate/downstreamadapter/dispatcher"
	"github.com/flowbehappy/tigate/eventpb"
	"github.com/flowbehappy/tigate/pkg/apperror"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/google/uuid"
	"github.com/ngaut/log"
	"go.uber.org/zap"
)

const RegisterDispatcherTopic = "RegisterDispatcher"
const EventFeedTopic = "EventFeed"

/*
EventCollector is responsible for collecting the events from event service and dispatching them to different dispatchers.
Besides, EventCollector also generate SyncPoint Event for dispatchers when necessary.
EventCollector is an instance-level component.
*/
type EventCollector struct {
	messageCenter     messaging.MessageCenter
	clusterID         messaging.ServerId
	targetID          messaging.ServerId
	dispatcherMap     map[common.DispatcherID]dispatcher.Dispatcher // dispatcher_id --> dispatcher
	wg                sync.WaitGroup
	globalMemoryQuota int64
}

func NewEventCollector(messageCenter messaging.MessageCenter, globalMemoryQuota int64, clusterID messaging.ServerId, targetID messaging.ServerId) *EventCollector {
	eventCollector := EventCollector{
		messageCenter:     messageCenter,
		clusterID:         clusterID,
		targetID:          targetID,
		globalMemoryQuota: globalMemoryQuota,
		dispatcherMap:     make(map[common.DispatcherID]dispatcher.Dispatcher),
	}
	eventCollector.messageCenter.RegisterHandler(EventFeedTopic, eventCollector.RecvEventsMessage)
	return &eventCollector
}

func (c *EventCollector) RegisterDispatcher(d dispatcher.Dispatcher, startTs uint64) error {
	err := c.messageCenter.SendEvent(&messaging.TargetMessage{
		To:    c.targetID,
		Topic: RegisterDispatcherTopic,
		Type:  messaging.TypeRegisterDispatcherRequest,
		Message: eventpb.RegisterDispatcherRequest{
			DispatcherId: uuid.UUID(d.GetId()).String(),
			TableSpan: &eventpb.TableSpan{
				TableID:  d.GetTableSpan().TableID,
				StartKey: d.GetTableSpan().StartKey,
				EndKey:   d.GetTableSpan().EndKey,
			},
			Remove:  false,
			StartTs: startTs,
		},
	})
	if err != nil {
		log.Error("failed to send register dispatcher request message", zap.Error(err))
		return err
	}
	return nil
}

func (c *EventCollector) RemoveDispatcher(d dispatcher.Dispatcher) error {
	err := c.messageCenter.SendEvent(&messaging.TargetMessage{
		To:    c.targetID,
		Topic: RegisterDispatcherTopic,
		Type:  messaging.TypeRegisterDispatcherRequest,
		Message: eventpb.RegisterDispatcherRequest{
			DispatcherId: uuid.UUID(d.GetId()).String(),
			Remove:       true,
		},
	})
	if err != nil {
		log.Error("failed to send register dispatcher request message", zap.Error(err))
		return err
	}
	return nil
}

func (c *EventCollector) RecvEventsMessage(msg *messaging.TargetMessage) error {
	/*
		if dispatcher.GetGlobalMemoryUsage().UsedBytes > c.globalMemoryQuota {
			// 卡一段时间,怎么拍啊？
			log.Info("downstream adapter is out of memory, waiting for 30 seconds")
			time.Sleep(30 * time.Second)
			continue
		}
	*/

	eventFeeds, ok := msg.Message.(*messaging.EventFeed)
	if !ok {
		log.Error("invalid event feed message", zap.Any("msg", msg))
		return apperror.AppError{Type: apperror.ErrorTypeInvalidMessage, Reason: fmt.Sprintf("invalid heartbeat response message")}
	}

	dispatcherId, err := uuid.Parse(eventFeeds.DispatcherId)
	if err != nil {
		log.Error("invalid dispatcher id", zap.String("dispatcher_id", eventFeeds.DispatcherId))
		return apperror.AppError{Type: apperror.ErrorTypeInvalidMessage, Reason: fmt.Sprintf("invalid dispatcher id: %s", eventFeeds.DispatcherId)}
	}

	if dispatcherItem, ok := c.dispatcherMap[common.DispatcherID(dispatcherId)]; ok {
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
		if eventFeeds.TableInfo != nil {
			dispatcherItem.(*dispatcher.TableEventDispatcher).InitTableInfo(eventFeeds.TableInfo)
		}
		for _, txnEvent := range eventFeeds.TxnEvents {
			dispatcherItem.PushEvent(txnEvent)
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
		}
		dispatcherItem.UpdateResolvedTs(eventFeeds.ResolvedTs)
	}
}
