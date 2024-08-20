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
	"sync"
	"time"

	"github.com/flowbehappy/tigate/downstreamadapter/dispatcher"
	"github.com/flowbehappy/tigate/eventpb"
	"github.com/flowbehappy/tigate/pkg/apperror"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type DispatcherMap struct {
	// dispatcher_id --> dispatcher
	m sync.Map
}

func (m *DispatcherMap) Get(dispatcherId string) (*dispatcher.Dispatcher, bool) {
	d, ok := m.m.Load(dispatcherId)
	if !ok {
		return nil, false
	}
	dispatcher, ok := d.(*dispatcher.Dispatcher)
	return dispatcher, ok
}

func (m *DispatcherMap) Set(dispatcherId string, d *dispatcher.Dispatcher) {
	m.m.Store(dispatcherId, d)
}

func (m *DispatcherMap) Delete(dispatcherId string) {
	m.m.Delete(dispatcherId)
}

type RegisterInfo struct {
	Dispatcher   *dispatcher.Dispatcher
	StartTs      uint64
	FilterConfig *eventpb.FilterConfig
}

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

	registerMessageChan                          *chann.DrainableChann[RegisterInfo] // for temp
	metricDispatcherReceivedKVEventCount         prometheus.Counter
	metricDispatcherReceivedResolvedTsEventCount prometheus.Counter
	metricReceiveEventLagDuration                prometheus.Observer
	metricReceiveResolvedTsEventLagDuration      prometheus.Observer
	metricResolvedTsLag                          prometheus.Gauge
}

func NewEventCollector(globalMemoryQuota int64, serverId messaging.ServerId) *EventCollector {
	eventCollector := EventCollector{
		serverId:                             serverId,
		globalMemoryQuota:                    globalMemoryQuota,
		dispatcherMap:                        &DispatcherMap{},
		registerMessageChan:                  chann.NewAutoDrainChann[RegisterInfo](),
		metricDispatcherReceivedKVEventCount: metrics.DispatcherReceivedEventCount.WithLabelValues("KVEvent"),
		metricDispatcherReceivedResolvedTsEventCount: metrics.DispatcherReceivedEventCount.WithLabelValues("ResolvedTs"),
		metricReceiveEventLagDuration:                metrics.EventCollectorReceivedEventLagDuration.WithLabelValues("KVEvent"),
		metricReceiveResolvedTsEventLagDuration:      metrics.EventCollectorReceivedEventLagDuration.WithLabelValues("ResolvedTs"),
		metricResolvedTsLag:                          metrics.EventCollectorResolvedTsLagGauge,
	}
	appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).RegisterHandler(messaging.EventCollectorTopic, eventCollector.RecvEventsMessage)

	eventCollector.wg.Add(1)
	go func() {
		defer eventCollector.wg.Done()
		for {
			registerInfo := <-eventCollector.registerMessageChan.Out()
			var err error
			if registerInfo.StartTs > 0 {
				err = eventCollector.RegisterDispatcher(registerInfo)
			} else {
				err = eventCollector.RemoveDispatcher(registerInfo.Dispatcher)
			}
			if err != nil {
				// Wait for a while to avoid sending too many requests, since the
				// event service may be busy.
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()
	// update metrics
	eventCollector.updateMetrics(context.Background())

	return &eventCollector
}

// RegisterDispatcher register a dispatcher to event collector.
// If the dispatcher is not table trigger event dispatcher, filterConfig will be nil.
func (c *EventCollector) RegisterDispatcher(info RegisterInfo) error {
	err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendEvent(&messaging.TargetMessage{
		To:    c.serverId, // demo 中 每个节点都有自己的 eventService
		Topic: messaging.EventServiceTopic,
		Type:  messaging.TypeRegisterDispatcherRequest,
		Message: messaging.RegisterDispatcherRequest{RegisterDispatcherRequest: &eventpb.RegisterDispatcherRequest{
			DispatcherId: info.Dispatcher.GetId(),
			TableSpan:    info.Dispatcher.GetTableSpan().TableSpan,
			Remove:       false,
			StartTs:      info.StartTs,
			ServerId:     c.serverId.String(),
			FilterConfig: info.FilterConfig,
		}},
	})
	if err != nil {
		log.Error("failed to send register dispatcher request message", zap.Error(err))
		c.registerMessageChan.In() <- info
		return err
	}
	c.dispatcherMap.Set(info.Dispatcher.GetId(), info.Dispatcher)
	metrics.EventCollectorRegisteredDispatcherCount.Inc()
	return nil
}

func (c *EventCollector) RemoveDispatcher(d *dispatcher.Dispatcher) error {
	err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendEvent(&messaging.TargetMessage{
		To:    c.serverId,
		Topic: messaging.EventServiceTopic,
		Type:  messaging.TypeRegisterDispatcherRequest,
		Message: messaging.RegisterDispatcherRequest{RegisterDispatcherRequest: &eventpb.RegisterDispatcherRequest{
			DispatcherId: d.GetId(),
			Remove:       true,
			ServerId:     c.serverId.String(),
			TableSpan:    d.GetTableSpan().TableSpan,
		},
		},
	})
	if err != nil {
		log.Error("failed to send register dispatcher request message", zap.Error(err))
		c.registerMessageChan.In() <- RegisterInfo{
			Dispatcher: d,
			StartTs:    0,
		}
		return err
	}
	c.dispatcherMap.Delete(string(d.GetId()))
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
		return apperror.AppError{Type: apperror.ErrorTypeInvalidMessage, Reason: "invalid heartbeat response message"}
	}
	inflightDuration := time.Since(time.Unix(0, msg.CrateAt)).Milliseconds()

	dispatcherID := txnEvent.DispatcherID

	if dispatcherItem, ok := c.dispatcherMap.Get(dispatcherID); ok {
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
		if txnEvent.IsDMLEvent() || txnEvent.IsDDLEvent() {
			dispatcherItem.PushTxnEvent(txnEvent)
			c.metricDispatcherReceivedKVEventCount.Inc()
			c.metricReceiveEventLagDuration.Observe(float64(inflightDuration))
		} else {
			dispatcherItem.UpdateResolvedTs(txnEvent.ResolvedTs)
			c.metricDispatcherReceivedResolvedTsEventCount.Inc()
			c.metricReceiveResolvedTsEventLagDuration.Observe(float64(inflightDuration))
		}

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

	}
	return nil
}

func (c *EventCollector) updateMetrics(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				minResolvedTs := uint64(0)
				c.dispatcherMap.m.Range(func(key, value interface{}) bool {
					d, ok := value.(*dispatcher.Dispatcher)
					if !ok {
						return true
					}
					if minResolvedTs == 0 || d.GetResolvedTs() < minResolvedTs {
						minResolvedTs = d.GetResolvedTs()
					}
					return true
				})
				if minResolvedTs == 0 {
					continue
				}
				phyResolvedTs := oracle.ExtractPhysical(minResolvedTs)
				lag := (oracle.GetPhysical(time.Now()) - phyResolvedTs) / 1e3
				c.metricResolvedTsLag.Set(float64(lag))
			}
		}
	}()
	return nil
}
