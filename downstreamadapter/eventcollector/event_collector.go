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

	"github.com/flowbehappy/tigate/pkg/node"

	"github.com/flowbehappy/tigate/downstreamadapter/dispatcher"
	"github.com/flowbehappy/tigate/eventpb"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/flowbehappy/tigate/utils/dynstream"
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

func (m *DispatcherMap) Get(dispatcherId common.DispatcherID) (*dispatcher.Dispatcher, bool) {
	d, ok := m.m.Load(dispatcherId)
	if !ok {
		return nil, false
	}
	dispatcher, ok := d.(*dispatcher.Dispatcher)
	return dispatcher, ok
}

func (m *DispatcherMap) Set(dispatcherId common.DispatcherID, d *dispatcher.Dispatcher) {
	m.m.Store(dispatcherId, d)
}

func (m *DispatcherMap) Delete(dispatcherId common.DispatcherID) {
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
	serverId          node.ID
	dispatcherMap     *DispatcherMap
	globalMemoryQuota int64
	wg                sync.WaitGroup

	dispatcherEventsDynamicStream dynstream.DynamicStream[common.DispatcherID, common.Event, *dispatcher.Dispatcher]

	registerMessageChan                          *chann.DrainableChann[RegisterInfo] // for temp
	metricDispatcherReceivedKVEventCount         prometheus.Counter
	metricDispatcherReceivedResolvedTsEventCount prometheus.Counter
	metricReceiveEventLagDuration                prometheus.Observer
	metricResolvedTsLag                          prometheus.Gauge
}

func NewEventCollector(globalMemoryQuota int64, serverId node.ID) *EventCollector {
	eventCollector := EventCollector{
		serverId:                                     serverId,
		globalMemoryQuota:                            globalMemoryQuota,
		dispatcherMap:                                &DispatcherMap{},
		dispatcherEventsDynamicStream:                dispatcher.GetDispatcherEventsDynamicStream(),
		registerMessageChan:                          chann.NewAutoDrainChann[RegisterInfo](),
		metricDispatcherReceivedKVEventCount:         metrics.DispatcherReceivedEventCount.WithLabelValues("KVEvent"),
		metricDispatcherReceivedResolvedTsEventCount: metrics.DispatcherReceivedEventCount.WithLabelValues("ResolvedTs"),
		metricReceiveEventLagDuration:                metrics.EventCollectorReceivedEventLagDuration.WithLabelValues("Msg"),
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
	return &eventCollector
}

// RegisterDispatcher register a dispatcher to event collector.
// If the dispatcher is not table trigger event dispatcher, filterConfig will be nil.
func (c *EventCollector) RegisterDispatcher(info RegisterInfo) error {
	err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendEvent(&messaging.TargetMessage{
		To:    c.serverId, // demo 中 每个节点都有自己的 eventService
		Topic: messaging.EventServiceTopic,
		Type:  messaging.TypeRegisterDispatcherRequest,
		Message: []messaging.IOTypeT{&messaging.RegisterDispatcherRequest{RegisterDispatcherRequest: &eventpb.RegisterDispatcherRequest{
			DispatcherId: info.Dispatcher.GetId().ToPB(),
			TableSpan:    info.Dispatcher.GetTableSpan(),
			Remove:       false,
			StartTs:      info.StartTs,
			ServerId:     c.serverId.String(),
			FilterConfig: info.FilterConfig,
		}}},
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
		Message: []messaging.IOTypeT{&messaging.RegisterDispatcherRequest{RegisterDispatcherRequest: &eventpb.RegisterDispatcherRequest{
			DispatcherId: d.GetId().ToPB(),
			Remove:       true,
			ServerId:     c.serverId.String(),
			TableSpan:    d.GetTableSpan(),
		}}}})
	if err != nil {
		log.Error("failed to send register dispatcher request message", zap.Error(err))
		c.registerMessageChan.In() <- RegisterInfo{
			Dispatcher: d,
			StartTs:    0,
		}
		return err
	}
	c.dispatcherMap.Delete(d.GetId())
	return nil
}

func (c *EventCollector) RecvEventsMessage(ctx context.Context, msg *messaging.TargetMessage) error {
	inflightDuration := time.Since(time.Unix(0, msg.CrateAt)).Milliseconds()
	c.metricReceiveEventLagDuration.Observe(float64(inflightDuration))
	for _, msg := range msg.Message {
		event, ok := msg.(common.Event)
		if !ok {
			log.Panic("invalid message type", zap.Any("msg", msg))
		}
		switch event.GetType() {
		case common.TypeBatchResolvedEvent:
			for _, e := range event.(*common.BatchResolvedEvent).Events {
				c.metricDispatcherReceivedResolvedTsEventCount.Inc()
				c.dispatcherEventsDynamicStream.In() <- e
			}
		default:
			c.metricDispatcherReceivedKVEventCount.Inc()
			c.dispatcherEventsDynamicStream.In() <- event
		}
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
