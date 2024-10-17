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
	"github.com/flowbehappy/tigate/pkg/node"

	"github.com/flowbehappy/tigate/eventpb"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
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

type DispatcherRequest struct {
	Dispatcher   *dispatcher.Dispatcher
	ActionType   eventpb.ActionType
	StartTs      uint64
	FilterConfig *eventpb.FilterConfig
}

const (
	eventServiceTopic         = messaging.EventServiceTopic
	eventCollectorTopic       = messaging.EventCollectorTopic
	typeRegisterDispatcherReq = messaging.TypeRegisterDispatcherRequest
)

/*
EventCollector is the relay between EventService and DispatcherManager, responsible for:
1. Send dispatcher request to EventService.
2. Collect the events from EvenService and dispatch them to different dispatchers.
3. Generate SyncPoint Event for dispatchers when necessary.
EventCollector is an instance-level component.
*/
type EventCollector struct {
	serverId          node.ID
	dispatcherMap     *DispatcherMap
	globalMemoryQuota int64
	mc                messaging.MessageCenter
	wg                sync.WaitGroup

	// dispatcherRequestChan is used cached dispatcher request when some error occurs.
	dispatcherRequestChan *chann.DrainableChann[DispatcherRequest]

	// ds is the dynamicStream for dispatcher events.
	// All the events from event service will be sent to ds to handle.
	// ds will dispatch the events to different dispatchers according to the dispatcherID.
	ds dynstream.DynamicStream[common.DispatcherID, dispatcher.DispatcherEvent, *dispatcher.Dispatcher]

	metricDispatcherReceivedKVEventCount         prometheus.Counter
	metricDispatcherReceivedResolvedTsEventCount prometheus.Counter
	metricReceiveEventLagDuration                prometheus.Observer
}

func New(ctx context.Context, globalMemoryQuota int64, serverId node.ID) *EventCollector {
	eventCollector := EventCollector{
		serverId:                             serverId,
		globalMemoryQuota:                    globalMemoryQuota,
		dispatcherMap:                        &DispatcherMap{},
		ds:                                   dispatcher.GetDispatcherEventsDynamicStream(),
		dispatcherRequestChan:                chann.NewAutoDrainChann[DispatcherRequest](),
		mc:                                   appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
		metricDispatcherReceivedKVEventCount: metrics.DispatcherReceivedEventCount.WithLabelValues("KVEvent"),
		metricDispatcherReceivedResolvedTsEventCount: metrics.DispatcherReceivedEventCount.WithLabelValues("ResolvedTs"),
		metricReceiveEventLagDuration:                metrics.EventCollectorReceivedEventLagDuration.WithLabelValues("Msg"),
	}
	eventCollector.mc.RegisterHandler(messaging.EventCollectorTopic, eventCollector.RecvEventsMessage)

	// wg is not `Wait`, and not controlled by the context.
	eventCollector.wg.Add(1)
	go func() {
		defer eventCollector.wg.Done()
		eventCollector.processDispatcherRequests()
	}()
	eventCollector.wg.Add(1)
	go func() {
		defer eventCollector.wg.Done()
		eventCollector.updateMetrics(ctx)
	}()
	return &eventCollector
}

func (c *EventCollector) processDispatcherRequests() {
	defer c.wg.Done()
	for req := range c.dispatcherRequestChan.Out() {
		if err := c.SendDispatcherRequest(req); err != nil {
			log.Error("failed to process dispatcher action", zap.Error(err))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (c *EventCollector) SendDispatcherRequest(req DispatcherRequest) error {
	message := &messaging.RegisterDispatcherRequest{
		RegisterDispatcherRequest: &eventpb.RegisterDispatcherRequest{
			DispatcherId: req.Dispatcher.GetId().ToPB(),
			ActionType:   req.ActionType,
			// FIXME: It can be another server id in the future.
			ServerId:  c.serverId.String(),
			TableSpan: req.Dispatcher.GetTableSpan(),
			StartTs:   req.StartTs,
		},
	}

	// If the action type is register, we need fill all config related fields.
	if req.ActionType == eventpb.ActionType_ACTION_TYPE_REGISTER {
		message.RegisterDispatcherRequest.FilterConfig = req.FilterConfig
		message.RegisterDispatcherRequest.EnableSyncPoint = req.Dispatcher.EnableSyncPoint()
		message.RegisterDispatcherRequest.SyncPointTs = req.Dispatcher.GetSyncPointTs()
		message.RegisterDispatcherRequest.SyncPointInterval = uint64(req.Dispatcher.GetSyncPointInterval().Seconds())
	}

	err := c.mc.SendCommand(&messaging.TargetMessage{
		To:      c.serverId,
		Topic:   eventServiceTopic,
		Type:    typeRegisterDispatcherReq,
		Message: []messaging.IOTypeT{message},
	})

	if err != nil {
		log.Error("failed to send dispatcher request message to event service, try again later", zap.Error(err))
		// Put the request back to the channel for later retry.
		c.dispatcherRequestChan.In() <- req
		return err
	}

	// If the action type is register or remove, we need to update the dispatcher map.
	if req.ActionType == eventpb.ActionType_ACTION_TYPE_REGISTER {
		c.dispatcherMap.Set(req.Dispatcher.GetId(), req.Dispatcher)
		metrics.EventCollectorRegisteredDispatcherCount.Inc()
	} else if req.ActionType == eventpb.ActionType_ACTION_TYPE_REMOVE {
		c.dispatcherMap.Delete(req.Dispatcher.GetId())
	}

	return nil
}

// RecvEventsMessage is the handler for the events message from EventService.
func (c *EventCollector) RecvEventsMessage(_ context.Context, msg *messaging.TargetMessage) error {
	inflightDuration := time.Since(time.Unix(0, msg.CreateAt)).Milliseconds()
	c.metricReceiveEventLagDuration.Observe(float64(inflightDuration))
	for _, msg := range msg.Message {
		event, ok := msg.(commonEvent.Event)
		if !ok {
			log.Panic("invalid message type", zap.Any("msg", msg))
		}
		switch event.GetType() {
		case commonEvent.TypeBatchResolvedEvent:
			for _, e := range event.(*commonEvent.BatchResolvedEvent).Events {
				c.metricDispatcherReceivedResolvedTsEventCount.Inc()
				c.ds.In() <- dispatcher.NewDispatcherEvent(e)
			}
		default:
			c.metricDispatcherReceivedKVEventCount.Inc()
			c.ds.In() <- dispatcher.NewDispatcherEvent(event)
		}
	}
	return nil
}

func (c *EventCollector) updateMetrics(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.updateResolvedTsMetric()
		}
	}
}

func (c *EventCollector) updateResolvedTsMetric() {
	var minResolvedTs uint64
	c.dispatcherMap.m.Range(func(_, value interface{}) bool {
		if d, ok := value.(*dispatcher.Dispatcher); ok {
			if minResolvedTs == 0 || d.GetResolvedTs() < minResolvedTs {
				minResolvedTs = d.GetResolvedTs()
			}
		}
		return true
	})

	if minResolvedTs > 0 {
		phyResolvedTs := oracle.ExtractPhysical(minResolvedTs)
		lagMs := float64(oracle.GetPhysical(time.Now())-phyResolvedTs) / 1e3
		metrics.EventCollectorResolvedTsLagGauge.Set(lagMs)
	}
}
