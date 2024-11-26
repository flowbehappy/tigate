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
	"sync/atomic"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/pingcap/ticdc/pkg/node"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

var (
	handleEventDuration = metrics.EventCollectorHandleEventDuration
)

type DispatcherRequest struct {
	Dispatcher dispatcher.EventDispatcher
	ActionType eventpb.ActionType
	StartTs    uint64
	OnlyUse    bool
}

type TargetAndDispatcherRequest struct {
	Target node.ID
	Topic  string
	Req    DispatcherRequest
}

const (
	eventServiceTopic         = messaging.EventServiceTopic
	eventCollectorTopic       = messaging.EventCollectorTopic
	logCoordinatorTopic       = messaging.LogCoordinatorTopic
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
	dispatcherMap     sync.Map
	globalMemoryQuota int64
	mc                messaging.MessageCenter
	wg                sync.WaitGroup

	// dispatcherRequestChan is used cached dispatcher request when some error occurs.
	dispatcherRequestChan *chann.DrainableChann[TargetAndDispatcherRequest]

	logCoordinatorRequestChan *chann.DrainableChann[*logservicepb.ReusableEventServiceRequest]

	// ds is the dynamicStream for dispatcher events.
	// All the events from event service will be sent to ds to handle.
	// ds will dispatch the events to different dispatchers according to the dispatcherID.
	ds dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherEvent, *DispatcherStat, *EventsHandler]

	coordinatorInfo struct {
		sync.RWMutex
		id node.ID
	}

	metricDispatcherReceivedKVEventCount         prometheus.Counter
	metricDispatcherReceivedResolvedTsEventCount prometheus.Counter
	metricReceiveEventLagDuration                prometheus.Observer
}

func New(ctx context.Context, globalMemoryQuota int64, serverId node.ID) *EventCollector {
	eventCollector := EventCollector{
		serverId:                             serverId,
		globalMemoryQuota:                    globalMemoryQuota,
		dispatcherMap:                        sync.Map{},
		dispatcherRequestChan:                chann.NewAutoDrainChann[TargetAndDispatcherRequest](),
		logCoordinatorRequestChan:            chann.NewAutoDrainChann[*logservicepb.ReusableEventServiceRequest](),
		mc:                                   appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
		metricDispatcherReceivedKVEventCount: metrics.DispatcherReceivedEventCount.WithLabelValues("KVEvent"),
		metricDispatcherReceivedResolvedTsEventCount: metrics.DispatcherReceivedEventCount.WithLabelValues("ResolvedTs"),
		metricReceiveEventLagDuration:                metrics.EventCollectorReceivedEventLagDuration.WithLabelValues("Msg"),
	}
	eventCollector.ds = NewEventDynamicStream(&eventCollector)
	eventCollector.mc.RegisterHandler(messaging.EventCollectorTopic, eventCollector.RecvEventsMessage)

	eventCollector.wg.Add(1)
	go func() {
		defer eventCollector.wg.Done()
		eventCollector.processFeedback(ctx)
	}()

	// wg is not `Wait`, and not controlled by the context.
	eventCollector.wg.Add(1)
	go func() {
		defer eventCollector.wg.Done()
		eventCollector.processDispatcherRequests(ctx)
	}()
	eventCollector.wg.Add(1)
	go func() {
		defer eventCollector.wg.Done()
		eventCollector.processLogCoordinatorRequest(ctx)
	}()
	eventCollector.wg.Add(1)
	go func() {
		defer eventCollector.wg.Done()
		eventCollector.updateMetrics(ctx)
	}()
	return &eventCollector
}

func (c *EventCollector) AddDispatcher(target dispatcher.EventDispatcher, memoryQuota int) {
	log.Info("add dispatcher", zap.Stringer("dispatcher", target.GetId()))
	defer func() {
		log.Info("add dispatcher done", zap.Stringer("dispatcher", target.GetId()))
	}()
	stat := &DispatcherStat{
		dispatcherID: target.GetId(),
		target:       target,
	}
	stat.reset()
	stat.sendCommitTs.Store(target.GetStartTs())
	c.dispatcherMap.Store(target.GetId(), stat)
	metrics.EventCollectorRegisteredDispatcherCount.Inc()

	areaSetting := dynstream.NewAreaSettings()
	areaSetting.MaxPendingSize = memoryQuota
	err := c.ds.AddPath(target.GetId(), stat, areaSetting)
	if err != nil {
		log.Error("add dispatcher to dynamic stream failed", zap.Error(err))
	}

	// TODO: handle the return error(now even it return error, it will be retried later, we can just ignore it now)
	c.mustSendDispatcherRequest(c.serverId, eventServiceTopic, DispatcherRequest{
		Dispatcher: target,
		StartTs:    target.GetStartTs(),
		ActionType: eventpb.ActionType_ACTION_TYPE_REGISTER,
	})

	c.logCoordinatorRequestChan.In() <- &logservicepb.ReusableEventServiceRequest{
		ID:      target.GetId().ToPB(),
		Span:    target.GetTableSpan(),
		StartTs: target.GetStartTs(),
	}
}

func (c *EventCollector) RemoveDispatcher(target *dispatcher.Dispatcher) {
	log.Info("remove dispatcher", zap.Stringer("dispatcher", target.GetId()))
	defer func() {
		log.Info("remove dispatcher done", zap.Stringer("dispatcher", target.GetId()))
	}()
	value, ok := c.dispatcherMap.Load(target.GetId())
	if !ok {
		return
	}
	stat := value.(*DispatcherStat)
	stat.unregisterDispatcher(c)
	c.dispatcherMap.Delete(target.GetId())

	err := c.ds.RemovePath(target.GetId())
	if err != nil {
		log.Error("remove dispatcher from dynamic stream failed", zap.Error(err))
	}
}

func (c *EventCollector) WakeDispatcher(dispatcherID common.DispatcherID) {
	c.ds.Wake(dispatcherID) <- dispatcherID
}

func (c *EventCollector) ResetDispatcherStat(stat *DispatcherStat) {
	stat.reset()
	stat.resetDispatcher(c)
}

func (c *EventCollector) addDispatcherRequestToSendingQueue(serverId node.ID, topic string, req DispatcherRequest) {
	c.dispatcherRequestChan.In() <- TargetAndDispatcherRequest{
		Target: serverId,
		Topic:  topic,
		Req:    req,
	}
}

func (c *EventCollector) processFeedback(ctx context.Context) {
	defer c.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case feedback := <-c.ds.Feedback():
			if feedback.Pause {
				feedback.Dest.pauseDispatcher(c)
			} else {
				feedback.Dest.resumeDispatcher(c)
			}
		}
	}
}

func (c *EventCollector) processDispatcherRequests(ctx context.Context) {
	defer c.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-c.dispatcherRequestChan.Out():
			if err := c.mustSendDispatcherRequest(req.Target, req.Topic, req.Req); err != nil {
				log.Error("failed to process dispatcher action", zap.Error(err))
				// Sleep a short time to avoid too many requests in a short time.
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

func (c *EventCollector) processLogCoordinatorRequest(ctx context.Context) {
	defer c.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-c.logCoordinatorRequestChan.Out():
			c.coordinatorInfo.RLock()
			targetMessage := messaging.NewSingleTargetMessage(c.coordinatorInfo.id, logCoordinatorTopic, req)
			c.coordinatorInfo.RUnlock()
			err := c.mc.SendCommand(targetMessage)
			if err != nil {
				log.Info("fail to send dispatcher request message to log coordinator, try again later", zap.Error(err))
				c.logCoordinatorRequestChan.In() <- req
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

// mustSendDispatcherRequest will keep retrying to send the dispatcher request to EventService until it succeed.
// Caller should avoid to use this method if the remote EventService maybe offline forever.
// And this method may be deprecated in the future.
func (c *EventCollector) mustSendDispatcherRequest(target node.ID, topic string, req DispatcherRequest) error {
	message := &messaging.RegisterDispatcherRequest{
		RegisterDispatcherRequest: &eventpb.RegisterDispatcherRequest{
			ChangefeedId: req.Dispatcher.GetChangefeedID().ToPB(),
			DispatcherId: req.Dispatcher.GetId().ToPB(),
			ActionType:   req.ActionType,
			// FIXME: It can be another server id in the future.
			ServerId:  c.serverId.String(),
			TableSpan: req.Dispatcher.GetTableSpan(),
			StartTs:   req.StartTs,
			OnlyReuse: req.OnlyUse,
		},
	}

	// If the action type is register, we need fill all config related fields.
	if req.ActionType == eventpb.ActionType_ACTION_TYPE_REGISTER ||
		req.ActionType == eventpb.ActionType_ACTION_TYPE_RESET {
		message.RegisterDispatcherRequest.FilterConfig = req.Dispatcher.GetFilterConfig()
		message.RegisterDispatcherRequest.EnableSyncPoint = req.Dispatcher.EnableSyncPoint()
		message.RegisterDispatcherRequest.SyncPointInterval = uint64(req.Dispatcher.GetSyncPointInterval().Seconds())
		message.RegisterDispatcherRequest.SyncPointTs = syncpoint.CalculateStartSyncPointTs(req.StartTs, req.Dispatcher.GetSyncPointInterval())
	}

	err := c.mc.SendCommand(&messaging.TargetMessage{
		To:      target,
		Topic:   eventServiceTopic,
		Type:    typeRegisterDispatcherReq,
		Message: []messaging.IOTypeT{message},
	})

	if err != nil {
		log.Info("failed to send dispatcher request message to event service, try again later",
			zap.Stringer("target", target),
			zap.Error(err))
		// Put the request back to the channel for later retry.
		c.dispatcherRequestChan.In() <- TargetAndDispatcherRequest{
			Target: target,
			Topic:  topic,
			Req:    req,
		}
		return err
	}
	return nil
}

// RecvEventsMessage is the handler for the events message from EventService.
func (c *EventCollector) RecvEventsMessage(_ context.Context, targetMessage *messaging.TargetMessage) error {
	inflightDuration := time.Since(time.UnixMilli(targetMessage.CreateAt)).Milliseconds()
	c.metricReceiveEventLagDuration.Observe(float64(inflightDuration))
	start := time.Now()
	for _, msg := range targetMessage.Message {
		switch msg.(type) {
		case *common.LogCoordinatorBroadcastRequest:
			c.coordinatorInfo.Lock()
			c.coordinatorInfo.id = targetMessage.From
			c.coordinatorInfo.Unlock()
		case *logservicepb.ReusableEventServiceResponse:
			// TODO: can we handle it here?
			value, ok := c.dispatcherMap.Load(msg.(*logservicepb.ReusableEventServiceResponse).ID)
			if !ok {
				continue
			}
			value.(*DispatcherStat).setRemoteCandidates(msg.(*logservicepb.ReusableEventServiceResponse).Nodes, c)
		case commonEvent.Event:
			event := msg.(commonEvent.Event)
			switch event.GetType() {
			case commonEvent.TypeBatchResolvedEvent:
				for _, e := range event.(*commonEvent.BatchResolvedEvent).Events {
					c.metricDispatcherReceivedResolvedTsEventCount.Inc()
					c.ds.In(e.DispatcherID) <- dispatcher.NewDispatcherEvent(targetMessage.From, e)
				}
			default:
				c.metricDispatcherReceivedKVEventCount.Inc()
				c.ds.In(event.GetDispatcherID()) <- dispatcher.NewDispatcherEvent(targetMessage.From, event)
			}
		default:
			log.Panic("invalid message type", zap.Any("msg", msg))
		}
	}
	handleEventDuration.Observe(float64(time.Since(start).Milliseconds()))
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
	c.dispatcherMap.Range(func(key, value interface{}) bool {
		if stat, ok := value.(*DispatcherStat); ok {
			d := stat.target
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

type DispatcherStat struct {
	dispatcherID common.DispatcherID
	target       dispatcher.EventDispatcher

	eventServiceInfo struct {
		sync.RWMutex
		// the server this dispatcher is currently connected to(except local event service)
		// if it is set to local event service id, ignore all messages from other event service
		serverID node.ID
		// whether has received ready signal from `serverID`
		readyEventReceived bool
		// the remote event services which may contain data this dispatcher needed
		remoteCandiates []node.ID
	}

	// lastEventSeq is the sequence number of the last received DML/DDL event.
	// It is used to ensure the order of events.
	lastEventSeq atomic.Uint64

	// waitHandshake is used to indicate whether the dispatcher is waiting for handshake event.
	// If true, the dispatcher will drop all data events it received.
	waitHandshake atomic.Bool

	// the largest commit ts that has been sent to the dispatcher.
	sendCommitTs atomic.Uint64
}

func (d *DispatcherStat) reset() {
	if d.waitHandshake.Load() {
		return
	}
	d.lastEventSeq.Store(0)
	d.waitHandshake.Store(true)
}

func (d *DispatcherStat) checkEventSeq(event dispatcher.DispatcherEvent, eventCollector *EventCollector) bool {
	switch event.GetType() {
	case commonEvent.TypeDMLEvent,
		commonEvent.TypeDDLEvent,
		commonEvent.TypeHandshakeEvent:
		expectedSeq := d.lastEventSeq.Add(1)
		if event.GetSeq() != expectedSeq {
			log.Warn("Received an out-of-order event, reset the dispatcher",
				zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
				zap.Stringer("dispatcher", d.target.GetId()),
				zap.Uint64("receivedSeq", event.GetSeq()),
				zap.Uint64("expectedSeq", expectedSeq),
				zap.Uint64("commitTs", event.GetCommitTs()))
			d.reset()
			eventCollector.addDispatcherRequestToSendingQueue(d.eventServiceInfo.serverID, eventServiceTopic, DispatcherRequest{
				Dispatcher: d.target,
				StartTs:    d.sendCommitTs.Load(),
				ActionType: eventpb.ActionType_ACTION_TYPE_RESET,
			})
			log.Info("reset dispatcher",
				zap.Stringer("dispatcher", d.target.GetId()),
				zap.Uint64("startTs", d.sendCommitTs.Load()))
			return false
		}
		return true
	default:
		return true
	}
}

func (d *DispatcherStat) shouldIgnoreDataEvent(event dispatcher.DispatcherEvent, eventCollector *EventCollector) bool {
	if d.eventServiceInfo.serverID != event.From {
		// TODO: unregister from this invalid event service if it send events for a long time
		return true
	}
	if d.waitHandshake.Load() {
		log.Warn("Receive event before handshake event, ignore it",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Stringer("dispatcher", d.target.GetId()),
			zap.Any("event", event))
		return true
	}
	if !d.checkEventSeq(event, eventCollector) {
		return true
	}
	// Note: a commit ts may have multiple transactions.
	// it is ok to send the same txn multiple times?
	// (we just want to avoid send old dml after new ddl)
	if event.GetCommitTs() < d.sendCommitTs.Load() {
		log.Warn("Receive resolved event before sendCommitTs, ignore it",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Stringer("dispatcher", d.target.GetId()),
			zap.Uint64("sendCommitTs", d.sendCommitTs.Load()),
			zap.Any("event", event))
		return true
	}
	d.sendCommitTs.Store(event.GetCommitTs())
	return false
}

func (d *DispatcherStat) handleHandshakeEvent(event dispatcher.DispatcherEvent, eventCollector *EventCollector) {
	d.eventServiceInfo.Lock()
	defer d.eventServiceInfo.Unlock()
	if event.GetType() != commonEvent.TypeHandshakeEvent {
		log.Panic("should not happen")
	}
	if d.eventServiceInfo.serverID == "" {
		log.Panic("should not happen: not server ID set")
	}
	if d.eventServiceInfo.serverID != event.From {
		// check invariant: if the handshake event is not from the current event service, we must be reading from local event service.
		if d.eventServiceInfo.serverID != eventCollector.serverId {
			log.Panic("receive handshake event from remote event service, but current event service is not local event service",
				zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
				zap.Stringer("dispatcher", d.target.GetId()),
				zap.Stringer("from", event.From))
		}
		return
	}
	if !d.checkEventSeq(event, eventCollector) {
		return
	}
	d.waitHandshake.Store(false)
	d.target.SetInitialTableInfo(event.Event.(*commonEvent.HandshakeEvent).TableInfo)
}

func (d *DispatcherStat) handleReadyEvent(event dispatcher.DispatcherEvent, eventCollector *EventCollector) {
	d.eventServiceInfo.Lock()
	defer d.eventServiceInfo.Unlock()
	if event.GetType() != commonEvent.TypeReadyEvent {
		log.Panic("should not happen")
	}
	server := event.From
	if d.eventServiceInfo.serverID == server {
		// case 1: already received ready signal from the same server
		if d.eventServiceInfo.readyEventReceived {
			return
		}
		// case 2: first ready signal from the server
		// (must be a remote candidate, because we won't set d.eventServiceInfo.serverID to local event service until we receive ready signal)
		d.eventServiceInfo.serverID = server
		d.eventServiceInfo.readyEventReceived = true
		eventCollector.addDispatcherRequestToSendingQueue(
			server,
			eventServiceTopic,
			DispatcherRequest{
				Dispatcher: d.target,
				StartTs:    d.sendCommitTs.Load(),
				ActionType: eventpb.ActionType_ACTION_TYPE_RESET,
			},
		)
	} else if server == eventCollector.serverId {
		// case 3: received first ready signal from local event service
		if d.eventServiceInfo.serverID != "" {
			eventCollector.addDispatcherRequestToSendingQueue(
				d.eventServiceInfo.serverID,
				eventServiceTopic,
				DispatcherRequest{
					Dispatcher: d.target,
					ActionType: eventpb.ActionType_ACTION_TYPE_REMOVE,
				},
			)
		}
		d.eventServiceInfo.serverID = server
		d.eventServiceInfo.readyEventReceived = true
		d.eventServiceInfo.remoteCandiates = nil
		eventCollector.addDispatcherRequestToSendingQueue(
			server,
			eventServiceTopic,
			DispatcherRequest{
				Dispatcher: d.target,
				StartTs:    d.sendCommitTs.Load(),
				ActionType: eventpb.ActionType_ACTION_TYPE_RESET,
			},
		)
	} else {
		log.Panic("should not happen: we have received ready signal from other remote server",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Stringer("dispatcher", d.target.GetId()),
			zap.Stringer("newRemote", server),
			zap.Stringer("oldRemote", d.eventServiceInfo.serverID))
	}
}

func (d *DispatcherStat) handleNotReusableEvent(event dispatcher.DispatcherEvent, eventCollector *EventCollector) {
	d.eventServiceInfo.Lock()
	defer d.eventServiceInfo.Unlock()
	if event.GetType() != commonEvent.TypeNotReusableEvent {
		log.Panic("should not happen")
	}
	if event.From == d.eventServiceInfo.serverID {
		if len(d.eventServiceInfo.remoteCandiates) > 0 {
			eventCollector.addDispatcherRequestToSendingQueue(
				d.eventServiceInfo.remoteCandiates[0],
				eventServiceTopic,
				DispatcherRequest{
					Dispatcher: d.target,
					StartTs:    d.target.GetStartTs(),
					ActionType: eventpb.ActionType_ACTION_TYPE_REGISTER,
					OnlyUse:    true,
				},
			)
			d.eventServiceInfo.serverID = d.eventServiceInfo.remoteCandiates[0]
			d.eventServiceInfo.remoteCandiates = d.eventServiceInfo.remoteCandiates[1:]
		}
	}
}

func (d *DispatcherStat) unregisterDispatcher(eventCollector *EventCollector) {
	d.eventServiceInfo.RLock()
	defer d.eventServiceInfo.RUnlock()
	// must unregister from local event service
	eventCollector.mustSendDispatcherRequest(eventCollector.serverId, eventServiceTopic, DispatcherRequest{
		Dispatcher: d.target,
		ActionType: eventpb.ActionType_ACTION_TYPE_REMOVE,
	})
	// unregister from remote event service if have
	if d.eventServiceInfo.serverID != eventCollector.serverId {
		eventCollector.mustSendDispatcherRequest(d.eventServiceInfo.serverID, eventServiceTopic, DispatcherRequest{
			Dispatcher: d.target,
			ActionType: eventpb.ActionType_ACTION_TYPE_REMOVE,
		})
	}
}

func (d *DispatcherStat) resetDispatcher(eventCollector *EventCollector) {
	d.eventServiceInfo.RLock()
	defer d.eventServiceInfo.RUnlock()

	if d.eventServiceInfo.serverID == "" || !d.eventServiceInfo.readyEventReceived {
		log.Panic("should not happen: reset dispatcher before receiving ready signal")
	}

}

func (d *DispatcherStat) pauseDispatcher(eventCollector *EventCollector) {
	d.eventServiceInfo.RLock()
	defer d.eventServiceInfo.RUnlock()

	if d.eventServiceInfo.serverID == "" || !d.eventServiceInfo.readyEventReceived {
		log.Panic("should not happen: pause dispatcher before receiving ready signal")
	}

	eventCollector.addDispatcherRequestToSendingQueue(d.eventServiceInfo.serverID, eventServiceTopic, DispatcherRequest{
		Dispatcher: d.target,
		ActionType: eventpb.ActionType_ACTION_TYPE_PAUSE,
	})
}

func (d *DispatcherStat) resumeDispatcher(eventCollector *EventCollector) {
	d.eventServiceInfo.RLock()
	defer d.eventServiceInfo.RUnlock()

	if d.eventServiceInfo.serverID == "" || !d.eventServiceInfo.readyEventReceived {
		log.Panic("should not happen: resume dispatcher before receiving ready signal")
	}

	eventCollector.addDispatcherRequestToSendingQueue(d.eventServiceInfo.serverID, eventServiceTopic, DispatcherRequest{
		Dispatcher: d.target,
		ActionType: eventpb.ActionType_ACTION_TYPE_RESUME,
	})
}

// TODO: better name
func (d *DispatcherStat) setRemoteCandidates(nodes []string, eventCollector *EventCollector) {
	if len(nodes) == 0 {
		return
	}
	d.eventServiceInfo.RLock()
	defer d.eventServiceInfo.RUnlock()
	// reading from a event service or checking remotes already, ignore
	if d.eventServiceInfo.serverID != "" {
		return
	}
	d.eventServiceInfo.serverID = node.ID(nodes[0])
	for i := 1; i < len(nodes); i++ {
		d.eventServiceInfo.remoteCandiates = append(d.eventServiceInfo.remoteCandiates, node.ID(nodes[i]))
	}

	eventCollector.addDispatcherRequestToSendingQueue(
		d.eventServiceInfo.serverID,
		eventServiceTopic,
		DispatcherRequest{
			Dispatcher: d.target,
			StartTs:    d.target.GetStartTs(),
			ActionType: eventpb.ActionType_ACTION_TYPE_REGISTER,
			OnlyUse:    true,
		},
	)
}
