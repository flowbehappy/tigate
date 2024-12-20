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

package maintainer

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/pingcap/ticdc/utils/threadpool"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

var (
	metricsDSInputChanLen    = metrics.DynamicStreamEventChanSize.WithLabelValues("maintainer-manager")
	metricsDSPendingQueueLen = metrics.DynamicStreamPendingQueueLen.WithLabelValues("maintainer-manager")
)

// Manager is the manager of all changefeed maintainer in a ticdc watcher, each ticdc watcher will
// start a Manager when the watcher is startup. the Manager should:
// 1. handle bootstrap command from coordinator and return all changefeed maintainer status
// 2. handle dispatcher command from coordinator: add or remove changefeed maintainer
// 3. check maintainer liveness
type Manager struct {
	mc   messaging.MessageCenter
	conf *config.SchedulerConfig

	// changefeedID -> maintainer
	maintainers sync.Map

	coordinatorID      node.ID
	coordinatorVersion int64

	selfNode    *node.Info
	pdAPI       pdutil.PDAPIClient
	tsoClient   replica.TSOClient
	regionCache *tikv.RegionCache

	msgCh chan *messaging.TargetMessage

	stream        dynstream.DynamicStream[int, common.GID, *Event, *Maintainer, *StreamHandler]
	taskScheduler threadpool.ThreadPool
}

// NewMaintainerManager create a changefeed maintainer manager instance,
// 1. manager receives bootstrap command from coordinator
// 2. manager manages maintainer lifetime
// 3. manager report maintainer status to coordinator
func NewMaintainerManager(selfNode *node.Info,
	conf *config.SchedulerConfig,
	pdAPI pdutil.PDAPIClient,
	pdClient replica.TSOClient,
	regionCache *tikv.RegionCache,
) *Manager {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	m := &Manager{
		mc:            mc,
		conf:          conf,
		maintainers:   sync.Map{},
		selfNode:      selfNode,
		msgCh:         make(chan *messaging.TargetMessage, 1024),
		taskScheduler: threadpool.NewThreadPoolDefault(),
		pdAPI:         pdAPI,
		tsoClient:     pdClient,
		regionCache:   regionCache,
	}
	m.stream = dynstream.NewDynamicStream(NewStreamHandler())
	m.stream.Start()

	mc.RegisterHandler(messaging.MaintainerManagerTopic, m.recvMessages)
	mc.RegisterHandler(messaging.MaintainerTopic,
		func(ctx context.Context, msg *messaging.TargetMessage) error {
			req := msg.Message[0].(*heartbeatpb.MaintainerCloseResponse)
			return m.dispatcherMaintainerMessage(ctx, common.NewChangefeedIDFromPB(req.ChangefeedID), msg)
		})
	return m
}

// recvMessages is the message handler for maintainer manager
func (m *Manager) recvMessages(ctx context.Context, msg *messaging.TargetMessage) error {
	switch msg.Type {
	// Coordinator related messages
	case messaging.TypeAddMaintainerRequest,
		messaging.TypeRemoveMaintainerRequest,
		messaging.TypeCoordinatorBootstrapRequest:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m.msgCh <- msg:
		}
		return nil
	// receive bootstrap response message from the dispatcher manager
	case messaging.TypeMaintainerBootstrapResponse:
		req := msg.Message[0].(*heartbeatpb.MaintainerBootstrapResponse)
		return m.dispatcherMaintainerMessage(ctx, common.NewChangefeedIDFromPB(req.ChangefeedID), msg)
	case messaging.TypeMaintainerPostBootstrapResponse:
		req := msg.Message[0].(*heartbeatpb.MaintainerPostBootstrapResponse)
		return m.dispatcherMaintainerMessage(ctx, common.NewChangefeedIDFromPB(req.ChangefeedID), msg)
	// receive heartbeat message from dispatchers
	case messaging.TypeHeartBeatRequest:
		req := msg.Message[0].(*heartbeatpb.HeartBeatRequest)
		return m.dispatcherMaintainerMessage(ctx, common.NewChangefeedIDFromPB(req.ChangefeedID), msg)
	case messaging.TypeBlockStatusRequest:
		req := msg.Message[0].(*heartbeatpb.BlockStatusRequest)
		return m.dispatcherMaintainerMessage(ctx, common.NewChangefeedIDFromPB(req.ChangefeedID), msg)
	case messaging.TypeCheckpointTsMessage:
		req := msg.Message[0].(*heartbeatpb.CheckpointTsMessage)
		return m.dispatcherMaintainerMessage(ctx, common.NewChangefeedIDFromPB(req.ChangefeedID), msg)
	default:
		log.Panic("unknown message type", zap.Any("message", msg.Message))
	}
	return nil
}

func (m *Manager) Name() string {
	return "maintainer-manager"
}

func (m *Manager) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond * 500)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-m.msgCh:
			m.handleMessage(msg)
		case <-ticker.C:
			//1.  try to send heartbeat to coordinator
			m.sendHeartbeat()
			//2. cleanup removed maintainers
			m.maintainers.Range(func(key, value interface{}) bool {
				cf := value.(*Maintainer)
				if cf.removed.Load() {
					cf.Close()
					log.Info("maintainer removed, remove it from dynamic stream",
						zap.String("changefeed", cf.id.String()))
					if err := m.stream.RemovePath(cf.id.Id); err != nil {
						log.Warn("remove path from dynstream failed, will retry later",
							zap.String("changefeed", cf.id.String()),
							zap.Error(err))
						// try it again later
						return true
					}
					m.maintainers.Delete(key)
				}
				return true
			})
			m.updateMetricsOnce()
		}
	}
}

func (m *Manager) newCoordinatorTopicMessage(msg messaging.IOTypeT) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(
		m.coordinatorID,
		messaging.CoordinatorTopic,
		msg,
	)
}

func (m *Manager) sendMessages(msg *heartbeatpb.MaintainerHeartbeat) {
	target := m.newCoordinatorTopicMessage(msg)
	err := m.mc.SendCommand(target)
	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
}

// Close closes, it's a block call
func (m *Manager) Close(_ context.Context) error {
	return nil
}

func (m *Manager) onCoordinatorBootstrapRequest(msg *messaging.TargetMessage) {
	req := msg.Message[0].(*heartbeatpb.CoordinatorBootstrapRequest)
	if m.coordinatorVersion > req.Version {
		log.Warn("ignore invalid coordinator version",
			zap.Int64("coordinatorVersion", m.coordinatorVersion),
			zap.Int64("version", req.Version))
		return
	}
	m.coordinatorID = msg.From
	m.coordinatorVersion = req.Version

	response := &heartbeatpb.CoordinatorBootstrapResponse{}
	m.maintainers.Range(func(key, value interface{}) bool {
		maintainer := value.(*Maintainer)
		response.Statuses = append(response.Statuses, maintainer.GetMaintainerStatus())
		maintainer.statusChanged.Store(false)
		maintainer.lastReportTime = time.Now()
		return true
	})

	msg = m.newCoordinatorTopicMessage(response)
	err := m.mc.SendCommand(msg)
	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
	log.Info("new coordinator online",
		zap.Int64("version", m.coordinatorVersion))
}

func (m *Manager) onAddMaintainerRequest(req *heartbeatpb.AddMaintainerRequest) {
	cfID := common.NewChangefeedIDFromPB(req.Id)
	_, ok := m.maintainers.Load(cfID)
	if ok {
		return
	}

	cfConfig := &config.ChangeFeedInfo{}
	err := json.Unmarshal(req.Config, cfConfig)
	if err != nil {
		log.Panic("decode changefeed fail", zap.Error(err))
	}
	if req.CheckpointTs == 0 {
		log.Panic("add maintainer with invalid checkpointTs",
			zap.Stringer("changefeed", cfID),
			zap.Uint64("checkpointTs", req.CheckpointTs),
			zap.Any("config", cfConfig))
	}
	cf := NewMaintainer(cfID, m.conf, cfConfig, m.selfNode, m.stream, m.taskScheduler,
		m.pdAPI, m.tsoClient, m.regionCache, req.CheckpointTs)
	err = m.stream.AddPath(cfID.Id, cf)
	if err != nil {
		log.Warn("add path to dynstream failed, coordinator will retry later", zap.Error(err))
		return
	}
	m.maintainers.Store(cfID, cf)
	m.stream.Push(cfID.Id, &Event{changefeedID: cfID, eventType: EventInit})
}

func (m *Manager) onRemoveMaintainerRequest(msg *messaging.TargetMessage) *heartbeatpb.MaintainerStatus {
	req := msg.Message[0].(*heartbeatpb.RemoveMaintainerRequest)
	cfID := common.NewChangefeedIDFromPB(req.GetId())
	_, ok := m.maintainers.Load(cfID)
	if !ok {
		if !req.Cascade {
			log.Warn("ignore remove maintainer request, "+
				"since the maintainer not found",
				zap.String("changefeed", cfID.String()),
				zap.Any("request", req))
			return &heartbeatpb.MaintainerStatus{
				ChangefeedID: req.GetId(),
				State:        heartbeatpb.ComponentState_Stopped,
			}
		}
		// it's cascade remove, we should remove the dispatcher from all node
		// here we create a maintainer to run the remove the dispatcher logic
		cf := NewMaintainerForRemove(cfID, m.conf, m.selfNode, m.stream, m.taskScheduler, m.pdAPI,
			m.tsoClient, m.regionCache)
		err := m.stream.AddPath(cfID.Id, cf)
		if err != nil {
			log.Warn("add path to dynstream failed, coordinator will retry later", zap.Error(err))
			return nil
		}
		m.maintainers.Store(cfID, cf)
	}
	log.Info("received remove maintainer request",
		zap.String("changefeed", cfID.String()))
	m.stream.Push(cfID.Id, &Event{
		changefeedID: cfID,
		eventType:    EventMessage,
		message:      msg,
	})
	return nil
}

func (m *Manager) onDispatchMaintainerRequest(
	msg *messaging.TargetMessage,
) *heartbeatpb.MaintainerStatus {
	if m.coordinatorID != msg.From {
		log.Warn("ignore invalid coordinator id",
			zap.Any("request", msg),
			zap.Any("coordinatorID", m.coordinatorID),
			zap.Any("from", msg.From))
		return nil
	}
	switch msg.Type {
	case messaging.TypeAddMaintainerRequest:
		req := msg.Message[0].(*heartbeatpb.AddMaintainerRequest)
		m.onAddMaintainerRequest(req)
	case messaging.TypeRemoveMaintainerRequest:
		return m.onRemoveMaintainerRequest(msg)
	default:
	}
	return nil
}

func (m *Manager) sendHeartbeat() {
	if m.coordinatorVersion > 0 {
		response := &heartbeatpb.MaintainerHeartbeat{}
		m.maintainers.Range(func(key, value interface{}) bool {
			cfMaintainer := value.(*Maintainer)
			if cfMaintainer.statusChanged.Load() || time.Since(cfMaintainer.lastReportTime) > time.Second*2 {
				response.Statuses = append(response.Statuses, cfMaintainer.GetMaintainerStatus())
				cfMaintainer.statusChanged.Store(false)
				cfMaintainer.lastReportTime = time.Now()
			}
			return true
		})
		if len(response.Statuses) != 0 {
			m.sendMessages(response)
		}
	}
}

func (m *Manager) handleMessage(msg *messaging.TargetMessage) {
	switch msg.Type {
	case messaging.TypeCoordinatorBootstrapRequest:
		log.Info("received coordinator bootstrap request", zap.String("from", msg.From.String()))
		m.onCoordinatorBootstrapRequest(msg)
	case messaging.TypeAddMaintainerRequest,
		messaging.TypeRemoveMaintainerRequest:
		if m.coordinatorVersion > 0 {
			status := m.onDispatchMaintainerRequest(msg)
			if status == nil {
				return
			}
			response := &heartbeatpb.MaintainerHeartbeat{
				Statuses: []*heartbeatpb.MaintainerStatus{status},
			}
			m.sendMessages(response)
		}
	default:
	}
}

func (m *Manager) dispatcherMaintainerMessage(
	ctx context.Context, changefeed common.ChangeFeedID, msg *messaging.TargetMessage,
) error {
	_, ok := m.maintainers.Load(changefeed)
	if !ok {
		log.Warn("maintainer is not found",
			zap.String("changefeedID", changefeed.Name()), zap.String("message", msg.String()))
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// m.stream.Push(changefeed.Id, &Event{
		// 	changefeedID: changefeed,
		// 	eventType:    EventMessage,
		// 	message:      msg,
		// })
		c, ok := m.maintainers.Load(changefeed)
		if !ok {
			log.Warn("maintainer is not found",
				zap.String("changefeedID", changefeed.Name()), zap.String("message", msg.String()))
			return nil
		}
		maintainer := c.(*Maintainer)
		maintainer.eventCh.In() <- &Event{
			changefeedID: changefeed,
			eventType:    EventMessage,
			message:      msg,
		}
	}
	return nil
}

func (m *Manager) updateMetricsOnce() {
	dsMetrics := m.stream.GetMetrics()
	metricsDSInputChanLen.Set(float64(dsMetrics.EventChanSize))
	metricsDSPendingQueueLen.Set(float64(dsMetrics.PendingQueueLen))
}
