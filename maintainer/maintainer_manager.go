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

	"github.com/flowbehappy/tigate/heartbeatpb"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/utils/dynstream"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
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
	regionCache *tikv.RegionCache

	msgCh chan *messaging.TargetMessage

	stream        dynstream.DynamicStream[string, *Event, *Maintainer]
	taskScheduler threadpool.ThreadPool
}

// NewMaintainerManager create a changefeed maintainer manager instance,
// 1. manager receives bootstrap command from coordinator
// 2. manager manages maintainer lifetime
// 3. manager report maintainer status to coordinator
func NewMaintainerManager(selfNode *node.Info,
	conf *config.SchedulerConfig,
	pdAPI pdutil.PDAPIClient,
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
		regionCache:   regionCache,
	}
	m.stream = dynstream.NewDynamicStream[string, *Event, *Maintainer](NewStreamHandler())
	m.stream.Start()
	mc.RegisterHandler(messaging.MaintainerManagerTopic, m.recvMessages)

	mc.RegisterHandler(messaging.MaintainerTopic,
		func(ctx context.Context, msg *messaging.TargetMessage) error {
			req := msg.Message[0].(*heartbeatpb.MaintainerCloseResponse)
			return m.dispatcherMaintainerMessage(ctx, req.ChangefeedID, msg)
		})
	return m
}

func (m *Manager) recvMessages(ctx context.Context, msg *messaging.TargetMessage) error {
	switch msg.Type {
	// receive message from coordinator
	case messaging.TypeAddMaintainerRequest, messaging.TypeRemoveMaintainerRequest:
		fallthrough
	case messaging.TypeCoordinatorBootstrapRequest:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m.msgCh <- msg:
		}
		return nil
	// receive bootstrap response message from the dispatcher manager
	case messaging.TypeMaintainerBootstrapResponse:
		req := msg.Message[0].(*heartbeatpb.MaintainerBootstrapResponse)
		return m.dispatcherMaintainerMessage(ctx, req.ChangefeedID, msg)
	// receive heartbeat message from dispatchers
	case messaging.TypeHeartBeatRequest:
		req := msg.Message[0].(*heartbeatpb.HeartBeatRequest)
		return m.dispatcherMaintainerMessage(ctx, req.ChangefeedID, msg)
	case messaging.TypeBlockStatusRequest:
		req := msg.Message[0].(*heartbeatpb.BlockStatusRequest)
		return m.dispatcherMaintainerMessage(ctx, req.ChangefeedID, msg)
	case messaging.TypeCheckpointTsMessage:
		req := msg.Message[0].(*heartbeatpb.CheckpointTsMessage)
		return m.dispatcherMaintainerMessage(ctx, req.ChangefeedID, msg)
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
					m.maintainers.Delete(key)
					log.Info("maintainer removed, remove it from dynamic stream",
						zap.String("changefeed", cf.id.String()))
					m.stream.RemovePaths(cf.id.ID)
				}
				return true
			})
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
	cfID := model.DefaultChangeFeedID(req.GetId())
	cf, ok := m.maintainers.Load(cfID)
	if ok {
		return
	}

	cfConfig := &config.ChangeFeedInfo{}
	err := json.Unmarshal(req.Config, cfConfig)
	if err != nil {
		log.Panic("decode changefeed fail", zap.Error(err))
	}
	cf = NewMaintainer(cfID, m.conf, cfConfig, m.selfNode, m.stream, m.taskScheduler,
		m.pdAPI, m.regionCache,
		req.CheckpointTs)
	err = m.stream.AddPath(cfID.ID, cf.(*Maintainer))
	if err != nil {
		log.Warn("add path to dynstream failed, coordinator will retry later", zap.Error(err))
		return
	}
	m.maintainers.Store(cfID, cf)
	m.stream.In() <- &Event{changefeedID: cfID.ID, eventType: EventInit}
}

func (m *Manager) onRemoveMaintainerRequest(msg *messaging.TargetMessage) *heartbeatpb.MaintainerStatus {
	req := msg.Message[0].(*heartbeatpb.RemoveMaintainerRequest)
	cfID := model.DefaultChangeFeedID(req.GetId())
	_, ok := m.maintainers.Load(cfID)
	if !ok {
		log.Warn("ignore remove maintainer request, "+
			"since the maintainer not found",
			zap.String("changefeed", cfID.String()),
			zap.Any("request", req))
		return &heartbeatpb.MaintainerStatus{
			ChangefeedID: req.GetId(),
			State:        heartbeatpb.ComponentState_Stopped,
		}
	}
	log.Info("received remove maintainer request",
		zap.String("changefeed", cfID.String()))
	m.stream.In() <- &Event{
		changefeedID: cfID.ID,
		eventType:    EventMessage,
		message:      msg,
	}
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
	case messaging.TypeAddMaintainerRequest, messaging.TypeRemoveMaintainerRequest:
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
	ctx context.Context, changefeed string, msg *messaging.TargetMessage,
) error {
	_, ok := m.maintainers.Load(model.DefaultChangeFeedID(changefeed))
	if !ok {
		log.Warn("maintainer is not found",
			zap.String("changefeedID", changefeed), zap.String("message", msg.String()))
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.stream.In() <- &Event{
		changefeedID: changefeed,
		eventType:    EventMessage,
		message:      msg,
	}:
	default:
		log.Warn("maintainer is busy", zap.String("changefeed", changefeed))
	}
	return nil
}
