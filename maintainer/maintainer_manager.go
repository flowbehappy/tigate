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
	"github.com/flowbehappy/tigate/pkg/common"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/utils/dynstream"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// Manager is the manager of all changefeed maintainer in a ticdc watcher, each ticdc watcher will
// start a Manager when the watcher is startup. the Manager should:
// 1. handle bootstrap command from coordinator and return all changefeed maintainer status
// 2. handle dispatcher command from coordinator: add or remove changefeed maintainer
// 3. check maintainer liveness
type Manager struct {
	mc messaging.MessageCenter

	maintainers sync.Map

	coordinatorID      messaging.ServerId
	coordinatorVersion int64

	selfNode    *common.NodeInfo
	pdEndpoints []string

	msgCh chan *messaging.TargetMessage

	maintainerStream *MaintainerStream
}

// NewMaintainerManager create a changefeed maintainer manager instance,
// 1. manager receives bootstrap command from coordinator
// 2. manager manages maintainer lifetime
// 3. manager report maintainer status to coordinator
func NewMaintainerManager(selfNode *common.NodeInfo, pdEndpoints []string) *Manager {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	m := &Manager{
		mc:               mc,
		maintainers:      sync.Map{},
		selfNode:         selfNode,
		pdEndpoints:      pdEndpoints,
		msgCh:            make(chan *messaging.TargetMessage, 1024),
		maintainerStream: NewMaintainerStream(),
	}
	mc.RegisterHandler(messaging.MaintainerManagerTopic, m.RecvMessages)

	mc.RegisterHandler(messaging.MaintainerTopic,
		func(ctx context.Context, msg *messaging.TargetMessage) error {
			req := msg.Message.(*heartbeatpb.MaintainerCloseResponse)
			return m.dispatcherMaintainerMessage(ctx, req.ChangefeedID, msg)
		})

	return m
}

func (m *Manager) RecvMessages(ctx context.Context, msg *messaging.TargetMessage) error {
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
	// receive bootstrap response message from dispatcher manager manager
	case messaging.TypeMaintainerBootstrapResponse:
		req := msg.Message.(*heartbeatpb.MaintainerBootstrapResponse)
		return m.dispatcherMaintainerMessage(ctx, req.ChangefeedID, msg)
	// receive heartbeat message from dispatchers
	case messaging.TypeHeartBeatRequest:
		req := msg.Message.(*heartbeatpb.HeartBeatRequest)
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
	tick := time.NewTicker(time.Millisecond * 500)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-m.msgCh:
			m.handleMessage(msg)
		case <-tick.C:
			//1.  try to send heartbeat to coordinator
			m.sendHeartbeat()

			//2. cleanup removed maintainers
			m.maintainers.Range(func(key, value interface{}) bool {
				cf := value.(*Maintainer)
				if cf.removed.Load() {
					cf.Close()
					m.maintainers.Delete(key)
					m.maintainerStream.stream.RemovePath(cf.id.ID)
				}
				return true
			})
		}
	}
}

func (m *Manager) sendMessages(msg *heartbeatpb.MaintainerHeartbeat) {
	target := messaging.NewTargetMessage(
		m.coordinatorID,
		messaging.CoordinatorTopic,
		msg,
	)
	err := m.mc.SendCommand(target)
	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
}

// Close closes, it's a block call
func (m *Manager) Close(ctx context.Context) error {
	return nil
}

func (m *Manager) onCoordinatorBootstrapRequest(msg *messaging.TargetMessage) {
	req := msg.Message.(*heartbeatpb.CoordinatorBootstrapRequest)
	if m.coordinatorVersion > req.Version {
		log.Warn("ignore invalid coordinator version",
			zap.Int64("version", req.Version))
		return
	}
	m.coordinatorID = msg.From
	m.coordinatorVersion = req.Version

	response := &heartbeatpb.CoordinatorBootstrapResponse{}
	m.maintainers.Range(func(key, value interface{}) bool {
		m := value.(*Maintainer)
		response.Statuses = append(response.Statuses, m.GetMaintainerStatus())
		m.statusChanged.Store(false)
		m.lastReportTime = time.Now()
		return true
	})

	err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendCommand(messaging.NewTargetMessage(
		m.coordinatorID,
		messaging.CoordinatorTopic,
		response,
	))
	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
	log.Info("New coordinator online",
		zap.Int64("version", m.coordinatorVersion))
}

func (m *Manager) onDispatchMaintainerRequest(
	msg *messaging.TargetMessage,
) string {
	if m.coordinatorID != msg.From {
		log.Warn("ignore invalid coordinator id",
			zap.Any("request", msg),
			zap.Any("coordinator", msg.From))
		return ""
	}
	switch msg.Type {
	case messaging.TypeAddMaintainerRequest:
		req := msg.Message.(*heartbeatpb.AddMaintainerRequest)
		cfID := model.DefaultChangeFeedID(req.GetId())
		cf, ok := m.maintainers.Load(cfID)
		if !ok {
			cfConfig := &model.ChangeFeedInfo{}
			err := json.Unmarshal(req.Config, cfConfig)
			if err != nil {
				log.Panic("decode changefeed fail", zap.Error(err))
			}
			cf = NewMaintainer(cfID, cfConfig, m.selfNode, req.CheckpointTs, m.pdEndpoints)
			err = m.maintainerStream.stream.AddPath(dynstream.PathAndDest[string, *Maintainer]{
				Path: cfID.ID,
				Dest: cf.(*Maintainer),
			})
			if err != nil {
				log.Warn("add path to dynstream failed, coordinator will retry later",
					zap.Error(err))
				return ""
			}
			m.maintainers.Store(cfID, cf)
			m.maintainerStream.stream.In() <- &Event{cfID: cfID.ID, eventType: EventInit}
		}
	case messaging.TypeRemoveMaintainerRequest:
		req := msg.Message.(*heartbeatpb.RemoveMaintainerRequest)
		cfID := model.DefaultChangeFeedID(req.GetId())
		_, ok := m.maintainers.Load(cfID)
		if !ok {
			log.Warn("ignore remove maintainer request, "+
				"since the maintainer not found",
				zap.String("changefeed", cfID.String()),
				zap.Any("request", req))
			return req.GetId()
		}
		m.maintainerStream.stream.In() <- &Event{
			cfID:      cfID.ID,
			eventType: EventMessage,
			message:   msg,
		}
	}
	return ""
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
		absent := m.onDispatchMaintainerRequest(msg)
		if m.coordinatorVersion > 0 {
			response := &heartbeatpb.MaintainerHeartbeat{}
			if absent != "" {
				response.Statuses = append(response.Statuses, &heartbeatpb.MaintainerStatus{
					ChangefeedID: absent,
					State:        heartbeatpb.ComponentState_Absent,
				})
			}
			if len(response.Statuses) != 0 {
				m.sendMessages(response)
			}
		}
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
	case m.maintainerStream.stream.In() <- &Event{
		cfID:      changefeed,
		eventType: EventMessage,
		message:   msg,
	}:
	default:
		log.Warn("maintainer is busy", zap.String("changefeed", changefeed))
	}
	return nil
}
