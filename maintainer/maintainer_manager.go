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
	"github.com/flowbehappy/tigate/pkg/messaging"
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

	selfServerID messaging.ServerId
	pdEndpoints  []string

	msgCh chan *messaging.TargetMessage
}

// NewMaintainerManager create a changefeed maintainer manager instance,
// 1. manager receives bootstrap command from coordinator
// 2. manager manages maintainer lifetime
// 3. manager report maintainer status to coordinator
func NewMaintainerManager(selfServerID messaging.ServerId, pdEndpoints []string) *Manager {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	m := &Manager{
		mc:           mc,
		maintainers:  sync.Map{},
		selfServerID: selfServerID,
		pdEndpoints:  pdEndpoints,
		msgCh:        make(chan *messaging.TargetMessage, 1024),
	}

	mc.RegisterHandler(messaging.MaintainerManagerTopic, m.RecvMessages)

	mc.RegisterHandler(messaging.MaintainerTopic,
		func(ctx context.Context, msg *messaging.TargetMessage) error {
			req := msg.Message.(*heartbeatpb.MaintainerCloseResponse)
			changefeedID := model.DefaultChangeFeedID(req.ChangefeedID)
			v, ok := m.maintainers.Load(changefeedID)
			if !ok {
				log.Warn("maintainer is not found",
					zap.Stringer("changefeedID", changefeedID), zap.String("message", msg.String()))
				return nil
			}

			maintainer := v.(*Maintainer)
			maintainer.onNodeClosed(msg.From.String(), req)
			return nil
		})

	return m
}

func (m *Manager) RecvMessages(ctx context.Context, msg *messaging.TargetMessage) error {
	switch msg.Type {
	// receive message from coordinator
	case messaging.TypeDispatchMaintainerRequest:
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
) []string {
	request := msg.Message.(*heartbeatpb.DispatchMaintainerRequest)
	if m.coordinatorID != msg.From {
		log.Warn("ignore invalid coordinator id",
			zap.Any("request", request),
			zap.Any("coordinator", msg.From))
		return nil
	}
	absent := make([]string, 0)
	for _, req := range request.AddMaintainers {
		cfID := model.DefaultChangeFeedID(req.GetId())
		cf, ok := m.maintainers.Load(cfID)
		if !ok {
			cfConfig := &model.ChangeFeedInfo{}
			err := json.Unmarshal(req.Config, cfConfig)
			if err != nil {
				log.Panic("decode changefeed fail", zap.Error(err))
			}
			cf = NewMaintainer(cfID, req.IsSecondary,
				cfConfig, req.CheckpointTs, m.pdEndpoints)
			m.maintainers.Store(cfID, cf)
			cf.(*Maintainer).Run()
		}
		cf.(*Maintainer).isSecondary.Store(req.IsSecondary)
	}

	for _, req := range request.RemoveMaintainers {
		cfID := model.DefaultChangeFeedID(req.GetId())
		cf, ok := m.maintainers.Load(cfID)
		if !ok {
			log.Warn("ignore remove maintainer request, "+
				"since the maintainer not found",
				zap.String("changefeed", cfID.String()),
				zap.Any("request", req))
			absent = append(absent, req.GetId())
			continue
		}
		cf.(*Maintainer).removing.Store(true)
		cf.(*Maintainer).cascadeRemoving.Store(req.Cascade)
	}
	return absent
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
		m.onCoordinatorBootstrapRequest(msg)
	case messaging.TypeDispatchMaintainerRequest:
		absent := m.onDispatchMaintainerRequest(msg)
		if m.coordinatorVersion > 0 {
			response := &heartbeatpb.MaintainerHeartbeat{}
			for _, id := range absent {
				response.Statuses = append(response.Statuses, &heartbeatpb.MaintainerStatus{
					ChangefeedID: id,
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
	v, ok := m.maintainers.Load(model.DefaultChangeFeedID(changefeed))
	if !ok {
		log.Warn("maintainer is not found",
			zap.String("changefeedID", changefeed), zap.String("message", msg.String()))
		return nil
	}

	maintainer := v.(*Maintainer)
	if maintainer.isSecondary.Load() || maintainer.removing.Load() {
		return nil
	}
	return maintainer.getMessageQueue().Push(ctx, msg)
}
