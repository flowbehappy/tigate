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
	"github.com/flowbehappy/tigate/utils/threadpool"
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
	maintainers sync.Map

	msgLock sync.RWMutex

	coordinatorID      messaging.ServerId
	coordinatorVersion int64

	selfServerID messaging.ServerId
	pdEndpoints  []string

	msgQueue *MessageQueue
	msgBuf   []*messaging.TargetMessage
}

// NewMaintainerManager create a changefeed maintainer manager instance,
// 1. manager receives bootstrap command from coordinator
// 2. manager manages maintainer lifetime
// 3. manager report maintainer status to coordinator
func NewMaintainerManager(selfServerID messaging.ServerId, pdEndpoints []string) *Manager {
	m := &Manager{
		maintainers:  sync.Map{},
		selfServerID: selfServerID,
		pdEndpoints:  pdEndpoints,
		msgQueue:     NewMessageQueue(1024),
		msgBuf:       make([]*messaging.TargetMessage, 1024),
	}
	// receive message coordinator
	appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).
		RegisterHandler(messaging.MaintainerManagerTopic,
			func(msg *messaging.TargetMessage) error {
				m.msgQueue.Push(msg)
				return nil
			})

	// receive bootstrap response message for maintainer
	appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).
		RegisterHandler(messaging.MaintainerBootstrapResponseTopic,
			func(msg *messaging.TargetMessage) error {
				req := msg.Message.(*heartbeatpb.MaintainerBootstrapResponse)
				m.cacheMaintainerMessage(req.ChangefeedID, msg)
				return nil
			})

	// receive heartbeat message for maintainer
	appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).
		RegisterHandler(messaging.DispatcherHeartBeatRequestTopic,
			func(msg *messaging.TargetMessage) error {
				req := msg.Message.(*heartbeatpb.HeartBeatRequest)
				m.cacheMaintainerMessage(req.ChangefeedID, msg)
				return nil
			})

	return m
}

func (m *Manager) Name() string {
	return "maintainer-manager"
}

func (m *Manager) Run(ctx context.Context) error {
	tick := time.NewTicker(time.Millisecond * 50)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			// 1. handle message from remote
			absent := m.handleMessages()

			//2.  try to send heartbeat to coordinator
			m.sendHeartbeat(absent)

			//3. cleanup removed maintainers
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
		"coordinator",
		msg,
	)
	err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendCommand(target)
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
	log.Info("bootstrap coordinator",
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
				cfConfig, req.Config, req.CheckpointTs, m.pdEndpoints)
			m.maintainers.Store(cfID, cf)
			threadpool.GetTaskSchedulerInstance().MaintainerTaskScheduler.Submit(cf.(*Maintainer),
				threadpool.CPUTask, time.Now())
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
		}
		cf.(*Maintainer).removing.Store(true)
		cf.(*Maintainer).cascadeRemoving.Store(req.Cascade)
	}
	return absent
}

func (m *Manager) sendHeartbeat(absent []string) {
	if m.coordinatorVersion > 0 {
		response := &heartbeatpb.MaintainerHeartbeat{}
		m.maintainers.Range(func(key, value interface{}) bool {
			m := value.(*Maintainer)
			if m.statusChanged.Load() ||
				time.Since(m.lastReportTime) > time.Second*2 {
				response.Statuses = append(response.Statuses, m.GetMaintainerStatus())
				m.statusChanged.Store(false)
				m.lastReportTime = time.Now()
			}
			return true
		})

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

func (m *Manager) handleMessages() []string {
	size := m.msgQueue.PopMessages(m.msgBuf, len(m.msgBuf))

	var absent []string
	for i := 0; i < size; i++ {
		msg := m.msgBuf[i]
		switch msg.Type {
		case messaging.TypeCoordinatorBootstrapRequest:
			m.onCoordinatorBootstrapRequest(msg)
		case messaging.TypeDispatchMaintainerRequest:
			absent = append(absent, m.onDispatchMaintainerRequest(msg)...)
		}
		m.msgBuf[i] = nil
	}
	return absent
}

func (m *Manager) cacheMaintainerMessage(changefeed string, msg *messaging.TargetMessage) {
	m.msgLock.RLock()
	v, ok := m.maintainers.Load(model.DefaultChangeFeedID(changefeed))
	m.msgLock.RUnlock()
	if !ok {
		log.Warn("maintainer is not found",
			zap.String("changefeedID", changefeed))
		return
	}

	maintainer := v.(*Maintainer)
	if maintainer.isSecondary.Load() {
		return
	}
	maintainer.getMessageQueue().Push(msg)
}
