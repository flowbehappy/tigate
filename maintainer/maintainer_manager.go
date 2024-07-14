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
	"sync"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// Manager is the manager of all changefeed maintainer in a ticdc wacher, each ticdc wacher will
// start a Manager when the wacher is startup. the Manager should:
// 1. handle bootstrap command from coordinator and return all changefeed maintainer status
// 2. handle dispatcher command from coordinator: add or remove changefeed maintainer
// 3. check maintainer liveness
type Manager struct {
	messageCenter messaging.MessageCenter

	maintainers map[model.ChangeFeedID]*Maintainer

	msgLock sync.RWMutex
	msgBuf  []*messaging.TargetMessage

	coordinatorID      messaging.ServerId
	coordinatorVersion int64

	selfServerID messaging.ServerId
}

// NewMaintainerManager create a changefeed maintainer manager instance,
// 1. manager receives bootstrap command from coordinator
// 2. manager manages maintainer lifetime
// 3. manager report maintainer status to coordinator
func NewMaintainerManager(messageCenter messaging.MessageCenter,
	selfServerID messaging.ServerId) *Manager {
	m := &Manager{
		messageCenter: messageCenter,
		maintainers:   make(map[model.ChangeFeedID]*Maintainer),
		selfServerID:  selfServerID,
	}
	messageCenter.RegisterHandler(m.Name(), func(msg *messaging.TargetMessage) error {
		m.msgLock.Lock()
		m.msgBuf = append(m.msgBuf, msg)
		m.msgLock.Unlock()
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
			m.msgLock.Lock()
			buf := m.msgBuf
			m.msgBuf = nil
			m.msgLock.Unlock()

			hasBootstrapMsg := false
			for _, msg := range buf {
				switch msg.Type {
				case messaging.TypeCoordinatorBootstrapRequest:
					req := msg.Message.(*messaging.CoordinatorBootstrapRequest)
					if m.coordinatorVersion > req.Version {
						log.Warn("ignore invalid coordinator version",
							zap.Int64("version", req.Version))
						continue
					}
					m.coordinatorID = msg.From
					m.coordinatorVersion = req.Version
					hasBootstrapMsg = true
					log.Info("bootstrap coordinator",
						zap.Int64("version", m.coordinatorVersion))
				case messaging.TypeDispatchMaintainerRequest:
					req := msg.Message.(*messaging.DispatchMaintainerRequest)
					if m.coordinatorID != msg.From {
						log.Warn("ignore invalid coordinator id",
							zap.Any("request", req),
							zap.Any("coordinator", msg.From))
						continue
					}
					err := m.handleDispatchMaintainerRequest(req)
					if err != nil {
						log.Error("handle dispatch maintainer request failed", zap.Error(err))
						return err
					}
				}
			}
			if m.coordinatorVersion > 0 {
				response := &heartbeatpb.MaintainerHeartbeat{
					Statuses: make([]*heartbeatpb.MaintainerStatus, 0, len(m.maintainers)),
				}
				for _, m := range m.maintainers {
					if hasBootstrapMsg || m.statusChanged.Load() ||
						time.Since(m.lastReportTime) > time.Second*2 {
						response.Statuses = append(response.Statuses, m.GetMaintainerStatus())
						m.statusChanged.Store(false)
						m.lastReportTime = time.Now()
					}
				}
				if hasBootstrapMsg || len(response.Statuses) != 0 {
					m.sendMessages(response)
				}
			}

			// cleanup removed maintainer
			for _, cf := range m.maintainers {
				if cf.removed.Load() {
					cf.Cancel()
					delete(m.maintainers, cf.id)
				}
			}
		}
	}
}

func (m *Manager) sendMessages(msg *heartbeatpb.MaintainerHeartbeat) {
	target := messaging.NewTargetMessage(
		m.coordinatorID,
		"coordinator",
		messaging.TypeMaintainerHeartbeatRequest,
		msg,
	)
	err := m.messageCenter.SendCommand(target)
	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
}

// Close closes the wacher, it's a block call
func (m *Manager) Close(ctx context.Context) error {
	return nil
}

func (m *Manager) handleDispatchMaintainerRequest(
	request *messaging.DispatchMaintainerRequest,
) error {
	for _, req := range request.AddMaintainers {
		cfID := model.DefaultChangeFeedID(req.GetId())
		task := &dispatchMaintainerTask{
			ID:        model.DefaultChangeFeedID(req.GetId()),
			IsRemove:  false,
			IsPrepare: req.IsSecondary,
			status:    dispatchTaskReceived,
		}
		cf, ok := m.maintainers[cfID]
		if !ok {
			cf = NewMaintainer(cfID, m.messageCenter)
			m.maintainers[cfID] = cf
			//if err := threadpool.GetTaskSchedulerInstance().MaintainerTaskScheduler.Submit(cf); err != nil {
			//	return errors.Trace(err)
			//}
			//go cf.Run()
		}
		task.Maintainer = cf
		cf.injectDispatchTableTask(task)
		cf.handleAddMaintainerTask()
		//cf.taskCh <- task
	}

	for _, req := range request.RemoveMaintainers {
		cfID := model.DefaultChangeFeedID(req.GetId())
		cf, ok := m.maintainers[cfID]
		if !ok {
			log.Warn("ignore remove maintainer request, "+
				"since the maintainer not found",
				zap.String("changefeed", cf.id.String()),
				zap.Any("request", req))
			return nil
		}
		task := &dispatchMaintainerTask{
			Maintainer: cf,
			ID:         cfID,
			IsRemove:   true,
			status:     dispatchTaskReceived,
		}
		//cf.taskCh <- task
		cf.injectDispatchTableTask(task)
		cf.handleRemoveMaintainerTask()
	}
	return nil
}
