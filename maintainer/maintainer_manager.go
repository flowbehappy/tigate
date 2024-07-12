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

	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/rpc"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// Manager is the manager of all changefeed maintainer in a ticdc node, each ticdc node will
// start a Manager when the node is startup. the Manager should:
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
	tick := time.NewTicker(time.Millisecond * 100)
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
				request, err := rpc.DecodeCoordinatorRequest(msg.Message.([]byte))
				if err != nil {
					log.Error("decode request failed", zap.Error(err))
				}

				if request.BootstrapRequest != nil {
					if m.coordinatorVersion > request.BootstrapRequest.Version {
						log.Warn("ignore invalid coordinator version",
							zap.Int64("version", request.BootstrapRequest.Version))
						continue
					}
					m.coordinatorID = msg.From
					m.coordinatorVersion = request.BootstrapRequest.Version
					hasBootstrapMsg = true
				}
				if request.DispatchMaintainerRequest != nil {
					if m.coordinatorID != msg.From {
						log.Warn("ignore invalid coordinator id",
							zap.Any("request", request),
							zap.Any("coordinator", msg.From))
						continue
					}
					err := m.handleDispatchMaintainerRequest(request.DispatchMaintainerRequest)
					if err != nil {
						log.Error("handle dispatch maintainer request failed", zap.Error(err))
						return err
					}
				}
			}
			if m.coordinatorVersion > 0 {
				response := &rpc.MaintainerManagerRequest{}
				for _, m := range m.maintainers {
					response.MaintainerStatus = append(response.MaintainerStatus, m.GetMaintainerStatus())
				}
				if hasBootstrapMsg || response.MaintainerStatus != nil {
					m.sendMessages(response)
				}
			}
		}
	}
}

func (m *Manager) sendMessages(msg *rpc.MaintainerManagerRequest) {
	buf, err := msg.Encode()
	if err != nil {
		log.Error("failed to encode coordinator request", zap.Any("msg", msg), zap.Error(err))
		return
	}
	target := messaging.NewTargetMessage(
		m.coordinatorID,
		"coordinator",
		messaging.TypeBytes,
		buf,
	)
	target.From = m.selfServerID
	err = m.messageCenter.SendCommand(target)

	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
}

// Close closes the module, it's a block call
func (m *Manager) Close(ctx context.Context) error {
	return nil
}

func (m *Manager) handleDispatchMaintainerRequest(
	request *rpc.DispatchMaintainerRequest,
) error {
	for _, req := range request.AddMaintainerRequests {
		task := &dispatchMaintainerTask{
			ID:        req.ID,
			IsRemove:  false,
			IsPrepare: req.IsSecondary,
			status:    dispatchTaskReceived,
		}
		cf, ok := m.maintainers[req.ID]
		if !ok {
			cf = NewMaintainer(req.ID)
			m.maintainers[req.ID] = cf
		}
		cf.injectDispatchTableTask(task)
		if err := cf.handleAddMaintainerTask(); err != nil {
			return errors.Trace(err)
		}
	}

	for _, req := range request.RemoveMaintainerRequests {
		span := req.ID
		cf, ok := m.maintainers[span]
		if !ok {
			log.Warn("ignore remove maintainer request, "+
				"since the table not found",
				zap.String("changefeed", cf.id.String()),
				zap.Any("request", req))
			return nil
		}
		task := &dispatchMaintainerTask{
			ID:       req.ID,
			IsRemove: true,
			status:   dispatchTaskReceived,
		}
		cf.injectDispatchTableTask(task)
		if err := cf.handleRemoveMaintainerTask(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
