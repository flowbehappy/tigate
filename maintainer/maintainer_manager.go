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
	"github.com/flowbehappy/tigate/rpc"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

type Manager struct {
	rpcClient rpc.RpcClient

	supervisor *scheduler.Supervisor
	scheduler  scheduler.Scheduler

	maintainers map[model.ChangeFeedID]*Maintainer

	coordinatorID string
}

func NewCoordinator(rpcClient rpc.RpcClient) *Manager {
	return &Manager{
		rpcClient: rpcClient,
	}
}

type ChangefeedStatus struct {
	ID           model.ChangeFeedID
	Status       scheduler.ComponentStatus
	CheckpointTs uint64
}

func (m *Manager) Heartbeat() {
	var changefeeds []*ChangefeedStatus
	for _, mt := range m.maintainers {
		changefeeds = append(changefeeds, &ChangefeedStatus{
			ID:           mt.id,
			Status:       mt.status,
			CheckpointTs: mt.checkpointTs,
		})
	}
}

func (m *Manager) onCoordinatorBootstrap(coordinatorCapture string) {
	log.Info("coordinator bootstrap started",
		zap.String("capture", coordinatorCapture))
	m.coordinatorID = coordinatorCapture

	var changefeeds []*ChangefeedStatus
	for _, mt := range m.maintainers {
		changefeeds = append(changefeeds, &ChangefeedStatus{
			ID:           mt.id,
			Status:       mt.status,
			CheckpointTs: mt.checkpointTs,
		})
	}
	//todo send to coordinator
}

func (m *Manager) onAddMaintainer(
	id model.ChangeFeedID,
	info *model.ChangeFeedInfo,
	checkpointTs uint64) {
	log.Info("add new maintainer",
		zap.String("id", id.String()),
		zap.Uint64("checkpointTs", checkpointTs))
	mt := NewMaintainer()
	m.maintainers[id] = mt
}

func (m *Manager) onRemoveMaintainer(id model.ChangeFeedID) {
	log.Info("remove maintainer",
		zap.String("id", id.String()))
	mt, ok := m.maintainers[id]
	if !ok {
		log.Warn("maintainer not found",
			zap.String("id", id.String()))
	}
	mt.Stop()
}
