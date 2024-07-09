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
	"github.com/flowbehappy/tigate/scheduler"
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
	supervisor *scheduler.Supervisor
	scheduler  scheduler.Scheduler

	maintainers map[model.ChangeFeedID]*Maintainer
}

// NewManager create a changefeed maintainer instance
func NewManager() *Manager {
	return &Manager{}
}

func (m *Manager) handleDispatchMaintainerRequest(
	request *DispatchMaintainerRequest,
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
		if err := cf.handleRemoveMaintainerTask(); err != nil {
			return errors.Trace(err)
		}
	}

	for _, req := range request.AddMaintainerRequests {
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

type DispatchMaintainerRequest struct {
	AddMaintainerRequests    []*AddMaintainerRequest
	RemoveMaintainerRequests []*RemoveMaintainerRequest
}

type BatchRemoveMaintainerRequest struct {
	Requests []*RemoveMaintainerRequest
}

type AddMaintainerRequest struct {
	ID          model.ChangeFeedID
	Config      *model.ChangeFeedInfo
	Status      *model.ChangeFeedStatus
	IsSecondary bool
}

type RemoveMaintainerRequest struct {
	ID      string
	Cascade bool
}
