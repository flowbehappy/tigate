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

package coordinator

import (
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/rpc"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type CaptureState int

const (
	// CaptureStateUninitialized means the server status is unknown,
	// no heartbeat response received yet.
	CaptureStateUninitialized CaptureState = 1
	// CaptureStateInitialized means scheduler has received heartbeat response.
	CaptureStateInitialized CaptureState = 2
)

type ServerStatus struct {
	state             CaptureState
	capture           *model.CaptureInfo
	lastBootstrapTime time.Time
}

func NewCaptureStatus(capture *model.CaptureInfo) *ServerStatus {
	return &ServerStatus{
		state:   CaptureStateUninitialized,
		capture: capture,
	}
}

// GetChangefeedStateMachine returns the state machine for that changefeed id
func (c *coordinator) GetChangefeedStateMachine(id model.ChangeFeedID) (*StateMachine, bool) {
	m, ok := c.stateMachines[id]
	return m, ok
}

// HandleAliveCaptureUpdate update captures liveness.
func (c *coordinator) HandleAliveCaptureUpdate(
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
) ([]rpc.Message, error) {
	var removed []model.CaptureID
	msgs := make([]rpc.Message, 0)
	for id, info := range aliveCaptures {
		if _, ok := c.captures[id]; !ok {
			// A new server.
			c.captures[id] = NewCaptureStatus(info)
			log.Info("find a new server",
				zap.String("ID", c.ID),
				zap.String("captureAddr", info.AdvertiseAddr),
				zap.String("server", id))
		}
	}

	// Find removed captures.
	for id, capture := range c.captures {
		if _, ok := aliveCaptures[id]; !ok {
			log.Info("removed a server",
				zap.String("ID", c.ID),
				zap.String("captureAddr", capture.capture.AdvertiseAddr),
				zap.String("server", id))
			delete(c.captures, id)

			// Only update changes after initialization.
			if !c.initialized {
				continue
			}
			removed = append(removed, id)
		}
		// not removed, if not initialized, try to send bootstrap message again
		if capture.state == CaptureStateUninitialized &&
			time.Since(capture.lastBootstrapTime) > time.Second {
			msgs = append(msgs, c.newBootstrapMessage(id))
		}
	}

	// Check if this is the first time all captures are initialized.
	if !c.initialized && c.checkAllCaptureInitialized() {
		log.Info("all server initialized",
			zap.String("ID", c.ID),
			zap.Int("captureCount", len(c.captures)))
		c.initialized = true

		// changefeed maintainer may run on multiple nodes at the same time, but only one can work
		statusMap := make(map[model.ChangeFeedID]map[model.CaptureID]*heartbeatpb.MaintainerStatus)
		for captureID, statuses := range c.initStatus {
			for _, status := range statuses {
				cfID := model.DefaultChangeFeedID(status.ChangefeedID)
				if _, ok := statusMap[cfID]; !ok {
					statusMap[cfID] = map[model.CaptureID]*heartbeatpb.MaintainerStatus{}
				}
				statusMap[cfID][captureID] = status
			}
		}

		for id, status := range statusMap {
			statemachine, err := NewStateMachine(id, status, newChangefeed(id))
			if err != nil {
				return nil, errors.Trace(err)
			}
			c.stateMachines[id] = statemachine
		}
		c.initStatus = nil
	}

	removedMsgs, err := c.handleRemovedNodes(removed)
	if err != nil {
		log.Error("handle changes failed", zap.Error(err))
		return nil, errors.Trace(err)
	}
	msgs = append(msgs, removedMsgs...)
	return msgs, nil
}

func (c *coordinator) newBootstrapMessage(captureID model.CaptureID) rpc.Message {
	return messaging.NewTargetMessage(
		messaging.ServerId(captureID),
		maintainerMangerTopic,
		messaging.TypeCoordinatorBootstrapRequest,
		&messaging.CoordinatorBootstrapRequest{
			CoordinatorBootstrapRequest: &heartbeatpb.CoordinatorBootstrapRequest{Version: c.version},
		})
}

// cacheBootstrapResponse caches the server status after receive a bootstrap message from remote
// supervisor will cache the status if the supervisor is not initialized
func (c *coordinator) cacheBootstrapResponse(from model.CaptureID, statuses []*heartbeatpb.MaintainerStatus) {
	capture, ok := c.captures[from]
	if !ok {
		log.Warn("server is not found",
			zap.String("ID", c.ID),
			zap.String("server", from))
	}
	if capture.state == CaptureStateUninitialized {
		capture.state = CaptureStateInitialized
		log.Info("server initialized",
			zap.String("ID", c.ID),
			zap.String("server", from),
			zap.String("captureAddr", capture.capture.AdvertiseAddr))
	}
	// scheduler is not initialized, is still collecting the remote server stauts
	// cache the last one
	if !c.initialized {
		c.initStatus[from] = statuses
	}
}

// HandleStatus handles inferior status reported by Inferior
func (c *coordinator) HandleStatus(
	from model.CaptureID, statuses []*heartbeatpb.MaintainerStatus,
) ([]rpc.Message, error) {
	sentMsgs := make([]rpc.Message, 0)
	for _, status := range statuses {
		stateMachine, ok := c.stateMachines[model.DefaultChangeFeedID(status.ChangefeedID)]
		if !ok {
			log.Info("ignore status no inferior found",
				zap.String("ID", c.ID),
				zap.Any("from", from),
				zap.Any("message", status))
			continue
		}
		msgs, err := stateMachine.HandleInferiorStatus(status, from)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if stateMachine.HasRemoved() {
			log.Info("inferior has removed",
				zap.String("ID", c.ID),
				zap.Any("from", from),
				zap.String("cfID", status.ChangefeedID))
			delete(c.stateMachines, model.DefaultChangeFeedID(status.ChangefeedID))
		}
		sentMsgs = append(sentMsgs, msgs...)
	}
	return sentMsgs, nil
}

// handleRemovedNodes handles server changes.
func (c *coordinator) handleRemovedNodes(
	removed []model.CaptureID,
) ([]rpc.Message, error) {
	sentMsgs := make([]rpc.Message, 0, len(removed))
	for id, stateMachine := range c.stateMachines {
		for _, captureID := range removed {
			msgs, affected, err := stateMachine.HandleCaptureShutdown(captureID)
			if err != nil {
				return nil, errors.Trace(err)
			}
			sentMsgs = append(sentMsgs, msgs...)
			if affected {
				// Cleanup its running task.
				delete(c.runningTasks, id)
			}
		}
	}
	return sentMsgs, nil
}

// HandleScheduleTasks handles schedule tasks.
func (c *coordinator) HandleScheduleTasks(
	tasks []*ScheduleTask,
) ([]rpc.Message, error) {
	// Check if a running task is finished.
	var toBeDeleted []model.ChangeFeedID
	for id, _ := range c.runningTasks {
		if stateMachine, ok := c.stateMachines[id]; ok {
			// If inferior is back to Replicating or Removed,
			// the running task is finished.
			if stateMachine.State == SchedulerStatusWorking || stateMachine.HasRemoved() {
				toBeDeleted = append(toBeDeleted, id)
			}
		} else {
			// No inferior found, remove the task
			toBeDeleted = append(toBeDeleted, id)
		}
	}
	for _, id := range toBeDeleted {
		delete(c.runningTasks, id)
	}

	sentMsgs := make([]rpc.Message, 0)
	for _, task := range tasks {
		// Check if accepting one more task exceeds maxTaskConcurrency.
		if len(c.runningTasks) == c.maxTaskConcurrency {
			log.Debug("too many running task",
				zap.String("id", c.ID))
			break
		}

		var cfID model.ChangeFeedID
		if task.AddInferior != nil {
			cfID = task.AddInferior.ID
		} else if task.RemoveInferior != nil {
			cfID = task.RemoveInferior.ID
		} else if task.MoveInferior != nil {
			cfID = task.MoveInferior.ID
		}

		// Skip task if the inferior is already running a task,
		// or the inferior has removed.
		if _, ok := c.runningTasks[cfID]; ok {
			log.Info("ignore task, already exists",
				zap.String("id", c.ID),
				zap.Any("task", task))
			continue
		}
		// it's remove or move inferior task, but we can not find the state machine
		if _, ok := c.stateMachines[cfID]; !ok && task.AddInferior == nil {
			log.Info("ignore task, inferior not found",
				zap.String("id", c.ID),
				zap.Any("task", task))
			continue
		}

		var msgs []rpc.Message
		var err error
		if task.AddInferior != nil {
			msgs, err = c.handleAddInferiorTask(task.AddInferior)
		} else if task.RemoveInferior != nil {
			msgs, err = c.handleRemoveInferiorTask(task.RemoveInferior)
		} else if task.MoveInferior != nil {
			msgs, err = c.handleMoveInferiorTask(task.MoveInferior)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		sentMsgs = append(sentMsgs, msgs...)
		c.runningTasks[cfID] = task
	}
	return sentMsgs, nil
}

func (c *coordinator) handleAddInferiorTask(
	task *AddInferior,
) ([]rpc.Message, error) {
	var err error
	cfID := task.ID
	stateMachine, ok := c.stateMachines[cfID]
	if !ok {
		stateMachine, err = NewStateMachine(task.ID, nil, newChangefeed(task.ID))
		if err != nil {
			return nil, errors.Trace(err)
		}
		c.stateMachines[cfID] = stateMachine
	}
	return stateMachine.HandleAddInferior(task.CaptureID)
}

func (c *coordinator) handleRemoveInferiorTask(
	task *RemoveInferior,
) ([]rpc.Message, error) {
	cfID := task.ID
	stateMachine, ok := c.stateMachines[cfID]
	if !ok {
		log.Warn("statemachine not found",
			zap.String("ID", c.ID),
			zap.Stringer("inferior", task.ID))
		return nil, nil
	}
	if stateMachine.HasRemoved() {
		log.Info("inferior has removed",
			zap.String("ID", c.ID),
			zap.Stringer("inferior", task.ID))
		delete(c.stateMachines, cfID)
		return nil, nil
	}
	return stateMachine.HandleRemoveInferior()
}

func (c *coordinator) handleMoveInferiorTask(
	task *MoveInferior,
) ([]rpc.Message, error) {
	cfID := task.ID
	stateMachine, ok := c.stateMachines[cfID]
	if !ok {
		log.Warn("statemachine not found",
			zap.String("ID", c.ID),
			zap.Stringer("inferior", task.ID))
		return nil, nil
	}
	return stateMachine.HandleMoveInferior(task.DestCapture)
}

// CheckAllCaptureInitialized check if all server is initialized.
// returns true when all server reports the bootstrap response
func (c *coordinator) CheckAllCaptureInitialized() bool {
	return c.initialized && c.checkAllCaptureInitialized()
}

func (c *coordinator) checkAllCaptureInitialized() bool {
	for _, captureStatus := range c.captures {
		// CaptureStateStopping is also considered initialized, because when
		// a server shutdown, it becomes stopping, we need to move its tables
		// to other captures.
		if captureStatus.state == CaptureStateUninitialized {
			return false
		}
	}
	return len(c.captures) != 0
}
