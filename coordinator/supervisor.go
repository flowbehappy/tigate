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
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/log"
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

type Supervisor struct {
	StateMachines map[common.MaintainerID]*scheduler.StateMachine[common.MaintainerID]

	RunningTasks       map[common.MaintainerID]*ScheduleTask
	maxTaskConcurrency int
	lastScheduleTime   time.Time
	schedulers         []Scheduler

	ID          common.CoordinatorID
	initialized bool

	captures map[node.ID]*CaptureStatus

	// track all status reported by remote inferiors when bootstrap
	initStatus map[node.ID][]*heartbeatpb.MaintainerStatus

	newInferior     scheduler.NewInferiorFn[common.MaintainerID]
	newBootstrapMsg scheduler.NewBootstrapFn
}

type CaptureStatus struct {
	state             CaptureState
	capture           *node.Info
	lastBootstrapTime time.Time
}

func NewCaptureStatus(capture *node.Info) *CaptureStatus {
	return &CaptureStatus{
		state:   CaptureStateUninitialized,
		capture: capture,
	}
}

func NewSupervisor(
	ID common.CoordinatorID,
	f scheduler.NewInferiorFn[common.MaintainerID],
	newBootstrapMsg scheduler.NewBootstrapFn,
	schedulers ...Scheduler,
) *Supervisor {
	return &Supervisor{
		ID:                 ID,
		StateMachines:      make(map[common.MaintainerID]*scheduler.StateMachine[common.MaintainerID]),
		RunningTasks:       make(map[common.MaintainerID]*ScheduleTask),
		initialized:        false,
		captures:           make(map[node.ID]*CaptureStatus),
		initStatus:         make(map[node.ID][]*heartbeatpb.MaintainerStatus),
		maxTaskConcurrency: 10000,
		schedulers:         schedulers,
		newInferior:        f,
		newBootstrapMsg:    newBootstrapMsg,
	}
}

func (s *Supervisor) GetAllCaptures() map[node.ID]*CaptureStatus {
	return s.captures
}

// GetInferiors return all state machines, caller should not modify it
func (s *Supervisor) GetInferiors() map[common.MaintainerID]*scheduler.StateMachine[common.MaintainerID] {
	return s.StateMachines
}

// HandleAliveCaptureUpdate update captures liveness.
func (s *Supervisor) HandleAliveCaptureUpdate(
	aliveCaptures map[node.ID]*node.Info,
) ([]*messaging.TargetMessage, error) {
	var removed []node.ID
	msgs := make([]*messaging.TargetMessage, 0)
	for id, info := range aliveCaptures {
		if _, ok := s.captures[id]; !ok {
			// A new server.
			s.captures[id] = NewCaptureStatus(info)
			log.Info("find a new server",
				zap.String("ID", s.ID.String()),
				zap.String("captureAddr", info.AdvertiseAddr),
				zap.Any("server", id))
		}
	}

	// Find removed captures.
	for id, capture := range s.captures {
		if _, ok := aliveCaptures[id]; !ok {
			log.Info("removed a server",
				zap.String("ID", s.ID.String()),
				zap.String("captureAddr", capture.capture.AdvertiseAddr),
				zap.Any("server", id))
			delete(s.captures, id)

			// Only update changes after initialization.
			if !s.initialized {
				continue
			}
			removed = append(removed, id)
		}
		// not removed, if not initialized, try to send bootstrap message again
		if capture.state == CaptureStateUninitialized &&
			time.Since(capture.lastBootstrapTime) > time.Millisecond*500 {
			msgs = append(msgs, s.newBootstrapMsg(id))
			capture.lastBootstrapTime = time.Now()
		}
	}

	// Check if this is the first time all captures are initialized.
	if !s.initialized && s.checkAllCaptureInitialized() {
		log.Info("all server initialized",
			zap.Any("ID", s.ID),
			zap.Int("captureCount", len(s.captures)))
		statusMap := make(map[common.MaintainerID]map[node.ID]any)
		for captureID, statuses := range s.initStatus {
			for _, status := range statuses {
				nodeMap, ok := statusMap[common.MaintainerID(status.ChangefeedID)]
				if !ok {
					nodeMap = make(map[node.ID]any)
					statusMap[common.MaintainerID(status.ChangefeedID)] = nodeMap
				}
				nodeMap[captureID] = status
			}
		}
		for id, status := range statusMap {
			statemachine := scheduler.NewStateMachine(id, status, s.newInferior(id))
			if statemachine.State != scheduler.SchedulerStatusAbsent {
				s.StateMachines[id] = statemachine
			}
		}
		s.initialized = true
		s.initStatus = nil
	}

	s.handleRemovedNodes(removed)
	return msgs, nil
}

// UpdateCaptureStatus update the server status after receive a bootstrap message from remote
// supervisor will cache the status if the supervisor is not initialized
func (s *Supervisor) UpdateCaptureStatus(from node.ID, statuses []*heartbeatpb.MaintainerStatus) {
	c, ok := s.captures[from]
	if !ok {
		log.Warn("server is not found",
			zap.String("ID", s.ID.String()),
			zap.Any("server", from))
	}
	if c.state == CaptureStateUninitialized {
		c.state = CaptureStateInitialized
		log.Info("server initialized",
			zap.String("ID", s.ID.String()),
			zap.Any("server", c.capture.ID),
			zap.String("captureAddr", c.capture.AdvertiseAddr))
	}
	// scheduler is not initialized, is still collecting the remote server stauts
	// cache the last one
	if !s.initialized {
		s.initStatus[from] = statuses
	}
}

// HandleStatus handles inferior status reported by Inferior
func (s *Supervisor) HandleStatus(
	from node.ID, statuses []*heartbeatpb.MaintainerStatus,
) {
	for _, status := range statuses {
		changefeedID := common.MaintainerID(status.ChangefeedID)
		stateMachine, ok := s.StateMachines[changefeedID]
		if !ok {
			log.Info("ignore status no inferior found",
				zap.String("ID", s.ID.String()),
				zap.Any("from", from),
				zap.Any("message", status))
			continue
		}
		stateMachine.HandleInferiorStatus(status.State, status, from)
		if stateMachine.HasRemoved() {
			log.Info("inferior has removed",
				zap.String("ID", s.ID.String()),
				zap.Any("from", from),
				zap.Stringer("inferiorID", changefeedID))
			delete(s.StateMachines, changefeedID)
		}
	}
}

// handleRemovedNodes handles server changes.
func (s *Supervisor) handleRemovedNodes(
	removed []node.ID,
) {
	for id, stateMachine := range s.StateMachines {
		for _, captureID := range removed {
			affected := stateMachine.HandleCaptureShutdown(captureID)
			if affected {
				// Cleanup its running task.
				delete(s.RunningTasks, id)
				log.Info("remove running task",
					zap.String("stid", s.ID.String()),
					zap.Stringer("id", id))
			}
		}
	}
}

// handleScheduleTasks handles schedule tasks.
func (s *Supervisor) handleScheduleTasks(
	tasks []*ScheduleTask,
) {
	for idx, task := range tasks {
		// Check if accepting one more task exceeds maxTaskConcurrency.
		if len(s.RunningTasks) >= s.maxTaskConcurrency {
			log.Warn("Drop tasks since there are too many running task",
				zap.String("id", s.ID.String()),
				zap.Int("maxTaskConcurrency", s.maxTaskConcurrency),
				zap.Int("runningTaskCount", len(s.RunningTasks)),
				zap.Int("pendingTaskCount", len(tasks)-idx))
			break
		}

		var id common.MaintainerID
		if task.AddInferior != nil {
			id = task.AddInferior.ID
		} else if task.RemoveInferior != nil {
			id = task.RemoveInferior.ID
		} else if task.MoveInferior != nil {
			id = task.MoveInferior.ID
		}

		// Skip task if the inferior is already running a task,
		// or the inferior has removed.
		if exist, ok := s.RunningTasks[id]; ok {
			log.Debug("ignore task, since there is one task belong to the same inferior exists",
				zap.String("id", s.ID.String()), zap.Any("exist", exist), zap.Any("task", task))
			continue
		}

		// it's remove or move inferior task, but we can not find the state machine
		if _, ok := s.StateMachines[id]; !ok && task.AddInferior == nil {
			log.Info("ignore task, inferior not found",
				zap.String("id", s.ID.String()), zap.Any("task", task))
			continue
		}

		if task.AddInferior != nil {
			s.handleAddInferiorTask(task.AddInferior)
		} else if task.RemoveInferior != nil {
			s.handleRemoveInferiorTask(task.RemoveInferior)
		} else if task.MoveInferior != nil {
			s.handleMoveInferiorTask(task.MoveInferior)
		}
		s.RunningTasks[id] = task
		log.Info("add running task", zap.String("supervisorID", s.ID.String()), zap.Any("task", task))
	}
}

func (s *Supervisor) handleAddInferiorTask(
	task *AddInferior,
) {
	stateMachine, ok := s.StateMachines[task.ID]
	if !ok {
		stateMachine = scheduler.NewStateMachine(task.ID, nil, s.newInferior(task.ID))
		s.StateMachines[task.ID] = stateMachine
	}
	stateMachine.HandleAddInferior(task.CaptureID)
}

func (s *Supervisor) handleRemoveInferiorTask(
	task *RemoveInferior,
) {
	stateMachine, ok := s.StateMachines[task.ID]
	if !ok {
		log.Warn("statemachine not found",
			zap.Stringer("ID", s.ID),
			zap.Stringer("inferior", task.ID))
		return
	}
	if stateMachine.HasRemoved() {
		log.Info("inferior has removed",
			zap.Stringer("ID", s.ID),
			zap.Stringer("inferior", task.ID))
		delete(s.StateMachines, task.ID)
		return
	}
	stateMachine.HandleRemoveInferior()
}

func (s *Supervisor) handleMoveInferiorTask(
	task *MoveInferior,
) {
	stateMachine, ok := s.StateMachines[task.ID]
	if !ok {
		log.Warn("statemachine not found",
			zap.Stringer("ID", s.ID),
			zap.Stringer("inferior", task.ID))
		return
	}
	stateMachine.HandleMoveInferior(task.DestCapture)
}

// CheckAllCaptureInitialized check if all server is initialized.
// returns true when all server reports the bootstrap response
func (s *Supervisor) CheckAllCaptureInitialized() bool {
	return s.initialized && s.checkAllCaptureInitialized()
}

func (s *Supervisor) checkAllCaptureInitialized() bool {
	for _, captureStatus := range s.captures {
		// CaptureStateStopping is also considered initialized, because when
		// a server shutdown, it becomes stopping, we need to move its tables
		// to other captures.
		if captureStatus.state == CaptureStateUninitialized {
			return false
		}
	}
	return len(s.captures) != 0
}
