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

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/log"
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

type Supervisor struct {
	StateMachines map[scheduler.ChangefeedID]*scheduler.StateMachine

	RunningTasks       map[scheduler.ChangefeedID]*ScheduleTask
	maxTaskConcurrency int
	lastScheduleTime   time.Time
	lastResendTime     time.Time
	schedulers         []Scheduler

	ID          scheduler.InferiorID
	initialized bool

	captures map[common.NodeID]*CaptureStatus

	// track all status reported by remote inferiors when bootstrap
	initStatus map[common.NodeID][]scheduler.InferiorStatus

	newInferior     func(id scheduler.InferiorID) scheduler.Inferior
	newBootstrapMsg func(id common.NodeID) *messaging.TargetMessage
}

type CaptureStatus struct {
	state             CaptureState
	capture           *common.NodeInfo
	lastBootstrapTime time.Time
}

func NewCaptureStatus(capture *common.NodeInfo) *CaptureStatus {
	return &CaptureStatus{
		state:   CaptureStateUninitialized,
		capture: capture,
	}
}

func NewSupervisor(
	ID scheduler.InferiorID, f func(id scheduler.InferiorID) scheduler.Inferior,
	newBootstrapMsg scheduler.NewBootstrapFn,
	schedulers ...Scheduler,
) *Supervisor {
	return &Supervisor{
		ID:                 ID,
		StateMachines:      make(map[scheduler.ChangefeedID]*scheduler.StateMachine),
		RunningTasks:       make(map[scheduler.ChangefeedID]*ScheduleTask),
		initialized:        false,
		captures:           make(map[common.NodeID]*CaptureStatus),
		initStatus:         make(map[common.NodeID][]scheduler.InferiorStatus),
		maxTaskConcurrency: 10000,
		schedulers:         schedulers,
		newInferior:        f,
		newBootstrapMsg:    newBootstrapMsg,
	}
}

func (s *Supervisor) GetAllCaptures() map[common.NodeID]*CaptureStatus {
	return s.captures
}

// GetInferiors return all state machines, caller should not modify it
func (s *Supervisor) GetInferiors() map[scheduler.ChangefeedID]*scheduler.StateMachine {
	return s.StateMachines
}

// HandleAliveCaptureUpdate update captures liveness.
func (s *Supervisor) HandleAliveCaptureUpdate(
	aliveCaptures map[common.NodeID]*common.NodeInfo,
) ([]*messaging.TargetMessage, error) {
	var removed []common.NodeID
	msgs := make([]*messaging.TargetMessage, 0)
	for id, info := range aliveCaptures {
		if _, ok := s.captures[id]; !ok {
			// A new server.
			s.captures[id] = NewCaptureStatus(info)
			log.Info("find a new server",
				zap.String("ID", s.ID.String()),
				zap.String("captureAddr", info.AdvertiseAddr),
				zap.String("server", id))
		}
	}

	// Find removed captures.
	for id, capture := range s.captures {
		if _, ok := aliveCaptures[id]; !ok {
			log.Info("removed a server",
				zap.String("ID", s.ID.String()),
				zap.String("captureAddr", capture.capture.AdvertiseAddr),
				zap.String("server", id))
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
			zap.String("ID", s.ID.String()),
			zap.Int("captureCount", len(s.captures)))
		statusMap := make(map[scheduler.InferiorID]map[common.NodeID]scheduler.InferiorStatus)
		for captureID, statuses := range s.initStatus {
			for _, status := range statuses {
				nodeMap, ok := statusMap[status.GetInferiorID()]
				if !ok {
					nodeMap = make(map[common.NodeID]scheduler.InferiorStatus)
					statusMap[status.GetInferiorID()] = nodeMap
				}
				nodeMap[captureID] = status
			}
		}
		for id, status := range statusMap {
			statemachine := scheduler.NewStateMachine(id, status, s.newInferior(id))
			if statemachine.State != scheduler.SchedulerStatusAbsent {
				s.StateMachines[id.(scheduler.ChangefeedID)] = statemachine
			}
		}
		s.initialized = true
		s.initStatus = nil
	}

	removedMsgs, err := s.handleRemovedNodes(removed)
	if err != nil {
		log.Error("handle changes failed", zap.Error(err))
		return nil, errors.Trace(err)
	}
	msgs = append(msgs, removedMsgs...)
	return msgs, nil
}

// UpdateCaptureStatus update the server status after receive a bootstrap message from remote
// supervisor will cache the status if the supervisor is not initialized
func (s *Supervisor) UpdateCaptureStatus(from common.NodeID, statuses []scheduler.InferiorStatus) {
	c, ok := s.captures[from]
	if !ok {
		log.Warn("server is not found",
			zap.String("ID", s.ID.String()),
			zap.String("server", from))
	}
	if c.state == CaptureStateUninitialized {
		c.state = CaptureStateInitialized
		log.Info("server initialized",
			zap.String("ID", s.ID.String()),
			zap.String("server", c.capture.ID),
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
	from common.NodeID, statuses []scheduler.InferiorStatus,
) ([]*messaging.TargetMessage, error) {
	sentMsgs := make([]*messaging.TargetMessage, 0)
	for _, status := range statuses {
		stateMachine, ok := s.StateMachines[status.GetInferiorID().(scheduler.ChangefeedID)]
		if !ok {
			log.Info("ignore status no inferior found",
				zap.String("ID", s.ID.String()),
				zap.Any("from", from),
				zap.Any("message", status))
			continue
		}
		msg := stateMachine.HandleInferiorStatus(status, from)
		if stateMachine.HasRemoved() {
			log.Info("inferior has removed",
				zap.String("ID", s.ID.String()),
				zap.Any("from", from),
				zap.String("inferiorID", status.GetInferiorID().String()))
			delete(s.StateMachines, status.GetInferiorID().(scheduler.ChangefeedID))
		}
		if msg != nil {
			sentMsgs = append(sentMsgs, msg)
		}
	}
	return sentMsgs, nil
}

// handleRemovedNodes handles server changes.
func (s *Supervisor) handleRemovedNodes(
	removed []common.NodeID,
) ([]*messaging.TargetMessage, error) {
	sentMsgs := make([]*messaging.TargetMessage, 0)
	if len(removed) > 0 {
		for id, stateMachine := range s.StateMachines {
			for _, captureID := range removed {
				msg, affected := stateMachine.HandleCaptureShutdown(captureID)
				if msg != nil {
					sentMsgs = append(sentMsgs, msg)
				}
				if affected {
					// Cleanup its running task.
					delete(s.RunningTasks, id)
					log.Info("remove running task",
						zap.String("stid", s.ID.String()),
						zap.String("id", id.String()))
				}
			}
		}
	}
	return sentMsgs, nil
}

// handleScheduleTasks handles schedule tasks.
func (s *Supervisor) handleScheduleTasks(
	tasks []*ScheduleTask,
) ([]*messaging.TargetMessage, error) {
	sentMsgs := make([]*messaging.TargetMessage, 0)
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

		var id scheduler.ChangefeedID
		if task.AddInferior != nil {
			id = task.AddInferior.ID
		} else if task.RemoveInferior != nil {
			id = task.RemoveInferior.ID
		} else if task.MoveInferior != nil {
			id = task.MoveInferior.ID
		}

		// Skip task if the inferior is already running a task,
		// or the inferior has removed.
		if _, ok := s.RunningTasks[id]; ok {
			log.Debug("ignore task, already exists",
				zap.String("id", s.ID.String()),
				zap.Any("task", task))
			continue
		}

		// it's remove or move inferior task, but we can not find the state machine
		if _, ok := s.StateMachines[id]; !ok && task.AddInferior == nil {
			log.Info("ignore task, inferior not found",
				zap.String("id", s.ID.String()),
				zap.Any("task", task))
			continue
		}

		var msg *messaging.TargetMessage
		if task.AddInferior != nil {
			msg = s.handleAddInferiorTask(task.AddInferior)
		} else if task.RemoveInferior != nil {
			msg = s.handleRemoveInferiorTask(task.RemoveInferior)
		} else if task.MoveInferior != nil {
			msg = s.handleMoveInferiorTask(task.MoveInferior)
		}
		if msg != nil {
			sentMsgs = append(sentMsgs, msg)
		}
		s.RunningTasks[id] = task
		log.Info("add running task",
			zap.String("supervisorID", s.ID.String()),
			zap.Any("task", task))
	}
	return sentMsgs, nil
}

func (s *Supervisor) handleAddInferiorTask(
	task *AddInferior,
) *messaging.TargetMessage {
	stateMachine, ok := s.StateMachines[task.ID]
	if !ok {
		stateMachine = scheduler.NewStateMachine(task.ID, nil, s.newInferior(task.ID))
		s.StateMachines[task.ID] = stateMachine
	}
	return stateMachine.HandleAddInferior(task.CaptureID)
}

func (s *Supervisor) handleRemoveInferiorTask(
	task *RemoveInferior,
) *messaging.TargetMessage {
	stateMachine, ok := s.StateMachines[task.ID]
	if !ok {
		log.Warn("statemachine not found",
			zap.Stringer("ID", s.ID),
			zap.Stringer("inferior", task.ID))
		return nil
	}
	if stateMachine.HasRemoved() {
		log.Info("inferior has removed",
			zap.Stringer("ID", s.ID),
			zap.Stringer("inferior", task.ID))
		delete(s.StateMachines, task.ID)
		return nil
	}
	return stateMachine.HandleRemoveInferior()
}

func (s *Supervisor) handleMoveInferiorTask(
	task *MoveInferior,
) *messaging.TargetMessage {
	stateMachine, ok := s.StateMachines[task.ID]
	if !ok {
		log.Warn("statemachine not found",
			zap.Stringer("ID", s.ID),
			zap.Stringer("inferior", task.ID))
		return nil
	}
	return stateMachine.HandleMoveInferior(task.DestCapture)
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
