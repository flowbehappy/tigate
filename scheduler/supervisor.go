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

package scheduler

import (
	"github.com/flowbehappy/tigate/rpc"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"time"
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
	stateMachines Map[InferiorID, *StateMachine]
	runningTasks  Map[InferiorID, *ScheduleTask]

	maxTaskConcurrency int
	NewInferior        func(InferiorID) Inferior

	// self ID
	ID          InferiorID
	initialized bool

	captures map[model.CaptureID]*CaptureStatus

	// track all status reported by remote inferiors when bootstrap
	initStatus map[model.CaptureID][]InferiorStatus

	bootstrapMessageFunc func(model.CaptureID) rpc.Message
}

type CaptureStatus struct {
	state             CaptureState
	capture           *model.CaptureInfo
	lastBootstrapTime time.Time
}

func NewCaptureStatus(capture *model.CaptureInfo) *CaptureStatus {
	return &CaptureStatus{
		state:   CaptureStateUninitialized,
		capture: capture,
	}
}

func NewSupervisor(
	ID InferiorID,
	newInferiorFunc func(InferiorID) Inferior,
	bootstrapMessageFunc func(model.CaptureID) rpc.Message) *Supervisor {
	return &Supervisor{
		ID:                   ID,
		stateMachines:        NewBtreeMap[InferiorID, *StateMachine](),
		runningTasks:         NewBtreeMap[InferiorID, *ScheduleTask](),
		NewInferior:          newInferiorFunc,
		initialized:          false,
		captures:             make(map[model.CaptureID]*CaptureStatus),
		bootstrapMessageFunc: bootstrapMessageFunc,
	}
}

func (s *Supervisor) GetAllCaptures() map[model.CaptureID]*CaptureStatus {
	return s.captures
}

// GetInferiors return all state machines, caller should not modify it
func (s *Supervisor) GetInferiors() Map[InferiorID, *StateMachine] {
	return s.stateMachines
}

// GetInferior returns the state machine for that inferior id, caller should not modify it
func (s *Supervisor) GetInferior(id InferiorID) (*StateMachine, bool) {
	return s.stateMachines.Get(id)
}

// HandleAliveCaptureUpdate update captures liveness.
func (s *Supervisor) HandleAliveCaptureUpdate(
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
) ([]rpc.Message, []model.CaptureID) {
	var removed []model.CaptureID
	msgs := make([]rpc.Message, 0)
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
			time.Since(capture.lastBootstrapTime) > time.Second {
			msgs = append(msgs, s.bootstrapMessageFunc(id))
		}
	}

	// Check if this is the first time all captures are initialized.
	if !s.initialized && s.checkAllCaptureInitialized() {
		log.Info("all server initialized",
			zap.String("ID", s.ID.String()),
			zap.Int("captureCount", len(s.captures)))
		s.initialized = true
	}
	if len(removed) > 0 {
		removedMsgs, err := s.HandleCaptureChanges(removed)
		if err != nil {
			log.Error("handle changes failed", zap.Error(err))
		}
		msgs = append(msgs, removedMsgs...)
	}
	return msgs, removed
}

// UpdateCaptureStatus update the server status after receive a bootstrap message from remote
// supervisor will cache the status if the supervisor is not initialized
func (s *Supervisor) UpdateCaptureStatus(from model.CaptureID, statuses []InferiorStatus) {
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
	from model.CaptureID, statuses []InferiorStatus,
) ([]rpc.Message, error) {
	sentMsgs := make([]rpc.Message, 0)
	for _, status := range statuses {
		stateMachine, ok := s.stateMachines.Get(status.GetInferiorID())
		if !ok {
			log.Info("ignore status no inferior found",
				zap.String("ID", s.ID.String()),
				zap.Any("from", from),
				zap.Any("message", status))
			continue
		}
		msgs, err := stateMachine.handleInferiorStatus(status, from)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if stateMachine.hasRemoved() {
			log.Info("inferior has removed",
				zap.String("ID", s.ID.String()),
				zap.Any("from", from),
				zap.String("inferiorID", status.GetInferiorID().String()))
			s.stateMachines.Delete(status.GetInferiorID())
		}
		sentMsgs = append(sentMsgs, msgs...)
	}
	return sentMsgs, nil
}

// HandleCaptureChanges handles server changes.
func (s *Supervisor) HandleCaptureChanges(
	removed []model.CaptureID,
) ([]rpc.Message, error) {
	if s.initStatus != nil {
		if s.stateMachines.Len() != 0 {
			log.Panic("init again",
				zap.Any("init", s.initStatus),
				zap.Any("statemachines", s.stateMachines.Len()))
		}
		statusMap := make(map[InferiorID]map[model.CaptureID]InferiorStatus)
		for captureID, statuses := range s.initStatus {
			for _, status := range statuses {
				if _, ok := statusMap[status.GetInferiorID()]; !ok {
					statusMap[status.GetInferiorID()] = map[model.CaptureID]InferiorStatus{}
				}
				statusMap[status.GetInferiorID()][captureID] = status
			}
		}
		for id, status := range statusMap {
			//todo: how to new inferior
			statemachine, err := NewStateMachine(id, status, s.NewInferior(id))
			if err != nil {
				return nil, errors.Trace(err)
			}
			s.stateMachines.ReplaceOrInsert(id, statemachine)
		}
		s.initStatus = nil
	}
	sentMsgs := make([]rpc.Message, 0)
	if removed != nil {
		var err error
		s.stateMachines.Ascend(func(id InferiorID, stateMachine *StateMachine) bool {
			for _, captureID := range removed {
				msgs, affected, err1 := stateMachine.handleCaptureShutdown(captureID)
				if err != nil {
					err = errors.Trace(err1)
					return false
				}
				sentMsgs = append(sentMsgs, msgs...)
				if affected {
					// Cleanup its running task.
					s.runningTasks.Delete(id)
				}
			}
			return true
		})
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return sentMsgs, nil
}

// HandleScheduleTasks handles schedule tasks.
func (s *Supervisor) HandleScheduleTasks(
	tasks []*ScheduleTask,
) ([]rpc.Message, error) {
	// Check if a running task is finished.
	var toBeDeleted []InferiorID
	s.runningTasks.Ascend(func(id InferiorID, task *ScheduleTask) bool {
		if stateMachine, ok := s.stateMachines.Get(id); ok {
			// If inferior is back to Replicating or Removed,
			// the running task is finished.
			if stateMachine.State == SchedulerStatusWorking || stateMachine.hasRemoved() {
				toBeDeleted = append(toBeDeleted, id)
			}
		} else {
			// No inferior found, remove the task
			toBeDeleted = append(toBeDeleted, id)
		}
		return true
	})
	for _, span := range toBeDeleted {
		s.runningTasks.Delete(span)
	}

	sentMsgs := make([]rpc.Message, 0)
	for _, task := range tasks {
		// Check if accepting one more task exceeds maxTaskConcurrency.
		if s.runningTasks.Len() == s.maxTaskConcurrency {
			log.Debug("too many running task",
				zap.String("id", s.ID.String()))
			// Does not use break, in case there is burst balance task
			// in the remaining tasks.
			continue
		}

		var id InferiorID
		if task.AddInferior != nil {
			id = task.AddInferior.ID
		} else if task.RemoveInferior != nil {
			id = task.RemoveInferior.ID
		} else if task.MoveInferior != nil {
			id = task.MoveInferior.ID
		}

		// Skip task if the inferior is already running a task,
		// or the inferior has removed.
		if _, ok := s.runningTasks.Get(id); ok {
			log.Info("ignore task, already exists",
				zap.String("id", s.ID.String()),
				zap.Any("task", task))
			continue
		}
		// it's remove or move inferior task, but we can not find the state machine
		if _, ok := s.stateMachines.Get(id); !ok && task.AddInferior == nil {
			log.Info("ignore task, inferior not found",
				zap.String("id", s.ID.String()),
				zap.Any("task", task))
			continue
		}

		var msgs []rpc.Message
		var err error
		if task.AddInferior != nil {
			msgs, err = s.handleAddInferiorTask(task.AddInferior)
		} else if task.RemoveInferior != nil {
			msgs, err = s.handleRemoveInferiorTask(task.RemoveInferior)
		} else if task.MoveInferior != nil {
			msgs, err = s.handleMoveInferiorTask(task.MoveInferior)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		sentMsgs = append(sentMsgs, msgs...)
		s.runningTasks.ReplaceOrInsert(id, task)
	}
	return sentMsgs, nil
}

func (s *Supervisor) handleAddInferiorTask(
	task *AddInferior,
) ([]rpc.Message, error) {
	var err error
	stateMachine, ok := s.stateMachines.Get(task.ID)
	if !ok {
		stateMachine, err = NewStateMachine(task.ID, nil, s.NewInferior(task.ID))
		if err != nil {
			return nil, errors.Trace(err)
		}
		s.stateMachines.ReplaceOrInsert(task.ID, stateMachine)
	}
	return stateMachine.handleAddInferior(task.CaptureID)
}

func (s *Supervisor) handleRemoveInferiorTask(
	task *RemoveInferior,
) ([]rpc.Message, error) {
	stateMachine, ok := s.stateMachines.Get(task.ID)
	if !ok {
		log.Warn("statemachine not found",
			zap.Stringer("ID", s.ID),
			zap.Stringer("inferior", task.ID))
		return nil, nil
	}
	if stateMachine.hasRemoved() {
		log.Info("inferior has removed",
			zap.Stringer("ID", s.ID),
			zap.Stringer("inferior", task.ID))
		s.stateMachines.Delete(task.ID)
		return nil, nil
	}
	return stateMachine.handleRemoveInferior()
}

func (s *Supervisor) handleMoveInferiorTask(
	task *MoveInferior,
) ([]rpc.Message, error) {
	stateMachine, ok := s.stateMachines.Get(task.ID)
	if !ok {
		log.Warn("statemachine not found",
			zap.Stringer("ID", s.ID),
			zap.Stringer("inferior", task.ID))
		return nil, nil
	}
	return stateMachine.handleMoveInferior(task.DestCapture)
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
