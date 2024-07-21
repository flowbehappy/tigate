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
	"time"

	"github.com/flowbehappy/tigate/rpc"
	"github.com/flowbehappy/tigate/utils"
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

type Supervisor struct {
	StateMachines utils.Map[InferiorID, *StateMachine]
	RunningTasks  utils.Map[InferiorID, *ScheduleTask]

	maxTaskConcurrency int
	schedulers         []Scheduler

	ID          InferiorID
	initialized bool

	captures map[model.CaptureID]*CaptureStatus

	// track all status reported by remote inferiors when bootstrap
	initStatus map[model.CaptureID][]InferiorStatus

	newInferior     func(id InferiorID) Inferior
	newBootstrapMsg func(id model.CaptureID) rpc.Message
}

type CaptureStatus struct {
	state             CaptureState
	capture           *model.CaptureInfo
	lastBootstrapTime time.Time

	CheckpointTs uint64
}

func NewCaptureStatus(capture *model.CaptureInfo) *CaptureStatus {
	return &CaptureStatus{
		state:        CaptureStateUninitialized,
		capture:      capture,
		CheckpointTs: 0,
	}
}

func NewSupervisor(
	ID InferiorID, f func(id InferiorID) Inferior,
	newBootstrap func(id model.CaptureID) rpc.Message,
	schedulers ...Scheduler,
) *Supervisor {
	return &Supervisor{
		ID:                 ID,
		StateMachines:      utils.NewBtreeMap[InferiorID, *StateMachine](),
		RunningTasks:       utils.NewBtreeMap[InferiorID, *ScheduleTask](),
		initialized:        false,
		captures:           make(map[model.CaptureID]*CaptureStatus),
		initStatus:         make(map[model.CaptureID][]InferiorStatus),
		maxTaskConcurrency: 10000,
		schedulers:         schedulers,
		newInferior:        f,
		newBootstrapMsg:    newBootstrap,
	}
}

func (s *Supervisor) GetAllCaptures() map[model.CaptureID]*CaptureStatus {
	return s.captures
}

// GetInferiors return all state machines, caller should not modify it
func (s *Supervisor) GetInferiors() utils.Map[InferiorID, *StateMachine] {
	return s.StateMachines
}

// GetInferior returns the state machine for that inferior id, caller should not modify it
func (s *Supervisor) GetInferior(id InferiorID) (*StateMachine, bool) {
	return s.StateMachines.Get(id)
}

// HandleAliveCaptureUpdate update captures liveness.
func (s *Supervisor) HandleAliveCaptureUpdate(
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
) ([]rpc.Message, error) {
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
		statusMap := utils.NewBtreeMap[InferiorID, map[model.CaptureID]InferiorStatus]()
		for captureID, statuses := range s.initStatus {
			for _, status := range statuses {
				if ok := statusMap.Has(status.GetInferiorID()); !ok {
					statusMap.ReplaceOrInsert(status.GetInferiorID(), map[model.CaptureID]InferiorStatus{})
				}
				m, _ := statusMap.Get(status.GetInferiorID())
				m[captureID] = status
			}
		}
		var err error
		statusMap.Ascend(func(id InferiorID, status map[model.CaptureID]InferiorStatus) bool {
			statemachine, err1 := NewStateMachine(id, status, s.newInferior(id))
			if err1 != nil {
				err = errors.Trace(err1)
				return false
			}
			s.StateMachines.ReplaceOrInsert(id, statemachine)
			return true
		})
		if err != nil {
			return nil, err
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
		stateMachine, ok := s.StateMachines.Get(status.GetInferiorID())
		if !ok {
			log.Info("ignore status no inferior found",
				zap.String("ID", s.ID.String()),
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
				zap.String("ID", s.ID.String()),
				zap.Any("from", from),
				zap.String("inferiorID", status.GetInferiorID().String()))
			s.StateMachines.Delete(status.GetInferiorID())
		}
		sentMsgs = append(sentMsgs, msgs...)
	}
	return sentMsgs, nil
}

// handleRemovedNodes handles server changes.
func (s *Supervisor) handleRemovedNodes(
	removed []model.CaptureID,
) ([]rpc.Message, error) {
	sentMsgs := make([]rpc.Message, 0)
	if len(removed) > 0 {
		var err error
		s.StateMachines.Ascend(func(id InferiorID, stateMachine *StateMachine) bool {
			for _, captureID := range removed {
				msgs, affected, err1 := stateMachine.HandleCaptureShutdown(captureID)
				if err != nil {
					err = errors.Trace(err1)
					return false
				}
				sentMsgs = append(sentMsgs, msgs...)
				if affected {
					// Cleanup its running task.
					s.RunningTasks.Delete(id)
					log.Info("remove running task",
						zap.String("stid", s.ID.String()),
						zap.String("id", id.String()))
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
	s.RunningTasks.Ascend(func(id InferiorID, task *ScheduleTask) bool {
		if stateMachine, ok := s.StateMachines.Get(id); ok {
			// If inferior is back to Replicating or Removed,
			// the running task is finished.
			if stateMachine.State == SchedulerStatusWorking || stateMachine.HasRemoved() {
				toBeDeleted = append(toBeDeleted, id)
			}
		} else {
			// No inferior found, remove the task
			toBeDeleted = append(toBeDeleted, id)
		}
		return true
	})
	for _, span := range toBeDeleted {
		s.RunningTasks.Delete(span)
		log.Info("remove running task",
			zap.String("stid", s.ID.String()),
			zap.String("id", span.String()))
	}

	sentMsgs := make([]rpc.Message, 0)
	for _, task := range tasks {
		// Check if accepting one more task exceeds maxTaskConcurrency.
		if s.RunningTasks.Len() == s.maxTaskConcurrency {
			log.Debug("too many running task",
				zap.String("id", s.ID.String()))
			break
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
		//if _, ok := s.RunningTasks.Get(id); ok {
		//	log.Info("ignore task, already exists",
		//		zap.String("id", s.ID.String()),
		//		zap.Any("task", task))
		//	continue
		//}

		// it's remove or move inferior task, but we can not find the state machine
		if _, ok := s.StateMachines.Get(id); !ok && task.AddInferior == nil {
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
		s.RunningTasks.ReplaceOrInsert(id, task)
		log.Info("add running task",
			zap.String("stid", s.ID.String()),
			zap.String("id", s.ID.String()),
			zap.Any("task", task))
	}
	return sentMsgs, nil
}

func (s *Supervisor) handleAddInferiorTask(
	task *AddInferior,
) ([]rpc.Message, error) {
	var err error
	stateMachine, ok := s.StateMachines.Get(task.ID)
	if !ok {
		stateMachine, err = NewStateMachine(task.ID, nil, s.newInferior(task.ID))
		if err != nil {
			return nil, errors.Trace(err)
		}
		s.StateMachines.ReplaceOrInsert(task.ID, stateMachine)
	}
	return stateMachine.HandleAddInferior(task.CaptureID)
}

func (s *Supervisor) handleRemoveInferiorTask(
	task *RemoveInferior,
) ([]rpc.Message, error) {
	stateMachine, ok := s.StateMachines.Get(task.ID)
	if !ok {
		log.Warn("statemachine not found",
			zap.Stringer("ID", s.ID),
			zap.Stringer("inferior", task.ID))
		return nil, nil
	}
	if stateMachine.HasRemoved() {
		log.Info("inferior has removed",
			zap.Stringer("ID", s.ID),
			zap.Stringer("inferior", task.ID))
		s.StateMachines.Delete(task.ID)
		return nil, nil
	}
	return stateMachine.HandleRemoveInferior()
}

func (s *Supervisor) handleMoveInferiorTask(
	task *MoveInferior,
) ([]rpc.Message, error) {
	stateMachine, ok := s.StateMachines.Get(task.ID)
	if !ok {
		log.Warn("statemachine not found",
			zap.Stringer("ID", s.ID),
			zap.Stringer("inferior", task.ID))
		return nil, nil
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
