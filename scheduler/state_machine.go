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
	"fmt"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// SchedulerStatus is the state of Inferior in scheduler.
//
//	absent -> commit --> working -> removing -> commit
//
//nolint:revive
type SchedulerStatus int

const (
	SchedulerStatusAbsent = iota
	SchedulerStatusCommiting
	SchedulerStatusWorking
	SchedulerStatusRemoving
)

func (r SchedulerStatus) String() string {
	switch r {
	case SchedulerStatusAbsent:
		return "Absent"
	case SchedulerStatusCommiting:
		return "Commiting"
	case SchedulerStatusWorking:
		return "Working"
	case SchedulerStatusRemoving:
		return "Removing"
	default:
		return fmt.Sprintf("Unknown %d", r)
	}
}

type StateMachine struct {
	ID    InferiorID
	State SchedulerStatus
	// Primary is the server ID that is currently running the inferior.
	Primary model.CaptureID
	// Primary is the server ID that is this inferior should be moved to
	Secondary model.CaptureID

	// Inferior handles the real logic
	Inferior Inferior

	lastMsgTime time.Time
}

// NewStateMachine build a state machine from all server reported status
// it could be called after a scheduler is bootstrapped
func NewStateMachine(
	id InferiorID,
	inferiorStatus map[model.CaptureID]InferiorStatus,
	inferior Inferior,
) (*StateMachine, error) {
	sm := &StateMachine{
		ID:          id,
		Inferior:    inferior,
		lastMsgTime: time.Now(),
	}
	inferior.SetStateMachine(sm)
	for captureID, status := range inferiorStatus {
		if status.GetInferiorID().Less(sm.ID) {
			return nil, sm.inconsistentError(status, captureID,
				"inferior id inconsistent")
		}
		sm.Inferior.UpdateStatus(status)

		switch status.GetInferiorState() {
		case heartbeatpb.ComponentState_Working:
			if len(sm.Primary) != 0 {
				return nil, sm.multiplePrimaryError(
					status, captureID, "multiple primary",
					zap.String("primary", sm.Primary),
					zap.String("status", status.GetInferiorState().String()))
			}
			sm.Primary = captureID
		case heartbeatpb.ComponentState_Absent,
			heartbeatpb.ComponentState_Stopped:
			// Ignore stop state.
		default:
			log.Warn("unknown inferior state",
				zap.String("ID", sm.ID.String()),
				zap.String("status", status.GetInferiorID().String()))
		}
	}

	// Build state from primary, secondary and captures.
	if len(sm.Primary) != 0 {
		sm.State = SchedulerStatusWorking
		log.Info("initialize a working state state machine",
			zap.String("statemachine", sm.ID.String()))
	} else {
		sm.State = SchedulerStatusAbsent
	}
	log.Info("initialize state machine",
		zap.String("id", sm.ID.String()),
		zap.String("state", sm.State.String()))

	return sm, nil
}

//nolint:unparam
func (s *StateMachine) inconsistentError(
	input InferiorStatus, captureID model.CaptureID,
	msg string, fields ...zap.Field,
) error {
	fields = append(fields, []zap.Field{
		zap.String("captureID", captureID),
		zap.String("state", input.GetInferiorState().String()),
		zap.String("statemachine", s.ID.String()),
	}...)
	log.L().WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
	return errors.New("inconsistent error: " + msg)
}

func (s *StateMachine) multiplePrimaryError(
	input InferiorStatus, captureID model.CaptureID, msg string, fields ...zap.Field,
) error {
	fields = append(fields, []zap.Field{
		zap.String("captureID", captureID),
		zap.String("state", input.GetInferiorState().String()),
		zap.String("statemachine", s.ID.String()),
	}...)
	log.L().WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
	return errors.New("inconsistent error: " + msg)
}

// HandleInferiorStatus transit state based on input and the current state.
func (s *StateMachine) HandleInferiorStatus(
	input InferiorStatus, captureID model.CaptureID,
) (*messaging.TargetMessage, error) {
	if s.Primary != captureID {
		return nil, nil
	}

	var err error
	oldState := s.State
	var msg *messaging.TargetMessage
	switch s.State {
	case SchedulerStatusCommiting:
		msg, err = s.pollOnCommit(input, captureID)
	case SchedulerStatusWorking:
		err = s.pollOnWorking(input, captureID)
	case SchedulerStatusRemoving:
		msg = s.pollOnRemoving(input, captureID)
	default:
		return nil, s.inconsistentError(
			input, captureID, "state unknown")
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if oldState != s.State {
		log.Info("state transition, poll",
			zap.String("status", input.GetInferiorID().String()),
			zap.String("captureID", captureID),
			zap.Stringer("old", oldState),
			zap.Stringer("new", s.State))
	}
	return msg, nil
}

func (s *StateMachine) pollOnCommit(
	input InferiorStatus, captureID model.CaptureID,
) (*messaging.TargetMessage, error) {
	switch input.GetInferiorState() {
	case heartbeatpb.ComponentState_Stopped, heartbeatpb.ComponentState_Absent:
		s.Inferior.UpdateStatus(input)
		s.Primary = ""
		// primary is stopped and there is no secondary, transit to Absent.
		log.Info("primary is stopped during Commit",
			zap.String("status", input.GetInferiorID().String()),
			zap.String("captureID", captureID),
			zap.String("statemachine", s.ID.String()))
		s.State = SchedulerStatusAbsent
		return nil, nil
	case heartbeatpb.ComponentState_Working:
		s.Inferior.UpdateStatus(input)
		s.State = SchedulerStatusWorking
		log.Info("state transition from commit to working",
			zap.String("statemachine", s.ID.String()),
			zap.String("inferiorID", input.GetInferiorID().String()))
		return nil, nil
	}
	log.Warn("ignore input, unexpected state",
		zap.String("status", input.GetInferiorID().String()),
		zap.String("captureID", captureID),
		zap.String("statemachine", s.ID.String()))
	return nil, nil
}

//nolint:unparam
func (s *StateMachine) pollOnWorking(
	input InferiorStatus, captureID model.CaptureID,
) error {
	switch input.GetInferiorState() {
	case heartbeatpb.ComponentState_Working:
		s.Inferior.UpdateStatus(input)
		return nil
	case heartbeatpb.ComponentState_Absent, heartbeatpb.ComponentState_Stopped:
		s.Inferior.UpdateStatus(input)
		s.Primary = ""
		s.State = SchedulerStatusAbsent
		return nil
	}
	log.Warn("ignore input, unexpected state",
		zap.String("status", input.GetInferiorID().String()),
		zap.String("captureID", captureID),
		zap.String("statemachine", s.ID.String()))
	return nil
}

//nolint:unparam
func (s *StateMachine) pollOnRemoving(
	input InferiorStatus, captureID model.CaptureID,
) *messaging.TargetMessage {
	switch input.GetInferiorState() {
	case heartbeatpb.ComponentState_Working:
		s.Inferior.UpdateStatus(input)
		return nil
	case heartbeatpb.ComponentState_Absent, heartbeatpb.ComponentState_Stopped:
		if s.Secondary != "" {
			s.Primary = s.Secondary
			s.Secondary = ""
			s.State = SchedulerStatusCommiting
			return s.Inferior.NewAddInferiorMessage(s.Primary)
		}
		// clear the primary to mark the statemachine as removed
		s.Primary = ""
	}
	log.Warn("ignore input, unexpected  state",
		zap.String("status", input.GetInferiorID().String()),
		zap.String("captureID", captureID),
		zap.String("statemachine", s.ID.String()))
	return nil
}

func (s *StateMachine) HandleAddInferior(
	captureID model.CaptureID,
) (*messaging.TargetMessage, error) {
	// Ignore add inferior if it's not in Absent state.
	if s.State != SchedulerStatusAbsent {
		log.Warn("add inferior is ignored",
			zap.String("captureID", captureID),
			zap.String("statemachine", s.ID.String()))
		return nil, nil
	}
	s.Primary = captureID
	oldState := s.State
	s.State = SchedulerStatusCommiting

	log.Info("state transition, add ingferior",
		zap.String("captureID", captureID),
		zap.String("statemachine", s.ID.String()),
		zap.Stringer("old", oldState),
		zap.Stringer("new", s.State))
	return s.Inferior.NewAddInferiorMessage(s.Primary), nil
}

func (s *StateMachine) HandleMoveInferior(
	dest model.CaptureID,
) (*messaging.TargetMessage, error) {
	// Ignore move inferior if it has been removed already.
	if s.HasRemoved() {
		log.Warn("move inferior is ignored",
			zap.String("statemachine", s.ID.String()))
		return nil, nil
	}
	// Ignore move inferior if
	// 1) it's not in Working state or
	// 2) the dest server is the primary.
	if s.State != SchedulerStatusWorking || s.Primary == dest {
		log.Warn("move inferior is ignored",
			zap.String("statemachine", s.ID.String()))
		return nil, nil
	}
	oldState := s.State
	s.State = SchedulerStatusRemoving
	s.Secondary = dest
	log.Info("state transition, move inferior",
		zap.Stringer("new", s.State),
		zap.String("statemachine", s.ID.String()),
		zap.Stringer("old", oldState))
	return s.Inferior.NewRemoveInferiorMessage(s.Primary), nil
}

func (s *StateMachine) HandleRemoveInferior() (*messaging.TargetMessage, error) {
	// Ignore remove inferior if it has been removed already.
	if s.HasRemoved() {
		log.Warn("remove inferior is ignored",
			zap.String("statemachine", s.ID.String()))
		return nil, nil
	}
	// Ignore remove inferior if it's not in Working state.
	if s.State == SchedulerStatusRemoving {
		log.Warn("remove inferior is ignored",
			zap.String("statemachine", s.ID.String()))
		return nil, nil
	}
	oldState := s.State
	s.State = SchedulerStatusRemoving
	log.Info("state transition, remove inferiror",
		zap.String("statemachine", s.ID.String()),
		zap.Stringer("old", oldState))
	return s.Inferior.NewRemoveInferiorMessage(s.Primary), nil
}

// HandleCaptureShutdown handle server shutdown event.
// Besides returning messages and errors, it also returns a bool to indicate
// whether s is affected by the server shutdown.
func (s *StateMachine) HandleCaptureShutdown(
	captureID model.CaptureID,
) (*messaging.TargetMessage, bool) {
	if s.Primary != captureID && s.Secondary != captureID {
		return nil, false
	}
	oldState := s.State
	var msg *messaging.TargetMessage
	switch oldState {
	case SchedulerStatusAbsent, SchedulerStatusCommiting, SchedulerStatusWorking:
		// primary node is stopped, set to absent to reschedule
		s.Primary = ""
		s.State = SchedulerStatusAbsent
	case SchedulerStatusRemoving:
		// check if we are moving this state machine
		if s.Secondary == "" {
			s.Primary = ""
			s.State = SchedulerStatusAbsent
		} else {
			if s.Secondary == captureID {
				// destination capture is stopped during moving, clear secondary node
				s.Secondary = ""
			} else {
				// primary capture is stopped, move to secondary
				s.State = SchedulerStatusCommiting
				msg = s.Inferior.NewAddInferiorMessage(s.Primary)
			}
		}
	}
	log.Info("state transition, server shutdown",
		zap.String("statemachine", s.ID.String()),
		zap.String("captureID", captureID),
		zap.Stringer("old", oldState),
		zap.Stringer("new", s.State))
	return msg, true
}

func (s *StateMachine) HasRemoved() bool {
	// It has been removed successfully if it's state is Removing,
	// and there is no server has it.
	return s.State == SchedulerStatusRemoving && len(s.Primary) == 0 && len(s.Secondary) == 0
}

func (s *StateMachine) handleResend() *messaging.TargetMessage {
	s.lastMsgTime = time.Now()
	switch s.State {
	case SchedulerStatusCommiting:
		return s.Inferior.NewAddInferiorMessage(s.Primary)
	case SchedulerStatusRemoving:
		return s.Inferior.NewRemoveInferiorMessage(s.Primary)
	default:
		return nil
	}
}
