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
	"encoding/json"
	"fmt"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/rpc"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// SchedulerStatus is the state of Inferior in scheduler.
//
//	 AddInferior
//	┌────────┐   ┌─────────┐
//	│ Absent ├─> │ Prepare │
//	└────────┘   └──┬──────┘
//	     ┌──────────┘   ^
//	     v              │ MoveInferior
//	┌────────┐   ┌──────┴──────┐ RemoveInferior ┌──────────┐
//	│ Commit ├──>│ Working     │───────────────>│ Removing │
//	└────────┘   └─────────────┘                └──────────┘
//
// When a server shutdown unexpectedly, we may need to transit the state to
// Absent or Working immediately.
//
//nolint:revive
type SchedulerStatus int

const (
	SchedulerStatusUnknown SchedulerStatus = iota
	SchedulerStatusAbsent
	SchedulerStatusPrepare
	SchedulerStatusCommit
	SchedulerStatusWorking
	SchedulerStatusRemoving
)

func (r SchedulerStatus) String() string {
	switch r {
	case SchedulerStatusAbsent:
		return "Absent"
	case SchedulerStatusPrepare:
		return "Prepare"
	case SchedulerStatusCommit:
		return "Commit"
	case SchedulerStatusWorking:
		return "Working"
	case SchedulerStatusRemoving:
		return "Removing"
	default:
		return fmt.Sprintf("Unknown %d", r)
	}
}

// Role is the role of a server.
type Role int

const (
	// RolePrimary primary role.
	RolePrimary = 1
	// RoleSecondary secondary role.
	RoleSecondary = 2
	// RoleUndetermined means that we don't know its state, it may be
	// working, stopping or stopped.
	RoleUndetermined = 3
)

func (r Role) String() string {
	switch r {
	case RolePrimary:
		return "Primary"
	case RoleSecondary:
		return "Secondary"
	case RoleUndetermined:
		return "Undetermined"
	default:
		return fmt.Sprintf("Unknown %d", r)
	}
}

type StateMachine struct {
	ID    InferiorID
	State SchedulerStatus
	// Primary is the server ID that is currently running the inferior.
	Primary model.CaptureID
	// Servers is a map of captures that has the inferior.
	// NB: Invariant, 1) at most one primary, 2) primary server must be in
	//     CaptureRolePrimary.
	Servers map[model.CaptureID]Role

	// Inferior handles the real logic
	Inferior Inferior

	LastMsgTime time.Time
}

// NewStateMachine build a state machine from all server reported status
// it could be called after a scheduler is bootstrapped
func NewStateMachine(
	id InferiorID,
	inferiorStatus map[model.CaptureID]InferiorStatus,
	inferior Inferior,
) (*StateMachine, error) {
	sm := &StateMachine{
		ID:       id,
		Servers:  make(map[string]Role),
		Inferior: inferior,
	}
	// Count of captures that is in Stopping states.
	stoppingCount := 0
	committed := false
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
					zap.Any("primary", sm.Primary),
					zap.Any("status", status))
			}
			// Recognize primary if it's inferior is in working state.
			err := sm.setCapture(captureID, RoleSecondary)
			if err != nil {
				return nil, errors.Trace(err)
			}
			err = sm.promoteSecondary(captureID)
			if err != nil {
				return nil, errors.Trace(err)
			}
		case heartbeatpb.ComponentState_Preparing:
			// Recognize secondary if it's inferior is in preparing state.
			err := sm.setCapture(captureID, RoleSecondary)
			if err != nil {
				return nil, errors.Trace(err)
			}
		case heartbeatpb.ComponentState_Prepared:
			// Recognize secondary and Commit state if it's inferior is in prepared state.
			committed = true
			err := sm.setCapture(captureID, RoleSecondary)
			if err != nil {
				return nil, errors.Trace(err)
			}
		case heartbeatpb.ComponentState_Stopping:
			// The server is stopping the inferior. It is possible that the
			// server is primary, and is still working.
			// We need to wait its state becomes Stopped or Absent before
			// proceeding further scheduling.
			log.Warn("found a stopping server during initializing",
				zap.Any("ID", sm.ID),
				zap.Any("statemachine", sm),
				zap.Any("status", status))
			err := sm.setCapture(captureID, RoleUndetermined)
			if err != nil {
				return nil, errors.Trace(err)
			}
			stoppingCount++
		case heartbeatpb.ComponentState_Absent,
			heartbeatpb.ComponentState_Stopped:
			// Ignore stop state.
		default:
			log.Warn("unknown inferior state",
				zap.Any("ID", sm.ID),
				zap.Any("statemachine", sm),
				zap.Any("status", status))
		}
	}

	// Build state from primary, secondary and captures.
	if len(sm.Primary) != 0 {
		sm.State = SchedulerStatusWorking
	}
	// Move inferior or add inferior is in-progress.
	if sm.hasRole(RoleSecondary) {
		sm.State = SchedulerStatusPrepare
	}
	// Move inferior or add inferior is committed.
	if committed {
		sm.State = SchedulerStatusCommit
	}
	// no server is bind, set to absent status, will trigger an add command
	if len(sm.Servers) == 0 {
		sm.State = SchedulerStatusAbsent
	}
	// all captures are in stopping status
	if sm.State == SchedulerStatusUnknown && len(sm.Servers) == stoppingCount {
		sm.State = SchedulerStatusRemoving
	}
	log.Info("initialize state machine",
		zap.Any("statemachine", sm))

	return sm, nil
}

func (s *StateMachine) hasRole(role Role) bool {
	_, has := s.GetRole(role)
	return has
}

func (s *StateMachine) isInRole(captureID model.CaptureID, role Role) bool {
	rc, ok := s.Servers[captureID]
	if !ok {
		return false
	}
	return rc == role
}

func (s *StateMachine) GetRole(role Role) (model.CaptureID, bool) {
	for captureID, cr := range s.Servers {
		if cr == role {
			return captureID, true
		}
	}
	return "", false
}

func (s *StateMachine) setCapture(captureID model.CaptureID, role Role) error {
	cr, ok := s.Servers[captureID]
	if ok && cr != role {
		jsonR, _ := json.Marshal(s)
		return errors.New("marshal server failure: " + string(jsonR))
	}
	s.Servers[captureID] = role
	return nil
}

func (s *StateMachine) clearCapture(captureID model.CaptureID, role Role) error {
	cr, ok := s.Servers[captureID]
	if ok && cr != role {
		jsonR, _ := json.Marshal(s)
		return errors.New("marshal server failure: " + string(jsonR))
	}
	delete(s.Servers, captureID)
	return nil
}

func (s *StateMachine) promoteSecondary(captureID model.CaptureID) error {
	if s.Primary == captureID {
		log.Warn("server is already promoted as the primary",
			zap.String("captureID", captureID),
			zap.Any("statemachine", s))
		return nil
	}
	role, ok := s.Servers[captureID]
	if ok && role != RoleSecondary {
		jsonR, _ := json.Marshal(s)
		return errors.New("marshal server failure: " + string(jsonR))
	}
	if s.Primary != "" {
		delete(s.Servers, s.Primary)
	}
	s.Primary = captureID
	s.Servers[s.Primary] = RolePrimary
	return nil
}

func (s *StateMachine) clearPrimary() {
	delete(s.Servers, s.Primary)
	s.Primary = ""
}

//nolint:unparam
func (s *StateMachine) inconsistentError(
	input InferiorStatus, captureID model.CaptureID,
	msg string, fields ...zap.Field,
) error {
	fields = append(fields, []zap.Field{
		zap.String("captureID", captureID),
		zap.Any("state", input),
		zap.Any("statemachine", s),
	}...)
	log.L().WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
	return errors.New("inconsistent error: " + msg)
}

func (s *StateMachine) multiplePrimaryError(
	input InferiorStatus, captureID model.CaptureID, msg string, fields ...zap.Field,
) error {
	fields = append(fields, []zap.Field{
		zap.String("captureID", captureID),
		zap.Any("state", input),
		zap.Any("statemachine", s),
	}...)
	log.L().WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
	return errors.New("inconsistent error: " + msg)
}

// checkInvariant ensures StateMachine invariant is hold.
func (s *StateMachine) checkInvariant(
	input InferiorStatus, captureID model.CaptureID,
) error {
	if !s.ID.Equal(input.GetInferiorID()) {
		return s.inconsistentError(input, captureID,
			"ID must be the same")
	}
	if len(s.Servers) == 0 {
		if s.State == SchedulerStatusPrepare ||
			s.State == SchedulerStatusCommit ||
			s.State == SchedulerStatusWorking {
			// When the state is in prepare, commit or working, there must
			// be at least one of primary and secondary.
			return s.inconsistentError(input, captureID,
				"empty primary/secondary in state prepare/commit/working")
		}
	}
	roleP, okP := s.Servers[s.Primary]
	if (!okP && s.Primary != "") || // Primary is not in Servers.
		(okP && roleP != RolePrimary) { // Primary is not in primary role.
		return s.inconsistentError(input, captureID,
			"server inconsistent")
	}

	// check if the primary role is correct
	for captureID, role := range s.Servers {
		if role == RolePrimary && captureID != s.Primary {
			return s.multiplePrimaryError(input, captureID,
				"server inconsistent")
		}
	}
	return nil
}

// poll transit state based on input and the current state.
func (s *StateMachine) poll(
	input InferiorStatus, captureID model.CaptureID,
) ([]rpc.Message, error) {
	if _, ok := s.Servers[captureID]; !ok {
		return nil, nil
	}

	msgBuf := make([]rpc.Message, 0)
	stateChanged := true
	for stateChanged {
		err := s.checkInvariant(input, captureID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldState := s.State
		var msg rpc.Message
		switch s.State {
		case SchedulerStatusAbsent:
			msg, stateChanged, err = s.pollOnAbsent(input, captureID)
		case SchedulerStatusPrepare:
			msg, stateChanged, err = s.pollOnPrepare(input, captureID)
		case SchedulerStatusCommit:
			msg, stateChanged, err = s.pollOnCommit(input, captureID)
		case SchedulerStatusWorking:
			stateChanged, err = s.pollOnWorking(input, captureID)
		case SchedulerStatusRemoving:
			msg, stateChanged, err = s.pollOnRemoving(input, captureID)
		default:
			return nil, s.inconsistentError(
				input, captureID, "state unknown")
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		if msg != nil {
			msgBuf = append(msgBuf, msg)
		}
		if stateChanged {
			log.Info("state transition, poll",
				zap.Any("status", input),
				zap.String("captureID", captureID),
				zap.Stringer("old", oldState),
				zap.Stringer("new", s.State))
		}
	}

	return msgBuf, nil
}

//nolint:unparam
func (s *StateMachine) pollOnAbsent(
	input InferiorStatus, captureID model.CaptureID,
) (rpc.Message, bool, error) {
	switch input.GetInferiorState() {
	case heartbeatpb.ComponentState_Absent:
		secondary, ok := s.GetRole(RoleSecondary)
		if s.Primary == "" && len(s.Servers) == 1 &&
			ok && secondary == captureID {
			s.State = SchedulerStatusCommit
			log.Info("state transition, poll, only has secondary capture, schedule directly",
				zap.Any("status", input),
				zap.String("captureID", captureID),
				zap.String("old", "absent"),
				zap.Stringer("new", s.State))
			// No primary, promote secondary to primary.
			// add directly or move inferior after all primary server is reported stopped status
			err := s.promoteSecondary(captureID)
			if err != nil {
				return nil, false, errors.Trace(err)
			}

			log.Info("promote secondary, no primary",
				zap.Any("status", input),
				zap.String("captureID", captureID),
				zap.Any("statemachine", s.ID.String()))
			return s.Inferior.NewAddInferiorMessage(captureID, false), false, nil
		}
		s.State = SchedulerStatusPrepare
		err := s.setCapture(captureID, RoleSecondary)
		return nil, true, errors.Trace(err)

	case heartbeatpb.ComponentState_Stopped:
		// Ignore stopped state as a server may be shutdown unexpectedly.
		// todo: fix ignore task, already exists error, when add a task and prepare, then receive a stop
		// the running task still has the scheduler task
		return nil, false, nil
	case heartbeatpb.ComponentState_Preparing,
		heartbeatpb.ComponentState_Prepared,
		heartbeatpb.ComponentState_Working,
		heartbeatpb.ComponentState_Stopping:
	}
	log.Warn("ignore input, unexpected state",
		zap.Any("status", input),
		zap.String("captureID", captureID),
		zap.Any("statemachine", s))
	return nil, false, nil
}

func (s *StateMachine) pollOnPrepare(
	input InferiorStatus, captureID model.CaptureID,
) (rpc.Message, bool, error) {
	switch input.GetInferiorState() {
	case heartbeatpb.ComponentState_Absent:
		if s.isInRole(captureID, RoleSecondary) {
			return s.Inferior.NewAddInferiorMessage(captureID, true), false, nil
		}
	case heartbeatpb.ComponentState_Preparing:
		if s.isInRole(captureID, RoleSecondary) {
			// Ignore secondary Preparing, it may take a long time.
			return nil, false, nil
		}
	case heartbeatpb.ComponentState_Prepared:
		if s.isInRole(captureID, RoleSecondary) {
			// Secondary is prepared, transit to Commit state.
			s.State = SchedulerStatusCommit
			return nil, true, nil
		}
	case heartbeatpb.ComponentState_Working:
		// moving state, and the primary watcher still report status
		if s.Primary == captureID {
			s.Inferior.UpdateStatus(input)
			return nil, false, nil
		}
	case heartbeatpb.ComponentState_Stopping, heartbeatpb.ComponentState_Stopped:
		// moving state, primary report status
		if s.Primary == captureID {
			// Primary is stopped, but we may still has secondary.
			// Clear primary and promote secondary when it's prepared.
			log.Info("primary is stopped during Prepare",
				zap.Any("status", input),
				zap.String("captureID", captureID),
				zap.Any("statemachine", s))
			s.clearPrimary()
			return nil, false, nil
		}
		if s.isInRole(captureID, RoleSecondary) {
			log.Info("server is stopped during Prepare",
				zap.Any("status", input),
				zap.String("captureID", captureID),
				zap.Any("statemachine", s.ID.String()))
			err := s.clearCapture(captureID, RoleSecondary)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			if s.Primary != "" {
				// moving state
				// Secondary is stopped, and we still has primary.
				// Transit to working.
				s.State = SchedulerStatusWorking
			} else {
				// Secondary is stopped, and we do not have primary.
				// Transit to Absent. scheduler will schedule it again
				s.State = SchedulerStatusAbsent
			}
			return nil, true, nil
		}
	}
	log.Warn("ignore input, unexpected state",
		zap.Any("status", input),
		zap.String("captureID", captureID),
		zap.Any("statemachine", s))
	return nil, false, nil
}

func (s *StateMachine) pollOnCommit(
	input InferiorStatus, captureID model.CaptureID,
) (rpc.Message, bool, error) {
	switch input.GetInferiorState() {
	case heartbeatpb.ComponentState_Prepared:
		if s.isInRole(captureID, RoleSecondary) {
			if s.Primary != "" {
				// Secondary server is prepared and waiting for stopping primary.
				// Send message to primary, ask for stopping.
				return s.Inferior.NewRemoveInferiorMessage(s.Primary), false, nil
			}
			if s.hasRole(RoleUndetermined) {
				// we will has the RoleUndetermined only when server reported stopping status, do not needed to send stop message
				// There are other captures that have the inferior.
				// Must waiting for other captures become stopped or absent
				// before promoting the secondary, otherwise there may be two
				// primary that write data and lead to data inconsistency.
				log.Info("there are unknown captures during commit",
					zap.Any("status", input),
					zap.String("captureID", captureID),
					zap.Any("statemachine", s))
				return nil, false, nil
			}
			// No primary, promote secondary to primary.
			// add directly or move inferior after all primary server is reported stopped status
			err := s.promoteSecondary(captureID)
			if err != nil {
				return nil, false, errors.Trace(err)
			}

			log.Info("promote secondary, no primary",
				zap.Any("status", input),
				zap.String("captureID", captureID),
				zap.Any("statemachine", s))
		}
		// Secondary has been promoted, retry add inferior request.
		if s.Primary == captureID && !s.hasRole(RoleSecondary) {
			return s.Inferior.NewAddInferiorMessage(captureID, false), false, nil
		}

	case heartbeatpb.ComponentState_Stopped, heartbeatpb.ComponentState_Absent:
		if s.Primary == captureID {
			s.Inferior.UpdateStatus(input)
			original := s.Primary
			s.clearPrimary()
			if !s.hasRole(RoleSecondary) {
				// primary is stopped and there is no secondary, transit to Absent.
				log.Info("primary is stopped during Commit",
					zap.Any("status", input),
					zap.String("captureID", captureID),
					zap.Any("statemachine", s))
				s.State = SchedulerStatusWorking
				return nil, true, nil
			}
			// Primary is stopped, promote secondary to primary.
			secondary, _ := s.GetRole(RoleSecondary)
			err := s.promoteSecondary(secondary)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			log.Info("state promote secondary",
				zap.Any("status", input),
				zap.String("secondary", captureID),
				zap.Any("statemachine", s),
				zap.String("original", original))
			return s.Inferior.NewAddInferiorMessage(s.Primary, false), false, nil
		} else if s.isInRole(captureID, RoleSecondary) {
			// As it sends Remove to the original primary
			// upon entering Commit state. Do not change state and wait
			// the original primary reports its inferior.
			log.Info("secondary is stopped during Commit",
				zap.Any("status", input),
				zap.String("captureID", captureID),
				zap.Any("statemachine", s))
			err := s.clearCapture(captureID, RoleSecondary)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			if s.Primary == "" {
				// If there is no primary and secondary is stopped, transit to Absent.
				s.State = SchedulerStatusAbsent
			}
			return nil, true, nil
		} else if s.isInRole(captureID, RoleUndetermined) {
			log.Info("server is stopped during Commit",
				zap.Any("status", input),
				zap.String("captureID", captureID),
				zap.Any("statemachine", s))
			err := s.clearCapture(captureID, RoleUndetermined)
			return nil, false, errors.Trace(err)
		}

	case heartbeatpb.ComponentState_Working:
		// primary is still working
		if s.Primary == captureID {
			s.Inferior.UpdateStatus(input)
			if s.hasRole(RoleSecondary) {
				// Original primary is not stopped, ask for stopping.
				return s.Inferior.NewRemoveInferiorMessage(captureID), false, nil
			}

			// There are three cases for empty secondary.
			//
			// 1. Secondary has promoted to primary, and the new primary is
			//    working, transit to working.
			// 2. Secondary has shutdown during Commit, the original primary
			//    does not receive Remove request and continues to
			//    working, transit to working.
			// 3. Secondary has shutdown during Commit, we receive a message
			//    before the original primary receives Remove request.
			//    Transit to Working, and wait for the next state of
			//    the primary, Stopping or Stopped.
			s.State = SchedulerStatusWorking
			return nil, true, nil
		}
		return nil, false, s.multiplePrimaryError(
			input, captureID, "multiple primary")

	case heartbeatpb.ComponentState_Stopping:
		if s.Primary == captureID && s.hasRole(RoleSecondary) {
			s.Inferior.UpdateStatus(input)
			return nil, false, nil
		} else if s.isInRole(captureID, RoleUndetermined) {
			log.Info("server is stopping during Commit",
				zap.Any("status", input),
				zap.String("captureID", captureID),
				zap.Any("statemachine", s))
			return nil, false, nil
		}

	case heartbeatpb.ComponentState_Preparing:
	}
	log.Warn("ignore input, unexpected state",
		zap.Any("status", input),
		zap.String("captureID", captureID),
		zap.Any("statemachine", s))
	return nil, false, nil
}

//nolint:unparam
func (s *StateMachine) pollOnWorking(
	input InferiorStatus, captureID model.CaptureID,
) (bool, error) {
	switch input.GetInferiorState() {
	case heartbeatpb.ComponentState_Working:
		if s.Primary == captureID {
			s.Inferior.UpdateStatus(input)
			return false, nil
		}
		return false, s.multiplePrimaryError(
			input, captureID, "multiple primary")

	case heartbeatpb.ComponentState_Absent:
	case heartbeatpb.ComponentState_Preparing:
	case heartbeatpb.ComponentState_Prepared:
	case heartbeatpb.ComponentState_Stopping:
		// wait stop
	case heartbeatpb.ComponentState_Stopped:
		if s.Primary == captureID {
			s.Inferior.UpdateStatus(input)
			// Primary is stopped, but we still has secondary.
			// Clear primary and promote secondary when it's prepared.
			log.Info("primary is stopped during Working",
				zap.Any("status", input),
				zap.String("captureID", captureID),
				zap.Any("statemachine", s.ID.String()))
			s.clearPrimary()
			s.State = SchedulerStatusAbsent
			return true, nil
		}
	}
	log.Warn("ignore input, unexpected state",
		zap.Any("status", input),
		zap.String("captureID", captureID),
		zap.Any("statemachine", s.ID.String()))
	return false, nil
}

//nolint:unparam
func (s *StateMachine) pollOnRemoving(
	input InferiorStatus, captureID model.CaptureID,
) (rpc.Message, bool, error) {
	switch input.GetInferiorState() {
	case heartbeatpb.ComponentState_Prepared,
		heartbeatpb.ComponentState_Preparing,
		heartbeatpb.ComponentState_Working:
		return s.Inferior.NewRemoveInferiorMessage(captureID), false, nil
	case heartbeatpb.ComponentState_Absent, heartbeatpb.ComponentState_Stopped:
		var err error
		if s.Primary == captureID {
			s.clearPrimary()
		} else if s.isInRole(captureID, RoleSecondary) {
			err = s.clearCapture(captureID, RoleSecondary)
		} else {
			err = s.clearCapture(captureID, RoleUndetermined)
		}
		if err != nil {
			log.Warn("remove server with error",
				zap.Any("status", input),
				zap.String("captureID", captureID),
				zap.Any("statemachine", s),
				zap.Error(err))
		}
		return nil, false, nil
	case heartbeatpb.ComponentState_Stopping:
		//wait for stopping
		return nil, false, nil
	}
	log.Warn("ignore input, unexpected  state",
		zap.Any("status", input),
		zap.String("captureID", captureID),
		zap.Any("statemachine", s))
	return nil, false, nil
}

func (s *StateMachine) HandleInferiorStatus(
	input InferiorStatus, from model.CaptureID,
) ([]rpc.Message, error) {
	return s.poll(input, from)
}

func (s *StateMachine) HandleAddInferior(
	captureID model.CaptureID,
) ([]rpc.Message, error) {
	// Ignore add inferior if it's not in Absent state.
	if s.State != SchedulerStatusAbsent {
		log.Warn("add inferior is ignored",
			zap.String("captureID", captureID),
			zap.Any("statemachine", s))
		return nil, nil
	}
	err := s.setCapture(captureID, RoleSecondary)
	if err != nil {
		return nil, errors.Trace(err)
	}
	oldState := s.State
	status := s.Inferior.NewInferiorStatus(heartbeatpb.ComponentState_Absent)
	msgs, err := s.poll(status, captureID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Info("state transition, add ingferior",
		zap.Any("status", status),
		zap.String("captureID", captureID),
		zap.Any("statemachine", s.ID.String()),
		zap.Stringer("old", oldState),
		zap.Stringer("new", s.State))
	return msgs, nil
}

func (s *StateMachine) HandleMoveInferior(
	dest model.CaptureID,
) ([]rpc.Message, error) {
	// Ignore move inferior if it has been removed already.
	if s.HasRemoved() {
		log.Warn("move inferior is ignored",
			zap.Any("statemachine", s))
		return nil, nil
	}
	// Ignore move inferior if
	// 1) it's not in Working state or
	// 2) the dest server is the primary.
	if s.State != SchedulerStatusWorking || s.Primary == dest {
		log.Warn("move inferior is ignored",
			zap.Any("statemachine", s))
		return nil, nil
	}
	oldState := s.State
	s.State = SchedulerStatusPrepare
	err := s.setCapture(dest, RoleSecondary)
	if err != nil {
		log.Info("move inferior is failed",
			zap.Stringer("new", s.State),
			zap.Any("statemachine", s),
			zap.Stringer("old", oldState),
			zap.Error(err))
		return nil, errors.Trace(err)
	}
	log.Info("state transition, move inferior",
		zap.Stringer("new", s.State),
		zap.Any("statemachine", s),
		zap.Stringer("old", oldState))
	status := s.Inferior.NewInferiorStatus(heartbeatpb.ComponentState_Absent)
	return s.poll(status, dest)
}

func (s *StateMachine) HandleRemoveInferior() ([]rpc.Message, error) {
	// Ignore remove inferior if it has been removed already.
	if s.HasRemoved() {
		log.Warn("remove inferior is ignored",
			zap.Any("statemachine", s))
		return nil, nil
	}
	// Ignore remove inferior if it's not in Working state.
	if s.State != SchedulerStatusWorking {
		log.Warn("remove inferior is ignored",
			zap.Any("statemachine", s))
		return nil, nil
	}
	oldState := s.State
	s.State = SchedulerStatusRemoving
	log.Info("state transition, remove inferiror",
		zap.Any("statemachine", s),
		zap.Stringer("old", oldState))
	// fake status to trigger a stop message
	status := s.Inferior.NewInferiorStatus(heartbeatpb.ComponentState_Working)
	return s.poll(status, s.Primary)
}

// HandleCaptureShutdown handle server shutdown event.
// Besides returning messages and errors, it also returns a bool to indicate
// whether s is affected by the server shutdown.
func (s *StateMachine) HandleCaptureShutdown(
	captureID model.CaptureID,
) ([]rpc.Message, bool, error) {
	_, ok := s.Servers[captureID]
	if !ok {
		// r is not affected by the server shutdown.
		return nil, false, nil
	}
	// The server has shutdown, the inferior has stopped.
	status := s.Inferior.NewInferiorStatus(heartbeatpb.ComponentState_Stopped)
	oldState := s.State
	msgs, err := s.poll(status, captureID)
	log.Info("state transition, server shutdown",
		zap.Any("statemachine", s),
		zap.Stringer("old", oldState),
		zap.Stringer("new", s.State))
	return msgs, true, errors.Trace(err)
}

func (s *StateMachine) HasRemoved() bool {
	// It has been removed successfully if it's state is Removing,
	// and there is no server has it.
	return s.State == SchedulerStatusRemoving && len(s.Servers) == 0
}
