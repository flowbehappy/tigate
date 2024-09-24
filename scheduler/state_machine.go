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
	"github.com/flowbehappy/tigate/pkg/node"
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

type StateMachine[ID comparable] struct {
	ID    ID
	State SchedulerStatus
	// Primary is the server ID that is currently running the inferior.
	Primary node.ID
	// Primary is the server ID that is this inferior should be moved to
	Secondary node.ID

	// Inferior handles the real logic
	Inferior Inferior

	lastMsgTime time.Time
	removed     bool
}

// NewStateMachine build a state machine from all server reported status
// it could be called after a scheduler is bootstrapped
func NewStateMachine[ID comparable](
	id ID,
	inferiorStatus map[node.ID]any,
	inferior Inferior,
) *StateMachine[ID] {
	sm := &StateMachine[ID]{
		ID:          id,
		Inferior:    inferior,
		lastMsgTime: time.Time{},
	}
	for captureID, status := range inferiorStatus {
		sm.Inferior.UpdateStatus(status)
		if len(sm.Primary) != 0 {
			log.Panic("multi node reported working state for the same task",
				zap.Any("status", inferiorStatus),
				zap.Error(sm.multiplePrimaryError(
					status, captureID, "multiple primary",
					zap.Any("primary", sm.Primary))))
		}
		sm.Primary = captureID
	}

	// Build state from primary, secondary and captures.
	if len(sm.Primary) != 0 {
		sm.State = SchedulerStatusWorking
		log.Info("initialize a working state state machine",
			zap.Any("statemachine", sm.ID))
	} else {
		sm.State = SchedulerStatusAbsent
	}
	log.Info("initialize state machine",
		zap.Any("id", sm.ID),
		zap.String("state", sm.State.String()))

	return sm
}

//nolint:unparam
func (s *StateMachine[ID]) inconsistentError(
	input any, captureID model.CaptureID,
	msg string, fields ...zap.Field,
) error {
	fields = append(fields, []zap.Field{
		zap.String("captureID", captureID),
		zap.Any("state", input),
		zap.Any("statemachine", s.ID),
	}...)
	log.L().WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
	return errors.New("inconsistent error: " + msg)
}

func (s *StateMachine[ID]) multiplePrimaryError(
	input any, node node.ID, msg string, fields ...zap.Field,
) error {
	fields = append(fields, []zap.Field{
		zap.Any("node", node),
		zap.Any("state", input),
		zap.Any("statemachine", s.ID),
	}...)
	log.L().WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
	return errors.New("inconsistent error: " + msg)
}

// HandleInferiorStatus transit state based on input and the current state.
func (s *StateMachine[ID]) HandleInferiorStatus(
	state heartbeatpb.ComponentState, status any, id node.ID,
) {
	if s.Primary != id {
		return
	}

	oldState := s.State
	switch s.State {
	case SchedulerStatusCommiting:
		s.pollOnCommit(state, status, id)
	case SchedulerStatusWorking:
		s.pollOnWorking(state, status, id)
	case SchedulerStatusRemoving:
		s.pollOnRemoving(state, status, id)
	default:
		log.Panic("state unknown",
			zap.Any("id", id),
			zap.Any("status", status),
			zap.Any("stm", s))
	}
	if oldState != s.State {
		log.Info("state transition, poll",
			zap.Any("status", s.ID),
			zap.Any("captureID", id),
			zap.Stringer("old", oldState),
			zap.Stringer("new", s.State))
	}
}

func (s *StateMachine[ID]) pollOnCommit(
	state heartbeatpb.ComponentState, status any, id node.ID,
) {
	switch state {
	case heartbeatpb.ComponentState_Stopped, heartbeatpb.ComponentState_Absent:
		s.Inferior.UpdateStatus(status)
		s.Primary = ""
		// primary is stopped and there is no secondary, transit to Absent.
		log.Info("primary is stopped during Commit",
			zap.Any("id", id),
			zap.Any("statemachine", s.ID))
		s.State = SchedulerStatusAbsent
	case heartbeatpb.ComponentState_Working:
		s.Inferior.UpdateStatus(status)
		s.State = SchedulerStatusWorking
	default:
		log.Warn("ignore input, unexpected state",
			zap.Any("id", id),
			zap.Any("statemachine", s.ID))
	}
}

func (s *StateMachine[ID]) pollOnWorking(
	state heartbeatpb.ComponentState, status any, id node.ID,
) {
	switch state {
	case heartbeatpb.ComponentState_Working:
		s.Inferior.UpdateStatus(status)
	case heartbeatpb.ComponentState_Absent, heartbeatpb.ComponentState_Stopped:
		s.Inferior.UpdateStatus(status)
		s.Primary = ""
		s.State = SchedulerStatusAbsent
	default:
		log.Warn("ignore input, unexpected state",
			zap.Any("id", id),
			zap.Any("statemachine", s.ID))
	}
}

//nolint:unparam
func (s *StateMachine[ID]) pollOnRemoving(
	state heartbeatpb.ComponentState, status any, id node.ID,
) {
	switch state {
	case heartbeatpb.ComponentState_Working:
		s.Inferior.UpdateStatus(status)
		return
	case heartbeatpb.ComponentState_Absent, heartbeatpb.ComponentState_Stopped:
		if s.Secondary != "" {
			// primary is stopped and reported last status
			s.Inferior.UpdateStatus(status)
			s.Primary = s.Secondary
			s.Secondary = ""
			s.State = SchedulerStatusCommiting
			return
		}
		// mark the statemachine as removed
		s.removed = true
		s.Primary = ""
	}
	log.Warn("ignore input, unexpected  state",
		zap.Any("id", id),
		zap.Any("statemachine", s.ID))
}

func (s *StateMachine[ID]) HandleAddInferior(
	id node.ID,
) {
	// Ignore add inferior if it's not in Absent state.
	if s.State != SchedulerStatusAbsent {
		log.Warn("add inferior is ignored",
			zap.Any("id", id),
			zap.Any("statemachine", s.ID))
		return
	}
	s.Primary = id
	oldState := s.State
	s.State = SchedulerStatusCommiting

	log.Info("state transition, add inferior",
		zap.Any("id", id),
		zap.Any("statemachine", s.ID),
		zap.Stringer("old", oldState),
		zap.Stringer("new", s.State))
}

func (s *StateMachine[ID]) HandleMoveInferior(
	dest node.ID,
) {
	// Ignore move inferior if it has been removed already.
	if s.HasRemoved() {
		log.Warn("move inferior is ignored",
			zap.Any("statemachine", s.ID))
		return
	}
	// Ignore move inferior if
	// 1) it's not in Working state or
	// 2) the dest server is the primary.
	if s.State != SchedulerStatusWorking || s.Primary == dest {
		log.Warn("move inferior is ignored",
			zap.Any("statemachine", s.ID))
		return
	}
	oldState := s.State
	s.State = SchedulerStatusRemoving
	s.Secondary = dest
	log.Info("state transition, move inferior",
		zap.Stringer("new", s.State),
		zap.Any("statemachine", s.ID),
		zap.Stringer("old", oldState))
}

func (s *StateMachine[ID]) HandleRemoveInferior() {
	// Ignore remove inferior if it has been removed already.
	if s.HasRemoved() {
		log.Warn("remove inferior is ignored",
			zap.Any("statemachine", s.ID))
		return
	}
	if s.State == SchedulerStatusAbsent {
		log.Info("remove an absent stm",
			zap.Any("statemachine", s.ID))
		s.removed = true
		// clear the node info
		s.Primary = ""
		s.Secondary = ""
		return
	}

	// Ignore remove inferior if it's not in Working state.
	if s.State == SchedulerStatusRemoving {
		log.Warn("remove inferior is ignored",
			zap.Any("statemachine", s.ID))
		if s.Secondary != "" {
			log.Info("secondary is not empty, set it to empty",
				zap.String("primary", s.Primary.String()),
				zap.String("secondary", s.Secondary.String()),
				zap.Any("statemachine", s.ID))
			s.Secondary = ""
		}
		return
	}
	oldState := s.State
	s.State = SchedulerStatusRemoving
	log.Info("state transition, remove inferior",
		zap.Any("statemachine", s.ID),
		zap.Stringer("old", oldState))
}

// HandleCaptureShutdown handle server shutdown event.
// Besides returning messages and errors, it also returns a bool to indicate
// whether s is affected by the server shutdown.
func (s *StateMachine[ID]) HandleCaptureShutdown(
	id node.ID,
) bool {
	if s.Primary != id && s.Secondary != id {
		return false
	}
	oldState := s.State
	switch oldState {
	case SchedulerStatusAbsent, SchedulerStatusCommiting, SchedulerStatusWorking:
		// primary node is stopped, set to absent to reschedule
		s.Primary = ""
		s.State = SchedulerStatusAbsent
	case SchedulerStatusRemoving:
		// check if we are moving this state machine
		if s.Secondary == "" {
			// no secondary capture, the primary must be stopped,
			// move the absent status to reschedule it
			s.Primary = ""
			s.State = SchedulerStatusAbsent
		} else {
			if s.Secondary == id {
				// destination capture is stopped during moving, clear secondary node
				s.Secondary = ""
				// move to working state, so we received a stopped status we can reschedule it
				s.State = SchedulerStatusWorking
			} else {
				// primary capture is stopped, move to secondary
				s.State = SchedulerStatusCommiting
			}
		}
	}
	log.Info("state transition, server shutdown",
		zap.Any("statemachine", s.ID),
		zap.Any("id", id),
		zap.Stringer("old", oldState),
		zap.Stringer("new", s.State))
	return true
}

func (s *StateMachine[ID]) HasRemoved() bool {
	return s.removed
}

func (s *StateMachine[ID]) GetSchedulingMessage() *messaging.TargetMessage {
	if s.State == SchedulerStatusWorking {
		return nil
	}
	if time.Since(s.lastMsgTime) < 500*time.Millisecond {
		return nil
	}
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
