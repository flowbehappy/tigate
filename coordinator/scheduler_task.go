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
	"fmt"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/node"
)

// ScheduleTask is a schedule task that wraps add/move/remove inferior tasks.
type ScheduleTask struct { //nolint:revive
	MoveInferior   *MoveInferior
	AddInferior    *AddInferior
	RemoveInferior *RemoveInferior
}

// Name returns the name of a schedule task.
func (s *ScheduleTask) Name() string {
	if s.MoveInferior != nil {
		return "moveInferior"
	} else if s.AddInferior != nil {
		return "addInferior"
	} else if s.RemoveInferior != nil {
		return "removeInferior"
	}
	return "unknown"
}

func (s *ScheduleTask) String() string {
	if s.MoveInferior != nil {
		return s.MoveInferior.String()
	}
	if s.AddInferior != nil {
		return s.AddInferior.String()
	}
	if s.RemoveInferior != nil {
		return s.RemoveInferior.String()
	}
	return ""
}

// MoveInferior is a schedule task for moving a inferior.
type MoveInferior struct {
	ID          common.MaintainerID
	DestCapture node.ID
}

func (t MoveInferior) String() string {
	return fmt.Sprintf("MoveInferior, span: %s, dest: %s",
		t.ID, t.DestCapture)
}

// AddInferior is a schedule task for adding an inferior.
type AddInferior struct {
	ID        common.MaintainerID
	CaptureID node.ID
}

func (t AddInferior) String() string {
	return fmt.Sprintf("AddInferior, span: %s, server: %s",
		t.ID, t.CaptureID)
}

// RemoveInferior is a schedule task for removing an inferior.
type RemoveInferior struct {
	ID common.MaintainerID
}

func (t RemoveInferior) String() string {
	return fmt.Sprintf("RemoveInferior, ID: %s", t.ID)
}
