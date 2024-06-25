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
	"github.com/pingcap/tiflow/cdc/model"
)

// ScheduleTask is a schedule task that wraps add/move/remove inferior tasks.
type ScheduleTask struct { //nolint:revive
	MoveInferior   *MoveInferior
	AddInferior    *AddInferior
	RemoveInferior *RemoveInferior
	BurstBalance   *BurstBalance
}

// Name returns the name of a schedule task.
func (s *ScheduleTask) Name() string {
	if s.MoveInferior != nil {
		return "moveInferior"
	} else if s.AddInferior != nil {
		return "addInferior"
	} else if s.RemoveInferior != nil {
		return "removeInferior"
	} else if s.BurstBalance != nil {
		return "burstBalance"
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
	if s.BurstBalance != nil {
		return s.BurstBalance.String()
	}
	return ""
}

// BurstBalance for set up or unplanned TiCDC node failure.
// Supervisor needs to balance interrupted inferior as soon as possible.
type BurstBalance struct {
	AddInferiors    []*AddInferior
	RemoveInferiors []*RemoveInferior
	MoveInferiors   []*MoveInferior
}

func (b BurstBalance) String() string {
	if len(b.AddInferiors) != 0 {
		return fmt.Sprintf("BurstBalance, add inferiors: %v", b.AddInferiors)
	}
	if len(b.RemoveInferiors) != 0 {
		return fmt.Sprintf("BurstBalance, remove inferiors: %v", b.RemoveInferiors)
	}
	if len(b.MoveInferiors) != 0 {
		return fmt.Sprintf("BurstBalance, move inferiors: %v", b.MoveInferiors)
	}
	return "BurstBalance, no tasks"
}

// MoveInferior is a schedule task for moving a inferior.
type MoveInferior struct {
	ID          InferiorID
	DestCapture model.CaptureID
}

func (t MoveInferior) String() string {
	return fmt.Sprintf("MoveInferior, span: %s, dest: %s",
		t.ID.String(), t.DestCapture)
}

// AddInferior is a schedule task for adding an inferior.
type AddInferior struct {
	ID        InferiorID
	CaptureID model.CaptureID
}

func (t AddInferior) String() string {
	return fmt.Sprintf("AddInferior, span: %s, capture: %s",
		t.ID.String(), t.CaptureID)
}

// RemoveInferior is a schedule task for removing an inferior.
type RemoveInferior struct {
	ID        InferiorID
	CaptureID model.CaptureID
}

func (t RemoveInferior) String() string {
	return fmt.Sprintf("RemoveInferior, ID: %s, capture: %s",
		t.ID.String(), t.CaptureID)
}
