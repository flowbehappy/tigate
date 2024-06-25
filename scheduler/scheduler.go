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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

type Scheduler interface {
	Name() string
	Schedule(
		allInferiors []Inferior,
		aliveCaptures map[model.CaptureID]*CaptureStatus,
		stateMachines Map[InferiorID, *StateMachine],
	) []*ScheduleTask
}

var _ Scheduler = &basicScheduler{}

type basicScheduler struct {
	batchSize int
}

func NewBasicScheduler(batchSize int) Scheduler {
	return &basicScheduler{
		batchSize: batchSize,
	}
}

func (b *basicScheduler) Name() string {
	return "basic-scheduler"
}

func (b *basicScheduler) Schedule(
	allInferiors []Inferior,
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	stateMachines Map[InferiorID, *StateMachine],
) []*ScheduleTask {
	tasks := make([]*ScheduleTask, 0)
	lenEqual := len(allInferiors) == stateMachines.Len()
	allFind := true
	newInferiors := make([]InferiorID, 0)
	for _, inferior := range allInferiors {
		if len(newInferiors) >= b.batchSize {
			break
		}
		stateMachine, ok := stateMachines.Get(inferior.GetID())
		if !ok {
			newInferiors = append(newInferiors, inferior.GetID())
			allFind = false
			continue
		}
		if stateMachine.State == SchedulerStatusRemoving {
			newInferiors = append(newInferiors, inferior.GetID())
		}
	}

	// Build add tasks.
	if len(newInferiors) > 0 {
		captureIDs := make([]model.CaptureID, 0, len(aliveCaptures))
		for captureID, _ := range aliveCaptures {
			captureIDs = append(captureIDs, captureID)
		}

		if len(captureIDs) == 0 {
			log.Warn("cannot found capture when add new inferior",
				zap.Any("allCaptureStatus", aliveCaptures))
			return tasks
		}
		tasks = append(
			tasks, newBurstAddInferior(newInferiors, captureIDs))
	}

	if !lenEqual || !allFind {
		// The two sets are not identical. We need to build a map to find removed inferiors.
		intersectionSet := NewBtreeMap[InferiorID, struct{}]()
		for _, inferior := range allInferiors {
			_, ok := stateMachines.Get(inferior.GetID())
			if ok {
				intersectionSet.ReplaceOrInsert(inferior.GetID(), struct{}{})
			}
		}
		rmInferiors := make([]InferiorID, 0)
		stateMachines.Ascend(func(key InferiorID, value *StateMachine) bool {
			ok := intersectionSet.Has(key)
			if !ok {
				rmInferiors = append(rmInferiors, key)
			}
			return true
		})
		removeTasks := newBurstRemoveInferiors(rmInferiors, stateMachines)
		if removeTasks != nil {
			tasks = append(tasks, removeTasks)
		}
	}
	return tasks
}

// newBurstAddInferior add each new inferior to captures in a round-robin way.
func newBurstAddInferior(newInferiors []InferiorID,
	captureIDs []model.CaptureID) *ScheduleTask {
	idx := 0
	inferiors := make([]*AddInferior, 0, len(newInferiors))
	for _, inferiorID := range newInferiors {
		targetCapture := captureIDs[idx]
		inferiors = append(inferiors, &AddInferior{
			ID:        inferiorID,
			CaptureID: targetCapture,
		})
		log.Info("burst add inferior",
			zap.String("inferior", inferiorID.String()),
			zap.String("captureID", targetCapture))

		idx++
		if idx >= len(captureIDs) {
			idx = 0
		}
	}
	return &ScheduleTask{
		BurstBalance: &BurstBalance{
			AddInferiors: inferiors,
		},
	}
}

func newBurstRemoveInferiors(
	rmInferiorIDs []InferiorID,
	stateMachines Map[InferiorID, *StateMachine],
) *ScheduleTask {
	removeInferiors := make([]*RemoveInferior, 0, len(rmInferiorIDs))
	for _, inferiorID := range rmInferiorIDs {
		stateMachine, _ := stateMachines.Get(inferiorID)
		var captureID = stateMachine.Primary

		if stateMachine.Primary == "" {
			log.Warn("primary or secondary not found for removed inferiorID,"+
				"this may happen if the capture shutdown",
				zap.Any("ID", inferiorID))
			continue
		}
		removeInferiors = append(removeInferiors, &RemoveInferior{
			ID:        inferiorID,
			CaptureID: captureID,
		})
		log.Info("burst remove inferiorID",
			zap.String("captureID", captureID),
			zap.Any("ID", inferiorID))
	}

	if len(removeInferiors) == 0 {
		return nil
	}

	return &ScheduleTask{
		BurstBalance: &BurstBalance{
			RemoveInferiors: removeInferiors,
		},
	}
}
