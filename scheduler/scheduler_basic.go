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

type BasicScheduler struct {
	batchSize int
}

func NewBasicScheduler(batchSize int) *BasicScheduler {
	return &BasicScheduler{batchSize: batchSize}
}

func (b *BasicScheduler) Name() string {
	return "basic-scheduler"
}

func (b *BasicScheduler) Schedule(
	allInferiors []Inferior,
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	stateMachines Map[InferiorID, *StateMachine],
) []*ScheduleTask {
	tasks := make([]*ScheduleTask, 0)
	lenEqual := len(allInferiors) == stateMachines.Len()
	allFind := true
	newInferiors := make([]InferiorID, 0)
	for _, inf := range allInferiors {
		if len(newInferiors) >= b.batchSize {
			break
		}
		st, ok := stateMachines.Get(inf.GetID())
		if !ok {
			newInferiors = append(newInferiors, inf.GetID())
			// The inferior ID is not in the state machine means the two sets are
			// not identical.
			allFind = false
			continue
		}
		// absent status means we should schedule it again
		if st.State == SchedulerStatusAbsent {
			newInferiors = append(newInferiors, inf.GetID())
		}
	}

	// Build add inferior tasks.
	if len(newInferiors) > 0 {
		captureIDs := make([]model.CaptureID, 0, len(aliveCaptures))
		for captureID, _ := range aliveCaptures {
			captureIDs = append(captureIDs, captureID)
		}

		if len(captureIDs) == 0 {
			// this should never happen, if no capture can be found
			// for a cluster with n captures, n should be at least 2
			// only n - 1 captures can be in the `stopping` at the same time.
			log.Warn("cannot found capture when add new inferior",
				zap.Any("allCaptureStatus", aliveCaptures))
			return tasks
		}
		tasks = append(
			tasks, newBurstAddInferiors(newInferiors, captureIDs))
	}

	// Build remove inferior tasks.
	// For most of the time, remove inferiors are unlikely to happen.
	//
	// Fast path for check whether two sets are identical
	if !lenEqual || !allFind {
		// The two sets are not identical. We need to build a map to find removed inferiors.
		intersectionIDS := NewBtreeMap[InferiorID, struct{}]()
		for _, inf := range allInferiors {
			ok := stateMachines.Has(inf.GetID())
			if ok {
				intersectionIDS.ReplaceOrInsert(inf.GetID(), struct{}{})
			}
		}
		rmInferiorIDs := make([]InferiorID, 0)
		stateMachines.Ascend(func(key InferiorID, value *StateMachine) bool {
			ok := intersectionIDS.Has(key)
			if !ok {
				rmInferiorIDs = append(rmInferiorIDs, key)
			}
			return true
		})
		removeInferiorTasks := newBurstRemoveInferiors(rmInferiorIDs, stateMachines)
		if removeInferiorTasks != nil {
			tasks = append(tasks, removeInferiorTasks)
		}
	}
	return tasks
}

// newBurstAddInferiors add each new inferior to captures in a round-robin way.
func newBurstAddInferiors(newInferiors []InferiorID, captureIDs []model.CaptureID,
) *ScheduleTask {
	idx := 0
	addInferiorTasks := make([]*AddInferior, 0, len(newInferiors))
	for _, infID := range newInferiors {
		targetCapture := captureIDs[idx]
		addInferiorTasks = append(addInferiorTasks, &AddInferior{
			ID:        infID,
			CaptureID: targetCapture,
		})
		log.Info("burst add inferior",
			zap.String("inferior", infID.String()),
			zap.String("captureID", targetCapture))

		idx++
		if idx >= len(captureIDs) {
			idx = 0
		}
	}
	return &ScheduleTask{
		BurstBalance: &BurstBalance{
			AddInferiors: addInferiorTasks,
		},
	}
}

func newBurstRemoveInferiors(
	rmInferiors []InferiorID,
	stateMachines Map[InferiorID, *StateMachine],
) *ScheduleTask {
	removeTasks := make([]*RemoveInferior, 0, len(rmInferiors))
	for _, id := range rmInferiors {
		ccf, _ := stateMachines.Get(id)
		var captureID model.CaptureID = ccf.Primary

		if ccf.Primary == "" {
			log.Warn("primary or secondary not found for removed inferior,"+
				"this may happen if the capture shutdown",
				zap.Any("ID", id.String()))
			continue
		}
		removeTasks = append(removeTasks, &RemoveInferior{
			ID:        id,
			CaptureID: captureID,
		})
		log.Info("burst remove inferior",
			zap.String("captureID", captureID),
			zap.Any("ID", id.String()))
	}

	if len(removeTasks) == 0 {
		return nil
	}

	return &ScheduleTask{
		BurstBalance: &BurstBalance{
			RemoveInferiors: removeTasks,
		},
	}
}
