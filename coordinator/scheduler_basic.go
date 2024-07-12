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
	"github.com/flowbehappy/tigate/scheduler"
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
	allInferiors []model.ChangeFeedID,
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	stateMachines map[model.ChangeFeedID]*scheduler.StateMachine,
) []*scheduler.ScheduleTask {
	tasks := make([]*scheduler.ScheduleTask, 0)
	lenEqual := len(allInferiors) == len(stateMachines)
	allFind := true
	newInferiors := make([]model.ChangeFeedID, 0)
	for _, inf := range allInferiors {
		if len(newInferiors) >= b.batchSize {
			break
		}
		st, ok := stateMachines[inf]
		if !ok {
			newInferiors = append(newInferiors, inf)
			// The inferior ID is not in the state machine means the two sets are
			// not identical.
			allFind = false
			continue
		}
		// absent status means we should schedule it again
		if st.State == scheduler.SchedulerStatusAbsent {
			newInferiors = append(newInferiors, inf)
		}
	}

	// Build add inferior tasks.
	if len(newInferiors) > 0 {
		captureIDs := make([]model.CaptureID, 0, len(aliveCaptures))
		for captureID, _ := range aliveCaptures {
			captureIDs = append(captureIDs, captureID)
		}

		if len(captureIDs) == 0 {
			// this should never happen, if no server can be found
			// for a cluster with n captures, n should be at least 2
			// only n - 1 captures can be in the `stopping` at the same time.
			log.Warn("cannot found server when add new inferior",
				zap.Any("allCaptureStatus", aliveCaptures))
			return tasks
		}
		tasks = append(
			tasks, newBurstAddInferiors(newInferiors, captureIDs)...)
	}

	// Build remove inferior tasks.
	// For most of the time, remove inferiors are unlikely to happen.
	//
	// Fast path for check whether two sets are identical
	if !lenEqual || !allFind {
		// The two sets are not identical. We need to build a map to find removed inferiors.
		intersectionIDS := make(map[model.ChangeFeedID]struct{})
		for _, inf := range allInferiors {
			_, ok := stateMachines[inf]
			if ok {
				intersectionIDS[inf] = struct{}{}
			}
		}
		rmInferiorIDs := make([]model.ChangeFeedID, 0)
		for key, _ := range stateMachines {
			_, ok := intersectionIDS[key]
			if !ok {
				rmInferiorIDs = append(rmInferiorIDs, key)
			}
		}
		removeInferiorTasks := newBurstRemoveInferiors(rmInferiorIDs, stateMachines)
		if removeInferiorTasks != nil {
			tasks = append(tasks, removeInferiorTasks...)
		}
	}
	return tasks
}

// newBurstAddInferiors add each new inferior to captures in a round-robin way.
func newBurstAddInferiors(newInferiors []model.ChangeFeedID, captureIDs []model.CaptureID,
) []*scheduler.ScheduleTask {
	idx := 0
	addInferiorTasks := make([]*scheduler.ScheduleTask, 0, len(newInferiors))
	for _, infID := range newInferiors {
		targetCapture := captureIDs[idx]
		addInferiorTasks = append(addInferiorTasks,
			&scheduler.ScheduleTask{
				AddInferior: &scheduler.AddInferior{
					ID:        ChangefeedID(infID),
					CaptureID: targetCapture,
				}})
		log.Info("burst add inferior",
			zap.String("inferior", infID.String()),
			zap.String("captureID", targetCapture))

		idx++
		if idx >= len(captureIDs) {
			idx = 0
		}
	}
	return addInferiorTasks
}

func newBurstRemoveInferiors(
	rmInferiors []model.ChangeFeedID,
	stateMachines map[model.ChangeFeedID]*scheduler.StateMachine,
) []*scheduler.ScheduleTask {
	removeTasks := make([]*scheduler.ScheduleTask, 0, len(rmInferiors))
	for _, id := range rmInferiors {
		ccf := stateMachines[id]
		var captureID model.CaptureID = ccf.Primary

		if ccf.Primary == "" {
			log.Warn("primary or secondary not found for removed inferior,"+
				"this may happen if the server shutdown",
				zap.Any("ID", id.String()))
			continue
		}
		removeTasks = append(removeTasks, &scheduler.ScheduleTask{
			RemoveInferior: &scheduler.RemoveInferior{
				ID:        ChangefeedID(id),
				CaptureID: captureID,
			},
		})
		log.Info("burst remove inferior",
			zap.String("captureID", captureID),
			zap.Any("ID", id.String()))
	}

	if len(removeTasks) == 0 {
		return nil
	}

	return removeTasks
}
