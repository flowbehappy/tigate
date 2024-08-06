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

	"github.com/flowbehappy/tigate/utils"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

type BasicScheduler struct {
	id                    InferiorID
	lastForceScheduleTime time.Time

	needAddInferior  bool
	addInferiorCache []InferiorID

	needRemoveInferior  bool
	removeInferiorCache []InferiorID
}

func NewBasicScheduler(id InferiorID) *BasicScheduler {
	return &BasicScheduler{
		lastForceScheduleTime: time.Now(),
	}
}

func (b *BasicScheduler) Name() string {
	return "basic-scheduler"
}

func (b *BasicScheduler) markNeedAddInferior() {
	b.needAddInferior = true
}

func (b *BasicScheduler) markNeedRemoveInferior() {
	b.needRemoveInferior = true
}

// hasPendingTask
func (b *BasicScheduler) hasPendingTask() bool {
	return len(b.addInferiorCache) > 0 || len(b.removeInferiorCache) > 0
}

func (b *BasicScheduler) Schedule(
	allInferiors utils.Map[InferiorID, Inferior],
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	stateMachines utils.Map[InferiorID, *StateMachine],
	batchSize int,
) (tasks []*ScheduleTask) {
	if !b.hasPendingTask() && time.Since(b.lastForceScheduleTime) > 120*time.Second {
		b.markNeedAddInferior()
		b.markNeedRemoveInferior()
		b.lastForceScheduleTime = time.Now()
	}

	// Build remove inferior tasks.
	if b.needRemoveInferior {
		// The two sets are not identical. We need to build a map to find removed inferiors.
		b.removeInferiorCache = make([]InferiorID, 0, batchSize)
		stateMachines.Ascend(func(key InferiorID, value *StateMachine) bool {
			ok := allInferiors.Has(key)
			if !ok {
				b.removeInferiorCache = append(b.removeInferiorCache, key)
			}
			return true
		})
		b.needRemoveInferior = false
		if len(b.removeInferiorCache) > 0 {
			log.Info("basic scheduler generate new remove inferiors cache",
				zap.String("id", b.id.String()),
				zap.Int("count", len(b.removeInferiorCache)))
		}
	}
	if len(b.removeInferiorCache) > 0 {
		batch := batchSize - len(tasks)
		if batchSize > len(b.removeInferiorCache) {
			batch = len(b.removeInferiorCache)
		}
		rmInferiors := b.removeInferiorCache[:batch]
		b.removeInferiorCache = b.removeInferiorCache[batch:]
		if len(b.removeInferiorCache) == 0 {
			// release for GC
			b.removeInferiorCache = nil
		}
		tasks = append(tasks, b.newBurstRemoveInferiors(rmInferiors, stateMachines)...)
		if len(rmInferiors) >= batchSize {
			return tasks
		}
	}

	// Build add inferior tasks.
	if b.needAddInferior {
		b.addInferiorCache = make([]InferiorID, 0, batchSize)
		allInferiors.Ascend(func(inf InferiorID, value Inferior) bool {
			st := value.GetStateMachine()
			if st == nil || st.State == SchedulerStatusAbsent {
				// add case 1: schedule a new inferior
				// add case 2: reschedule an absent inferior. Currently, we only reschedule each 2 minutes.
				// TODO: store absent inferiors in a separate map to trigger reschedule quickly.
				b.addInferiorCache = append(b.addInferiorCache, inf)
			}
			return true
		})
		b.needAddInferior = false
		if len(b.addInferiorCache) > 0 {
			log.Info("basic scheduler generate new add inferiors cache",
				zap.String("id", b.id.String()),
				zap.Int("count", len(b.addInferiorCache)))
		}
	}
	if len(b.addInferiorCache) > 0 {
		batch := batchSize
		if batchSize > len(b.addInferiorCache) {
			batch = len(b.addInferiorCache)
		}
		newInferiors := b.addInferiorCache[:batch]
		b.addInferiorCache = b.addInferiorCache[batch:]
		if len(b.addInferiorCache) == 0 {
			// release for GC
			b.addInferiorCache = nil
		}

		captureIDs := make([]model.CaptureID, 0, len(aliveCaptures))
		for captureID := range aliveCaptures {
			captureIDs = append(captureIDs, captureID)
		}
		if len(captureIDs) == 0 {
			// this should never happen, if no server can be found
			// for a cluster with n captures, n should be at least 2
			// only n - 1 captures can be in the `stopping` at the same time.
			log.Warn("cannot found server when add new inferior",
				zap.String("id", b.id.String()),
				zap.Any("allCaptureStatus", aliveCaptures))
			return tasks
		}
		tasks = append(tasks, b.newBurstAddInferiors(newInferiors, captureIDs)...)
		if len(newInferiors) >= batchSize {
			return tasks
		}
	}

	return tasks
}

// newBurstAddInferiors add each new inferior to captures in a round-robin way.
func (b *BasicScheduler) newBurstAddInferiors(newInferiors []InferiorID, captureIDs []model.CaptureID,
) []*ScheduleTask {
	idx := 0
	addInferiorTasks := make([]*ScheduleTask, 0, len(newInferiors))
	for _, infID := range newInferiors {
		targetCapture := captureIDs[idx]
		addInferiorTasks = append(addInferiorTasks,
			&ScheduleTask{
				AddInferior: &AddInferior{
					ID:        infID,
					CaptureID: targetCapture,
				}})
		log.Info("burst add inferior",
			zap.String("id", b.id.String()),
			zap.String("inferior", infID.String()),
			zap.String("captureID", targetCapture))

		idx++
		if idx >= len(captureIDs) {
			idx = 0
		}
	}
	return addInferiorTasks
}

// TODO: maybe remove task does not need captureID.
func (b *BasicScheduler) newBurstRemoveInferiors(
	rmInferiors []InferiorID,
	stateMachines utils.Map[InferiorID, *StateMachine],
) []*ScheduleTask {
	removeTasks := make([]*ScheduleTask, 0, len(rmInferiors))
	for _, id := range rmInferiors {
		state, _ := stateMachines.Get(id)
		var captureID string
		for server := range state.Servers {
			captureID = server
			break
		}
		if state.Primary == "" {
			log.Warn("primary or secondary not found for removed inferior,"+
				"this may happen if the server shutdown",
				zap.String("id", b.id.String()),
				zap.Any("ID", id.String()))
			continue
		}
		removeTasks = append(removeTasks, &ScheduleTask{
			RemoveInferior: &RemoveInferior{
				ID:        id,
				CaptureID: captureID,
			},
		})
		// log.Info("burst remove inferior",
		// 	zap.String("captureID", captureID),
		// 	zap.Any("ID", id.String()))
	}

	if len(removeTasks) == 0 {
		return nil
	}

	return removeTasks
}
