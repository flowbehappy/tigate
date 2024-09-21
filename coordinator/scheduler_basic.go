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
	"time"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type BasicScheduler struct {
	id                    common.CoordinatorID
	lastForceScheduleTime time.Time

	needAddInferior  bool
	addInferiorCache []common.MaintainerID

	needRemoveInferior  bool
	removeInferiorCache []common.MaintainerID
}

func NewBasicScheduler(id common.CoordinatorID) *BasicScheduler {
	return &BasicScheduler{
		id:                    id,
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
	allInferiors map[common.MaintainerID]scheduler.Inferior[common.MaintainerID],
	aliveCaptures map[node.ID]*CaptureStatus,
	stateMachines map[common.MaintainerID]*scheduler.StateMachine[common.MaintainerID],
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
		b.removeInferiorCache = make([]common.MaintainerID, 0, batchSize)
		for key, _ := range stateMachines {
			_, ok := allInferiors[key]
			if !ok {
				b.removeInferiorCache = append(b.removeInferiorCache, key)
			}
		}
		b.needRemoveInferior = false
		if len(b.removeInferiorCache) > 0 {
			log.Info("basic scheduler generate new remove inferiors cache",
				zap.Stringer("id", b.id),
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
		b.addInferiorCache = make([]common.MaintainerID, 0, batchSize)
		for inf, _ := range allInferiors {
			st := stateMachines[inf]
			if st == nil || st.State == scheduler.SchedulerStatusAbsent {
				// add case 1: schedule a new inferior
				// add case 2: reschedule an absent inferior. Currently, we only reschedule each 2 minutes.
				// TODO: store absent inferiors in a separate map to trigger reschedule quickly.
				b.addInferiorCache = append(b.addInferiorCache, inf)
			}
		}
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

		captureIDs := make([]node.ID, 0, len(aliveCaptures))
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
func (b *BasicScheduler) newBurstAddInferiors(newInferiors []common.MaintainerID, captureIDs []node.ID,
) []*ScheduleTask {
	idx := 0
	addInferiorTasks := make([]*ScheduleTask, 0, len(newInferiors))
	for _, infID := range newInferiors {
		target := captureIDs[idx]
		addInferiorTasks = append(addInferiorTasks,
			&ScheduleTask{
				AddInferior: &AddInferior{
					ID:        infID,
					CaptureID: target,
				}})
		log.Info("burst add inferior",
			zap.String("id", b.id.String()),
			zap.String("inferior", infID.String()),
			zap.Any("serverID", target))

		idx++
		if idx >= len(captureIDs) {
			idx = 0
		}
	}
	return addInferiorTasks
}

// TODO: maybe remove task does not need captureID.
func (b *BasicScheduler) newBurstRemoveInferiors(
	rmInferiors []common.MaintainerID,
	stateMachines map[common.MaintainerID]*scheduler.StateMachine[common.MaintainerID],
) []*ScheduleTask {
	removeTasks := make([]*ScheduleTask, 0, len(rmInferiors))
	for _, id := range rmInferiors {
		state, _ := stateMachines[id]
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
				CaptureID: state.Primary,
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
