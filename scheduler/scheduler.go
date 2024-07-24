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

// Scheduler schedules check all untracked inferiors and generate ScheduleTask
type Scheduler interface {
	Name() string
	Schedule(
		allInferiors utils.Map[InferiorID, Inferior],
		aliveCaptures map[model.CaptureID]*CaptureStatus,
		stateMachines utils.Map[InferiorID, *StateMachine],
	) []*ScheduleTask
}

// Schedule generates schedule tasks based on the inputs.
func (s *Supervisor) Schedule(
	allInferiors utils.Map[InferiorID, Inferior],
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	stateMachines utils.Map[InferiorID, *StateMachine],
) []*ScheduleTask {
	if allInferiors.Len() == stateMachines.Len() && time.Since(s.lastScheduleTime) < 60*time.Second {
		return nil
	}
	if s.RunningTasks.Len() >= s.maxTaskConcurrency {
		log.Warn("Skip scheduling since there are too many running task",
			zap.String("id", s.ID.String()),
			zap.Int("totalInferiors", allInferiors.Len()),
			zap.Int("totalStateMachines", stateMachines.Len()),
			zap.Int("maxTaskConcurrency", s.maxTaskConcurrency),
			zap.Int("runningTasks", s.RunningTasks.Len()),
		)
		return nil
	}
	s.lastScheduleTime = time.Now()
	for _, sched := range s.schedulers {
		tasks := sched.Schedule(allInferiors, aliveCaptures, stateMachines)
		if len(tasks) != 0 {
			return tasks
		}
	}
	return nil
}

func (s *Supervisor) Name() string {
	return "combine-scheduler"
}
