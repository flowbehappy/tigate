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
	"github.com/flowbehappy/tigate/utils"
	"github.com/pingcap/tiflow/cdc/model"
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
func (c *Supervisor) Schedule(
	allInferiors utils.Map[InferiorID, Inferior],
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	stateMachines utils.Map[InferiorID, *StateMachine],
) []*ScheduleTask {
	for _, sched := range c.schedulers {
		tasks := sched.Schedule(allInferiors, aliveCaptures, stateMachines)
		if len(tasks) != 0 {
			return tasks
		}
	}
	return nil
}

func (c *Supervisor) Name() string {
	return "combine-scheduler"
}
