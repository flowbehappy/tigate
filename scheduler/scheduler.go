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

	"github.com/flowbehappy/tigate/rpc"
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
		batchSize int,
	) []*ScheduleTask
}

// Schedule generates schedule tasks based on the inputs.
func (s *Supervisor) schedule(
	allInferiors utils.Map[InferiorID, Inferior],
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	stateMachines utils.Map[InferiorID, *StateMachine],
	batchSize int,
) []*ScheduleTask {
	for _, sched := range s.schedulers {
		tasks := sched.Schedule(allInferiors, aliveCaptures, stateMachines, batchSize)
		if len(tasks) != 0 {
			return tasks
		}
	}
	return nil
}

// Schedule generates schedule tasks based on the inputs.
func (s *Supervisor) Schedule(allInferiors utils.Map[InferiorID, Inferior]) ([]rpc.Message, error) {
	msgs := s.checkRunningTasks()

	if !s.CheckAllCaptureInitialized() {
		log.Info("skip scheduling since not all captures are initialized",
			zap.String("id", s.ID.String()),
			zap.Int("totalInferiors", allInferiors.Len()),
			zap.Int("totalStateMachines", s.StateMachines.Len()),
			zap.Int("maxTaskConcurrency", s.maxTaskConcurrency),
			zap.Int("runningTasks", s.RunningTasks.Len()),
		)
		return msgs, nil
	}
	batchSize := s.maxTaskConcurrency - s.RunningTasks.Len()
	if batchSize <= 0 {
		log.Warn("Skip scheduling since there are too many running task",
			zap.String("id", s.ID.String()),
			zap.Int("totalInferiors", allInferiors.Len()),
			zap.Int("totalStateMachines", s.StateMachines.Len()),
			zap.Int("maxTaskConcurrency", s.maxTaskConcurrency),
			zap.Int("runningTasks", s.RunningTasks.Len()),
		)
		return msgs, nil
	}

	tasks := s.schedule(allInferiors, s.GetAllCaptures(), s.GetInferiors(), batchSize)
	msgs1, err := s.handleScheduleTasks(tasks)
	msgs = append(msgs, msgs1...)
	return msgs, err
}

func (s *Supervisor) MarkNeedAddInferior() {
	basciScheduler := s.schedulers[0].(*BasicScheduler)
	basciScheduler.markNeedAddInferior()
}

func (s *Supervisor) MarkNeedRemoveInferior() {
	basciScheduler := s.schedulers[0].(*BasicScheduler)
	basciScheduler.markNeedRemoveInferior()
}

func (s *Supervisor) Name() string {
	return "combine-scheduler"
}

func (s *Supervisor) checkRunningTasks() (msgs []rpc.Message) {
	needResend := false
	if time.Since(s.lastResendTime) > time.Second*2 {
		needResend = true
		s.lastResendTime = time.Now()
	}

	// Check if a running task is finished.
	var toBeDeleted []InferiorID
	s.RunningTasks.Ascend(func(id InferiorID, task *ScheduleTask) bool {
		stateMachine, ok := s.StateMachines.Get(id)
		if !ok || stateMachine.HasRemoved() || stateMachine.State == SchedulerStatusWorking {
			// 1. No inferior found, remove the task
			// 2. The inferior has been removed, remove the task
			// 3. The task is still working, remove the task
			toBeDeleted = append(toBeDeleted, id)
			return true
		}

		if needResend {
			msg := stateMachine.handleResend()
			log.Debug("resend message", zap.Any("msg", msg))
			msgs = append(msgs, msg...)
		}
		return true
	})

	for _, span := range toBeDeleted {
		s.RunningTasks.Delete(span)
		log.Debug("schedule finished, remove running task",
			zap.String("stid", s.ID.String()),
			zap.String("id", span.String()))
	}
	return
}
