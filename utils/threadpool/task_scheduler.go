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

package threadpool

import (
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

/**
 * ┌────────────────────────────┐
 * │      task scheduler        │
 * │                            │
 * │    ┌───────────────────┐   │
 * │ ┌──┤io task thread pool◄─┐ │
 * │ │  └──────▲──┬─────────┘ │ │
 * │ │         │  │           │ │
 * │ │ ┌───────┴──▼─────────┐ │ │
 * │ │ │cpu task thread pool│ │ │
 * │ │ └───────▲──┬─────────┘ │ │
 * │ │         │  │           │ │
 * │ │    ┌────┴──▼────┐      │ │
 * │ └────►wait reactor├──────┘ │
 * │      └────────────┘        │
 * │                            │
 * └────────────────────────────┘
 *
 * A globally shared execution scheduler, used by pipeline executor.
 * - cpu task thread pool: for operator cpu intensive compute.
 * - io task thread pool: for operator io intensive block.
 * - wait reactor: for polling asynchronous io status, etc.
 */

// 这个有没有必要做成 interface，要想一下
type TaskScheduler struct {
	cpuTaskThreadPool *ThreadPool
	ioTaskThreadPool  *ThreadPool
	waitReactor       *WaitReactor
}

func NewTaskScheduler() *TaskScheduler {
	taskScheduler := &TaskScheduler{}
	taskScheduler.cpuTaskThreadPool = NewThreadPool(taskScheduler, NewFIFOTaskQueue(), 8)
	taskScheduler.ioTaskThreadPool = NewThreadPool(taskScheduler, NewFIFOTaskQueue(), 8)
	taskScheduler.waitReactor = NewWaitReactor(taskScheduler, 8)
	return taskScheduler
}

func (s *TaskScheduler) Submit(task Task) error {
	task_status := task.getStatus()
	switch task_status {
	case IO:
		s.submitTaskToIOThreadPool(task)
	case Running:
		s.submitTaskToCPUThreadPool(task)
	case Waiting:
		s.submitTaskToWaitReactorThreadPool(task)
	default:
		log.Error("TaskScheduler submit with Error Status: ", zap.Any("status", task_status))
	}
	return nil
}

func (s *TaskScheduler) submitTaskToCPUThreadPool(task *Task) {
	s.cpuTaskThreadPool.submit(task)
}

func (s *TaskScheduler) submitTaskToIOThreadPool(task *Task) {
	s.ioTaskThreadPool.submit(task)
}

func (s *TaskScheduler) submitTaskToWaitReactorThreadPool(task *Task) {
	s.waitReactor.submit(task)
}
