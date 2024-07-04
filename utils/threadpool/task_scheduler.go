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
	"runtime"

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
type TaskScheduler struct {
	cpuTaskThreadPool *ThreadPool
	ioTaskThreadPool  *ThreadPool
	waitReactor       *WaitReactor
}

type TaskSchedulerConfig struct {
	// <0 means use 2 * logical cpu
	// >=0 means use this thread pool with specified threads
	cpuThreads         int
	ioThreads          int
	waitReactorThreads int
}

var DefaultTaskSchedulerConfig = TaskSchedulerConfig{
	cpuThreads:         -1,
	ioThreads:          -1,
	waitReactorThreads: 1,
}

// 这个应该传 taskQueue 初始化的函数进来
func NewTaskScheduler(taskQueue TaskQueue, threadpoolConfig *TaskSchedulerConfig, name string) *TaskScheduler {
	// 加参数
	taskScheduler := &TaskScheduler{}
	logicalCpu := runtime.NumCPU()

	if threadpoolConfig.cpuThreads >= 0 {
		taskScheduler.cpuTaskThreadPool = NewThreadPool(taskScheduler, taskQueue, threadpoolConfig.cpuThreads, name+"-CPU")
	} else {
		log.Info("TaskScheduler use default cpu threads", zap.Int("threads", 2*logicalCpu))
		taskScheduler.cpuTaskThreadPool = NewThreadPool(taskScheduler, taskQueue, 2*logicalCpu, name+"-CPU")
	}

	if threadpoolConfig.ioThreads >= 0 {
		taskScheduler.ioTaskThreadPool = NewThreadPool(taskScheduler, taskQueue, threadpoolConfig.ioThreads, name+"-IO")
	} else {
		taskScheduler.ioTaskThreadPool = NewThreadPool(taskScheduler, taskQueue, 2*logicalCpu, name+"-IO")
	}

	if threadpoolConfig.waitReactorThreads >= 0 {
		taskScheduler.waitReactor = NewWaitReactor(taskScheduler, threadpoolConfig.waitReactorThreads, name)
	} else {
		taskScheduler.waitReactor = NewWaitReactor(taskScheduler, 2*logicalCpu, name)
	}

	return taskScheduler
}

func (s *TaskScheduler) Submit(task Task) error {
	task_status := task.GetStatus()
	switch task_status {
	case IO:
		s.submitTaskToIOThreadPool(&task)
	case Running:
		s.submitTaskToCPUThreadPool(&task)
	case Waiting:
		s.submitTaskToWaitReactorThreadPool(&task)
	default:
		log.Error("TaskScheduler submit with Error Status: ", zap.Any("status", task_status))
	}
	return nil
}

func (s *TaskScheduler) submitTaskToCPUThreadPool(task *Task) {
	// 我需要做这个 task nil 的检查么？还是应该让外部自己来保证呢？这个加了肯定会有一点点性能的影响，但是不大
	if task == nil {
		log.Info("TaskScheduler submitTaskToCPUThreadPool with nil task")
	}
	s.cpuTaskThreadPool.submit(task)
}

func (s *TaskScheduler) submitTaskToIOThreadPool(task *Task) {
	s.ioTaskThreadPool.submit(task)
}

func (s *TaskScheduler) submitTaskToWaitReactorThreadPool(task *Task) {
	s.waitReactor.submit(task)
}

func (s *TaskScheduler) Finish() {
	s.cpuTaskThreadPool.finish()
	s.ioTaskThreadPool.finish()
	s.waitReactor.finish()

	s.cpuTaskThreadPool.waitForStop()
	s.ioTaskThreadPool.waitForStop()
	s.waitReactor.waitForStop()
}
