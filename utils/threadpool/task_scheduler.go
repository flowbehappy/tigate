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
	"sync/atomic"
	"time"
)

/*
Task scheduler is the entry to use thread pool. It provides a unified interface to submit task to different thread pools.
TaskSchedulerConfig is used to configure the threads number and task queue of the thread pool and wait reactor.
*/
type TaskScheduler struct {
	cpuTaskThreadPool *ThreadPool
	ioTaskThreadPool  *ThreadPool

	nextTaskId atomic.Uint64
}

// Thread number:
//   - <0 disable the thread pool
//   - 0 (by defualt) means use 2 * logical cpu
//   - >0 to choose another value
func NewTaskScheduler(name string, cpuThreads int, ioThreads int) *TaskScheduler {
	ts := &TaskScheduler{}
	defaultThreadCount := runtime.NumCPU() * 2

	createTP := func(taskType TaskStatus, name string, threadCount int) *ThreadPool {
		c := 0
		if threadCount < 0 {
			return nil
		} else if threadCount == 0 {
			c = defaultThreadCount
		} else {
			c = threadCount
		}
		return NewThreadPool(taskType, name, ts, c)
	}

	ts.cpuTaskThreadPool = createTP(CPUTask, name+"-CPU", cpuThreads)
	ts.ioTaskThreadPool = createTP(IOTask, name+"-IO", ioThreads)

	return ts
}

func NewTaskSchedulerDefault(name string) *TaskScheduler {
	return NewTaskScheduler(name, 0, 0)
}

func (s *TaskScheduler) toBeScheduled(status TaskStatus) chan *scheduledTask {
	switch status {
	case CPUTask:
		return s.cpuTaskThreadPool.tobeScheduled()
	case IOTask:
		return s.ioTaskThreadPool.tobeScheduled()
	default:
		panicOnTaskStatus(status)
	}
	return nil
}

// Generate a new unique task id
func (s *TaskScheduler) NewTaskId() TaskId {
	return TaskId(s.nextTaskId.Add(1))
}

func (s *TaskScheduler) Submit(task Task, initalStatus TaskStatus, next time.Time) {
	switch initalStatus {
	case CPUTask:
		scheduleTask(task, initalStatus, next, s.cpuTaskThreadPool.schedule)
	case IOTask:
		scheduleTask(task, initalStatus, next, s.ioTaskThreadPool.schedule)
	default:
		panicOnTaskStatus(initalStatus)
	}
}

func (s *TaskScheduler) Update(task Task, status TaskStatus, next time.Time) {
	s.Submit(task, status, next)
}

// It is mainly used by test cases to stop the scheduler working.
func (s *TaskScheduler) blockForTest(until int, taskType TaskStatus) {
	switch taskType {
	case CPUTask:
		s.cpuTaskThreadPool.waitReactor.blockForTest(until)
	case IOTask:
		s.ioTaskThreadPool.waitReactor.blockForTest(until)
	default:
		panicOnTaskStatus(taskType)
	}
}

func (s *TaskScheduler) Finish() {
	if s.cpuTaskThreadPool != nil {
		s.cpuTaskThreadPool.finish()
		s.cpuTaskThreadPool.waitForStop()
	}
	if s.ioTaskThreadPool != nil {
		s.ioTaskThreadPool.finish()
		s.ioTaskThreadPool.waitForStop()
	}
}
