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
	"fmt"
	"runtime"
	"sync/atomic"
	"time"
)

// TaskScheduler is the entry to use thread pool. It provides a unified interface to submit task to different thread pools.
// For detailed usage, please refer to task.go
type TaskScheduler struct {
	cpuTaskThreadPool *threadPool
	ioTaskThreadPool  *threadPool

	// The reactors will make the execution plan of the tasks.
	// And send the ready tasks to corresponding thread pools.
	// Note the for the Done tasks, they will not be sent to any thread pool, but remove from the taskMap.
	cpuReactor  *waitReactor
	ioReactor   *waitReactor
	doneReactor *waitReactor

	taskMap    taskMap
	nextTaskId atomic.Uint64
}

// Thread number:
//   - <0 disable the thread pool
//   - 0 (by defualt) means use 2 * logical cpu
//   - >0 to choose another value
func NewTaskScheduler(name string, cpuThreads int, ioThreads int) *TaskScheduler {
	ts := &TaskScheduler{
		taskMap: newTaskMap(),
	}
	defaultThreadCount := runtime.NumCPU() * 2

	createTP := func(taskType TaskStatus, name string, threadCount int) *threadPool {
		c := 0
		if threadCount < 0 {
			return nil
		} else if threadCount == 0 {
			c = defaultThreadCount
		} else {
			c = threadCount
		}
		return newthreadPool(taskType, name, ts, c)
	}

	ts.cpuTaskThreadPool = createTP(CPUTask, name+"-CPU", cpuThreads)
	ts.ioTaskThreadPool = createTP(IOTask, name+"-IO", ioThreads)

	ts.cpuReactor = newWaitReactor(ts, &ts.taskMap)
	ts.ioReactor = newWaitReactor(ts, &ts.taskMap)
	ts.doneReactor = newWaitReactor(ts, &ts.taskMap)

	return ts
}

func NewTaskSchedulerDefault(name string) *TaskScheduler {
	return NewTaskScheduler(name, 0, 0)
}

func (s *TaskScheduler) toBeScheduled(status TaskStatus) chan *scheduledTask {
	switch status {
	case CPUTask:
		if s.cpuTaskThreadPool == nil {
			panic(fmt.Sprintf("CPU thread pool does not exist"))
		}
		return s.cpuReactor.tobeScheduled()
	case IOTask:
		if s.ioTaskThreadPool == nil {
			panic(fmt.Sprintf("IO thread pool does not exist"))
		}
		return s.ioReactor.tobeScheduled()
	case Done:
		return s.doneReactor.tobeScheduled()
	default:
		panicOnTaskStatus(status)
	}
	return nil
}

func (s *TaskScheduler) toBeExecuted(status TaskStatus) chan *scheduledTask {
	switch status {
	case CPUTask:
		return s.cpuTaskThreadPool.toBeExecuted()
	case IOTask:
		return s.ioTaskThreadPool.toBeExecuted()
	default:
		panicOnTaskStatus(status)
	}
	return nil
}

// Generate a new unique task id
func (s *TaskScheduler) NewTaskId() TaskId { return TaskId(s.nextTaskId.Add(1)) }

func (s *TaskScheduler) Submit(task Task, status TaskStatus, next time.Time) {
	st := &scheduledTask{task: task, status: status, time: next}
	// The task will be handled by one of the waitReactor.
	s.toBeScheduled(status) <- st
}

func (s *TaskScheduler) Update(task Task, status TaskStatus, next time.Time) {
	s.Submit(task, status, next)
}

func (s *TaskScheduler) Cancel(task Task) {
	s.Submit(task, Done, time.Time{})
}

// It is mainly used by test cases to stop the scheduler working.
func (s *TaskScheduler) blockForTest(until int, taskType TaskStatus) {
	switch taskType {
	case CPUTask:
		s.cpuReactor.blockForTest(until)
	case IOTask:
		s.ioReactor.blockForTest(until)
	default:
		panicOnTaskStatus(taskType)
	}
}

func (s *TaskScheduler) Stop() {
	s.cpuReactor.stop()
	s.ioReactor.stop()
	s.doneReactor.stop()

	s.cpuReactor.waitForStop()
	s.ioReactor.waitForStop()
	s.doneReactor.waitForStop()

	if s.cpuTaskThreadPool != nil {
		s.cpuTaskThreadPool.stop()
		s.cpuTaskThreadPool.waitForStop()
	}
	if s.ioTaskThreadPool != nil {
		s.ioTaskThreadPool.stop()
		s.ioTaskThreadPool.waitForStop()
	}
}
