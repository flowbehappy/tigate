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
	"sync"
	"sync/atomic"
	"time"
)

type TaskId uint64
type TaskStatus int

const (
	// A task with Done status can not be changed to other status.
	Done    TaskStatus = 0
	CPUTask TaskStatus = 1
	IOTask  TaskStatus = 2
)

func panicOnTaskStatus(status TaskStatus) {
	panic(fmt.Sprintf("unexpected task status: %d", status))
}

// Task is the interface for the task to be executed by the thread pool.
//
// The return value of the methods are TaskStatus and time.Time.
//
// There are three possible values for TaskStatus:
//   - CPUTask: The task is a CPU-bound task, which will be executed by the CPU thread pool next time.
//   - IOTask: The task is an IO-bound task, which will be executed by the IO thread pool next time.
//   - Done: The task is done, and will not be executed again.
//
// The value of time.Time means the next disired execution time. It can be smaller than the current time.
//   - If a task is not going to be executed again, the return value of Task.Execute should be Done and time.Time{}.
//   - If a task wants to be executed as soon as possible, the return value of Task.Execute should be CPUTask or IOTask and time.Now().
//
// Note that:
//   - It is not guaranteed that a task will be executed at the exact disired time. The task scheduler will try to execute the task as soon as possible.
//   - It is not guaranteed that a task will be executed by the same goroutine.
//   - It is guaranteed that a task can only be executed by one goroutine at a time. I.e. a task will not be executed concurrently.
//   - The thread pool guarantees the call of Task.Execute is thread-safe. I.e. you don't need to add any locks in the Execute method to guarantee the thread safety,
//     including the visibility of the fields in the Task or any other memory. But you still need to guarantee the thread safety between
//     the Task.Execute and other methods of the Task.
//
// If you want to remove a task from the task scheduler, use one of the approaches below(using both is also fine):
//   - The future call of Task.Execute method returns Done and time.Time{}
//   - Call the TaskHandle.Cancel method. Note that the task will not be removed immediately, but will be removed before the next execution.
//     If the task is already running, the execution will not be stopped immediately.
//
// Refer to the task_example for a typical implementation of the Task interface.
type Task interface {
	// Execute the task.
	Execute() (TaskStatus, time.Time)
}

type TaskHandle struct {
	st *scheduledTask
	ts *TaskScheduler
}

func (h *TaskHandle) Cancel() { h.ts.cancel(h.st) }

type scheduledTask struct {
	task   Task
	status atomic.Int32 // TaskStatus
	time   time.Time    // Next disired execution time.

	index int // The index of the task in the heap.

	// To prevent the task from being executed concurrently.
	runMutex sync.Mutex
}

func newScheduledTask(task Task, status TaskStatus, time time.Time) *scheduledTask {
	st := &scheduledTask{
		task: task,
		time: time,

		index: -1, // <0 means the task is not in the heap.
	}
	st.status.Store(int32(status))
	return st
}

// Implemented by TaskScheduler
type toBeScheduled interface {
	toBeScheduled(status TaskStatus) chan *scheduledTask
}

// Implemented by TaskScheduler
type toBeExecuted interface {
	toBeExecuted(status TaskStatus) chan *scheduledTask
}
