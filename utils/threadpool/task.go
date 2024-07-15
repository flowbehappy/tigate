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
	Done    TaskStatus = 0
	CPUTask TaskStatus = 1
	IOTask  TaskStatus = 2
)

func panicOnTaskStatus(status TaskStatus) {
	panic(fmt.Sprintf("unexpected task status: %d", status))
}

// Task is the interface for the task to be executed by the thread pool.
//
// A task should have a unique TaskId. It is recommended to use the NewTaskId from a TaskScheduler
// or NewGlobalTaskId to generate the TaskId.
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
// We provide two ways to tell the task scheduler about the next disired execution time: the result of Task.Execute method and
// the TaskScheduler.Update method. Some notes:
//   - The TaskScheduler.Update is not guaranteed to be executed immediately. It is passed through a channel and handled by a goroutine.
//   - If the task is being executed when the TaskScheduler.Update command arrives, the new disired execution time will be overwritten by the result of Task.Execute.
//   - If the Done status is returned by Task.Execute, the task is guaranteed to be removed from the task scheduler and won't be executed again.
//
// Note that:
//   - It is not guaranteed that a task will be executed at the exact disired time. The task scheduler will try to execute the task as soon as possible.
//   - It is not guaranteed that a task will be executed by the same goroutine.
//   - It is guaranteed that a task can only be executed by one goroutine at a time. I.e. a task will not be executed concurrently.
//   - The thread pool guarantees the call of Task.Execute is thread-safe. I.e. you don't need to add any locks in the Execute method to guarantee the thread safety,
//     including the visibility of the fields in the Task or any other memory. But you still need to guarantee the thread safety between
//     different tasks' Execute methods, if they share some resources.
//
// If you want to remove a task from the task scheduler, use one of the approaches below(using both is also fine):
//   - The future call of Task.Execute method returns Done and time.Time{}
//   - Call the TaskScheduler.Cancel method. Note that the task will not be removed immediately, but will be removed before the next execution.
//     If the task is already running, the execution will not be stopped immediately.
//
// The typical implmentation of the Task interface is as follows:
//   - Use a taskId field to store the unique TaskId. Please don't modify it after the task is created.
//   - Use a nextExecTime field to store the next disired execution time.
//   - Before returning the result of Execute, update the nextExecTime field with the return value.
//   - If you want to change the next execute time of a task, first update nextExecTime field, and then call the Update in the TaskScheduler.
//     It is particularly useful when the task is blocked by some resources, and you want to execute it as soon as the resources are available.
//     For instance, you want to execute a dispatcher task immediately after some events are ready.
//   - To avoid calling Update too frequently, you should check whether the new disiered execution time is earlier than nextExecTime or not.
//     E.g. if newExecTime >= nextExecTime, it means the task is already executing or at least at the front of the waiting list,
//     and there is no need to call Update again.
type Task interface {
	// The unique Id of the task. The thread pool uses this id as the map key.
	TaskId() TaskId
	// Execute the task.
	// The status indicates what kind of thread pool is executing the task.
	Execute() (TaskStatus, time.Time)
}

var globalTaskId atomic.Uint64

func NewGlobalTaskId() TaskId {
	return TaskId(globalTaskId.Add(1))
}

type scheduledTask struct {
	task   Task
	status TaskStatus
	time   time.Time // Next disired execution time.

	index int // The index of the task in the heap.

	// To prevent the task from being executed concurrently.
	runMutex sync.Mutex
}

// Implemented by TaskScheduler
type toBeScheduled interface {
	toBeScheduled(status TaskStatus) chan *scheduledTask
}

// Implemented by TaskScheduler
type toBeExecuted interface {
	toBeExecuted(status TaskStatus) chan *scheduledTask
}

type taskMap struct {
	idToTask map[TaskId]*scheduledTask
	mutex    sync.Mutex
}

func newTaskMap() taskMap { return taskMap{idToTask: make(map[TaskId]*scheduledTask)} }
