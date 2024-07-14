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
//   - CPUTask: The task is a CPU-bound task, which should be executed by the CPU thread pool.
//   - IOTask: The task is an IO-bound task, which should be executed by the IO thread pool.
//   - Done: The task is done, and should not be executed again.
//
// The value of time.Time means the next disired execution time. It can be smaller than the current time.
//   - If a task is not going to be executed again, the return value of Execute should be Done and time.Time{}.
//   - If a task wants to be executed as soon as possible, the return value of Execute should be CPUTask or IOTask and time.Now().
//
// We provide two ways to tell the task scheduler about the next disired execution time: the result of Execute method and
// the Update method in the TaskScheduler.
//
// Note that if you call Update method after a task is Submit, the task could be executed by multiple threads concurrently,
// so the Execute method should be thread-safe. If you don't want a task to be executed concurrently,
// you should add a mark to avoid the situation.
//
// If you want to remove a task from the task scheduler, make sure
//   - The future call of Execute moethod returns Done and time.Time{}
//   - Call the Update method in the TaskScheduler with Done and time.Time{}.
//
// The typical implmentation of the Task interface is as follows:
//   - Use a taskId field to store the unique TaskId. Please don't modify it after the task is created.
//   - Use a isRunning field to avoid the task being executed concurrently in the Execute method.
//   - Use a nextExecTime field to store the next disired execution time.
//   - Before returning the result of Execute, update the nextExecTime field with the return value.
//   - If you want to execute the task earlier or later, first update nextExecTime field, and then call the Update in the TaskScheduler.
//     It is particularly useful when the task is blocked by some resources, and you want to execute it as soon as the resources are available.
//     For instance, you want to execute a dispatcher task immediately after some events are ready.
//   - To avoid calling Update too frequently, you should check whether the new disiered execution time is earlier than nextExecTime or not.
//     E.g. if newExecTime >= nextExecTime, it means the task is already executing or at least at the front of the waiting list,
//     and there is no need to call Update again.
type Task interface {
	// The Unique Id of the task. The thread pool uses this id as the map key.
	TaskId() TaskId
	// Execute the task.
	// The status indicates what kind of thread pool is executing the task.
	Execute(status TaskStatus) (TaskStatus, time.Time)
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
}

type toBeScheduled interface {
	toBeScheduled(status TaskStatus) chan *scheduledTask
}

type toBeExecuted interface {
	toBeExecuted() chan Task
}

func scheduleTask(task Task, status TaskStatus, next time.Time, toBeScheduled toBeScheduled) {
	switch status {
	case CPUTask, IOTask:
		tw := scheduledTask{task: task, status: status, time: next}
		toBeScheduled.toBeScheduled(status) <- &tw
	case Done:
		// Don't put the task into any thread pool.
		return
	default:
		panicOnTaskStatus(status)
	}
}
