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
	"time"
)

type TaskId uint64

// Task is the interface for the task to be executed by the thread pool.
//
// The return value of the methods is time.Time, which means the next disired execution time. It can be smaller than the current time.
//   - If a task is not going to be executed again, the return value of Task.Execute should be time.Time{}.
//   - If a task wants to be executed as soon as possible, the return value of Task.Execute should be time.Now().
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
// Refer to task_example.go for a typical implementation of the Task interface.
type Task interface {
	// Execute the task.
	Execute() time.Time
}

type FuncTask func() time.Time

type TaskHandle struct {
	st *scheduledTask
	ts *threadPoolImpl
}

func (h *TaskHandle) Cancel() { h.ts.cancel(h.st) }

type ThreadPool interface {
	Submit(task Task, next time.Time) *TaskHandle
	SubmitFunc(task FuncTask, next time.Time) *TaskHandle
	Stop()
}

func NewThreadPoolDefault() ThreadPool { return NewThreadPool(0) }
func NewThreadPool(threadCount int) ThreadPool {
	if threadCount == 0 {
		threadCount = runtime.NumCPU() * 2
	}
	return newThreadPoolImpl(threadCount)
}

type funcTaskImpl struct {
	f FuncTask
}

func (t *funcTaskImpl) Execute() time.Time { return t.f() }

type scheduledTask struct {
	task Task
	time time.Time // Next disired execution time. time.Time{} means the task is done.

	heapIndex int // The index of the task in the heap.
}

func (m *scheduledTask) SetHeapIndex(index int)             { m.heapIndex = index }
func (m *scheduledTask) GetHeapIndex() int                  { return m.heapIndex }
func (m *scheduledTask) LessThan(other *scheduledTask) bool { return m.time.Before(other.time) }
