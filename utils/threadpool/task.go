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
	"time"
)

type TaskId uint64
type TaskStatus int

const (
	CPUTask TaskStatus = iota
	IOTask
	Done
)

const (
	MaxDuration time.Duration = time.Duration(1<<63 - 1)
)

// Task is the interface for the task to be executed by the thread pool.
// A task should implement two methods: Probe and Execute.
//
// The return value of the methods are TaskStatus and time.Duration.
// There are three possible values for TaskStatus:
//   - CPUTask: The task is a CPU-bound task, which should be executed by the CPU thread pool.
//   - IOTask: The task is an IO-bound task, which should be executed by the IO thread pool.
//   - Done: The task is done, and should not be executed again.
//
// The value of time.Duration means the expected duration between now and the next expected execution time.
type Task interface {
	// Probe the task.
	// This method could be called to check the status of the task by any time.
	Probe() (TaskStatus, time.Duration)
	// Execute the task.
	Execute() (TaskStatus, time.Duration)
	// // release the resources used by the task
	// Release()
	// // Get status of the task
	// GetStatus() TaskStatus
	// // Set status of the task
	// SetStatus(status TaskStatus)
	// // Cancel the task
	// // When the user calls cancel, the current task may still call execute once
	// Cancel()
}
