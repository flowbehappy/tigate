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

type TaskQueueType int

const (
	FIFOTaskQueueType TaskQueueType = iota
)

/*
TaskQueue is used to manage task lists in thread pool.
It provides following methods:
1. take a task from the queue
2. submit a task to the queue
3. release resources when the queue is finished.
*/
type TaskQueue interface {
	Take() (bool, *Task)
	Submit(task *Task)
	Finish()
}

func NewTaskQueue(t TaskQueueType) TaskQueue {
	switch t {
	case FIFOTaskQueueType:
		return NewFIFOTaskQueue()
	default:
		return nil
	}
}
