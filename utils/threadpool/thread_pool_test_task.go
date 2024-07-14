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
	"sync/atomic"
	"time"
)

var testCount int64 = 0

type BasicCPUTask struct {
	id TaskId
}

func newBasicCPUTask() *BasicCPUTask {
	return &BasicCPUTask{
		id: NewGlobalTaskId(),
	}
}

func (t *BasicCPUTask) TaskId() TaskId { return t.id }

func (t *BasicCPUTask) Execute(TaskStatus) (TaskStatus, time.Time) {
	for i := 0; i < 100; i++ {
		atomic.AddInt64(&testCount, 1)
	}
	return Done, time.Time{}
}

type BasicIOTask struct {
	id TaskId
}

func newBasicIOTask() *BasicIOTask {
	return &BasicIOTask{
		id: NewGlobalTaskId(),
	}
}

func (t *BasicIOTask) TaskId() TaskId { return t.id }

func (t *BasicIOTask) Execute(TaskStatus) (TaskStatus, time.Time) {
	time.Sleep(50 * time.Millisecond)
	atomic.AddInt64(&testCount, 1)
	return Done, time.Time{}
}

type BasicWaitTask struct {
	id TaskId
}

func newBasicWaitTask() *BasicWaitTask {
	return &BasicWaitTask{
		id: NewGlobalTaskId(),
	}
}

func (t *BasicWaitTask) TaskId() TaskId { return t.id }

func (t *BasicWaitTask) Execute(TaskStatus) (TaskStatus, time.Time) {
	time.Sleep(50 * time.Millisecond)
	atomic.AddInt64(&testCount, 1)
	return Done, time.Time{}
}

type PureCPUTask struct {
	id TaskId

	finalChan *chan int
	ch        *chan int
	target    int64
	addCount  int
}

func newPureCPUTask(finalChan *chan int, ch *chan int, target int64, addCount int) *PureCPUTask {
	return &PureCPUTask{
		id:        NewGlobalTaskId(),
		finalChan: finalChan,
		ch:        ch,
		target:    target,
		addCount:  addCount,
	}
}
func (t *PureCPUTask) TaskId() TaskId { return t.id }

func (t *PureCPUTask) Execute(TaskStatus) (TaskStatus, time.Time) {
	select {
	case <-*t.ch:
		for i := 0; i < t.addCount; i++ {
			atomic.AddInt64(&testCount, 1)
		}
		if atomic.LoadInt64(&testCount) == int64(t.addCount)*t.target {
			*t.finalChan <- 1
		}
		return Done, time.Time{}
	default:
		return CPUTask, time.Now()
	}
}

type CPUWithWaitTask struct {
	id           TaskId
	nextExecTime time.Time

	finalChan *chan int
	taskCount int
	addCount  int
}

func (t *CPUWithWaitTask) TaskId() TaskId { return t.id }

func newCPUWithWaitTask(finalChan *chan int, taskCount int, addCount int, wait time.Duration) *CPUWithWaitTask {
	return &CPUWithWaitTask{
		id:           NewGlobalTaskId(),
		nextExecTime: time.Now().Add(wait),

		finalChan: finalChan,
		taskCount: taskCount,
		addCount:  addCount,
	}
}

func (t *CPUWithWaitTask) Execute(TaskStatus) (TaskStatus, time.Time) {
	if time.Now().Before(t.nextExecTime) {
		return CPUTask, t.nextExecTime
	}

	for i := 0; i < t.addCount; i++ {
		atomic.AddInt64(&testCount, 1)
	}

	if atomic.LoadInt64(&testCount) == int64(t.addCount*t.taskCount) {
		*t.finalChan <- 1
	}

	return Done, time.Time{}
}

type CPUTimeTask struct {
	id TaskId

	finalChan *chan int
	addCount  int
	taskCount int
}

func newCPUTimeTask(finalChan *chan int, addCount int, taskCount int) *CPUTimeTask {
	return &CPUTimeTask{
		id:        NewGlobalTaskId(),
		finalChan: finalChan,
		addCount:  addCount,
		taskCount: taskCount,
	}
}

func (t *CPUTimeTask) TaskId() TaskId { return t.id }

func (t *CPUTimeTask) Execute(TaskStatus) (TaskStatus, time.Time) {
	for i := 0; i < t.addCount; i++ {
		atomic.AddInt64(&testCount, 1)
	}
	if atomic.LoadInt64(&testCount) == int64(t.addCount*t.taskCount) {
		*t.finalChan <- 1
	}

	return Done, time.Time{}
}
