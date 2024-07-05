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
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
)

var testCount int64 = 0

type BasicCPUTask struct {
	taskStatus TaskStatus
}

func newBasicCPUTask() *BasicCPUTask {
	return &BasicCPUTask{
		taskStatus: Running,
	}
}

func (t *BasicCPUTask) GetStatus() TaskStatus {
	return t.taskStatus
}

func (t *BasicCPUTask) Execute(timeout time.Duration) TaskStatus {
	for i := 0; i < 100; i++ {
		atomic.AddInt64(&testCount, 1)
	}
	return Success
}

func (t *BasicCPUTask) Await() TaskStatus {
	log.Error("BasicCPUTask should not called Await()")
	return Failed
}

func (t *BasicCPUTask) Release() {
}

type BasicIOTask struct {
	taskStatus TaskStatus
}

func newBasicIOTask() *BasicIOTask {
	return &BasicIOTask{
		taskStatus: IO,
	}
}

func (t *BasicIOTask) GetStatus() TaskStatus {
	return t.taskStatus
}

func (t *BasicIOTask) Execute(timeout time.Duration) TaskStatus {
	time.Sleep(50 * time.Millisecond)
	atomic.AddInt64(&testCount, 1)
	return Success
}

func (t *BasicIOTask) Await() TaskStatus {
	log.Error("BasicIOTask should not called Await()")
	return Failed
}

func (t *BasicIOTask) Release() {
}

type BasicWaitTask struct {
	taskStatus TaskStatus
}

func newBasicWaitTask() *BasicWaitTask {
	return &BasicWaitTask{
		taskStatus: Waiting,
	}
}

func (t *BasicWaitTask) GetStatus() TaskStatus {
	return t.taskStatus
}

func (t *BasicWaitTask) Execute(timeout time.Duration) TaskStatus {
	time.Sleep(50 * time.Millisecond)
	atomic.AddInt64(&testCount, 1)
	return Success
}

func (t *BasicWaitTask) Await() TaskStatus {
	randomValue := rand.Intn(10)
	if randomValue < 5 {
		return Running
	}
	return Waiting
}

func (t *BasicWaitTask) Release() {
}

type PureCPUTask struct {
	taskStatus TaskStatus
	finalChan  *chan int
	ch         *chan int
	target     int64
	addCount   int
}

func newPureCPUTask(finalChan *chan int, ch *chan int, target int64, addCount int) *PureCPUTask {
	return &PureCPUTask{
		taskStatus: Running,
		finalChan:  finalChan,
		ch:         ch,
		target:     target,
		addCount:   addCount,
	}
}

func (t *PureCPUTask) GetStatus() TaskStatus {
	return t.taskStatus
}

func (t *PureCPUTask) Execute(timeout time.Duration) TaskStatus {
	select {
	case <-*t.ch:
		for i := 0; i < t.addCount; i++ {
			atomic.AddInt64(&testCount, 1)
		}
		if atomic.LoadInt64(&testCount) == int64(t.addCount)*t.target {
			*t.finalChan <- 1
		}
		return Success
	default:
		return Running
	}
}

func (t *PureCPUTask) Await() TaskStatus { return Failed }

func (t *PureCPUTask) Release() {}

type CPUWithWaitTask struct {
	taskStatus TaskStatus
	finalChan  *chan int
	ch         *chan int
	target     int64
	addCount   int
}

func newCPUWithWaitTask(finalChan *chan int, ch *chan int, target int64, addCount int) *CPUWithWaitTask {
	return &CPUWithWaitTask{
		taskStatus: Waiting,
		finalChan:  finalChan,
		ch:         ch,
		target:     target,
		addCount:   addCount,
	}
}

func (t *CPUWithWaitTask) GetStatus() TaskStatus {
	return t.taskStatus
}

func (t *CPUWithWaitTask) Execute(timeout time.Duration) TaskStatus {
	for i := 0; i < t.addCount; i++ {
		atomic.AddInt64(&testCount, 1)
	}
	if atomic.LoadInt64(&testCount) == int64(t.addCount)*t.target {
		*t.finalChan <- 1
	}

	return Success
}

func (t *CPUWithWaitTask) Await() TaskStatus {
	select {
	case <-*t.ch:
		return Running
	default:
		return Waiting
	}
}

func (t *CPUWithWaitTask) Release() {}

type CPUTimeTask struct {
	taskStatus TaskStatus
	finalChan  *chan int
	addCount   int
	taskCount  int
}

func newCPUTimeTask(finalChan *chan int, addCount int, taskCount int) *CPUTimeTask {
	return &CPUTimeTask{
		taskStatus: Running,
		finalChan:  finalChan,
		addCount:   addCount,
		taskCount:  taskCount,
	}
}

func (t *CPUTimeTask) GetStatus() TaskStatus {
	return t.taskStatus
}

func (t *CPUTimeTask) Execute(timeout time.Duration) TaskStatus {
	for i := 0; i < t.addCount; i++ {
		atomic.AddInt64(&testCount, 1)
	}
	if atomic.LoadInt64(&testCount) == int64(t.addCount*t.taskCount) {
		*t.finalChan <- 1
	}

	return Success
}

func (t *CPUTimeTask) Await() TaskStatus { return Failed }

func (t *CPUTimeTask) Release() {}
