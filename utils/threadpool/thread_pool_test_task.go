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
	"sync"
	"sync/atomic"
	"time"
)

var testCount int64 = 0

type BasicCPUTask struct {
	wg *sync.WaitGroup
}

func newBasicCPUTask(wg *sync.WaitGroup) *BasicCPUTask {
	return &BasicCPUTask{wg}
}

func (t *BasicCPUTask) Execute() time.Time {
	for i := 0; i < 100; i++ {
		atomic.AddInt64(&testCount, 1)
	}
	t.wg.Done()
	return time.Time{}
}

type BasicWaitTask struct {
	wg *sync.WaitGroup
}

func newBasicWaitTask(wg *sync.WaitGroup) *BasicWaitTask {
	return &BasicWaitTask{wg}
}

func (t *BasicWaitTask) Execute() time.Time {
	time.Sleep(50 * time.Millisecond)
	atomic.AddInt64(&testCount, 1)
	t.wg.Done()

	return time.Time{}
}

type PureCPUTask struct {
	finalChan *chan int
	ch        *chan int
	target    int64
	addCount  int
}

func newPureCPUTask(finalChan *chan int, ch *chan int, target int64, addCount int) *PureCPUTask {
	return &PureCPUTask{
		finalChan: finalChan,
		ch:        ch,
		target:    target,
		addCount:  addCount,
	}
}

func (t *PureCPUTask) Execute() time.Time {
	select {
	case <-*t.ch:
		for i := 0; i < t.addCount; i++ {
			atomic.AddInt64(&testCount, 1)
		}
		if atomic.LoadInt64(&testCount) == int64(t.addCount)*t.target {
			*t.finalChan <- 1
		}
		return time.Time{}
	default:
		return time.Now()
	}
}

type CPUWithWaitTask struct {
	nextExecTime time.Time

	finalChan *chan int
	taskCount int
	addCount  int
}

func newCPUWithWaitTask(finalChan *chan int, taskCount int, addCount int, wait time.Duration) *CPUWithWaitTask {
	return &CPUWithWaitTask{
		nextExecTime: time.Now().Add(wait),

		finalChan: finalChan,
		taskCount: taskCount,
		addCount:  addCount,
	}
}

func (t *CPUWithWaitTask) Execute() time.Time {
	if time.Now().Before(t.nextExecTime) {
		return t.nextExecTime
	}

	for i := 0; i < t.addCount; i++ {
		atomic.AddInt64(&testCount, 1)
	}

	if atomic.LoadInt64(&testCount) == int64(t.addCount*t.taskCount) {
		*t.finalChan <- 1
	}

	return time.Time{}
}

type CPUTimeTask struct {
	finalChan *chan int
	addCount  int
	taskCount int
}

func newCPUTimeTask(finalChan *chan int, addCount int, taskCount int) *CPUTimeTask {
	return &CPUTimeTask{
		finalChan: finalChan,
		addCount:  addCount,
		taskCount: taskCount,
	}
}

func (t *CPUTimeTask) Execute() time.Time {
	for i := 0; i < t.addCount; i++ {
		atomic.AddInt64(&testCount, 1)
	}
	if atomic.LoadInt64(&testCount) == int64(t.addCount*t.taskCount) {
		*t.finalChan <- 1
	}

	return time.Time{}
}
