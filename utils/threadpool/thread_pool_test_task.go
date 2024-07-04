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
