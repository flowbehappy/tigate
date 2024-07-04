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
	"testing"
	"time"

	"github.com/zeebo/assert"
)

// BasicThreadPool test the basic functionality of threadpool
// Including the functionality of CPU Threadpoool, IO Threadpool, and WaitReactor
func TestBasicThreadPool(t *testing.T) {
	// Test CPU Task
	{
		testCount = 0
		taskScheduler := NewTaskScheduler(NewFIFOTaskQueue(), &DefaultTaskSchedulerConfig, "BasicCPUTask")
		for i := 0; i < 10000; i++ {
			taskScheduler.Submit(newBasicCPUTask())
		}
		time.Sleep(5 * time.Second)
		taskScheduler.Finish()
		assert.Equal(t, testCount, int64(10000*100))
	}

	// Test IO Task
	{
		testCount = 0
		taskScheduler := NewTaskScheduler(NewFIFOTaskQueue(), &DefaultTaskSchedulerConfig, "BasicIOTask")
		for i := 0; i < 1000; i++ {
			taskScheduler.Submit(newBasicIOTask())
		}
		time.Sleep(5 * time.Second)
		taskScheduler.Finish()
		assert.Equal(t, testCount, int64(1000))
	}

	// Test Wait+CPU Task
	{
		testCount = 0
		taskScheduler := NewTaskScheduler(NewFIFOTaskQueue(), &DefaultTaskSchedulerConfig, "BasicWaitAndCPUTask")
		for i := 0; i < 1000; i++ {
			taskScheduler.Submit(newBasicWaitTask())
		}
		time.Sleep(10 * time.Second)
		taskScheduler.Finish()
		assert.Equal(t, testCount, int64(1000))
	}
}
