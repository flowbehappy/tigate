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
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/zeebo/assert"
	"go.uber.org/zap"
)

// 这个文件用来测试 threadpool / task_scheduler / task_queue 的功能和性能
var count int64

type BasicTask struct {
	taskStatus TaskStatus
}

func newBasicTask() *BasicTask {
	return &BasicTask{
		taskStatus: Running,
	}
}

func (t *BasicTask) GetStatus() TaskStatus {
	return t.taskStatus
}

func (t *BasicTask) Execute(timeout time.Duration) TaskStatus {
	//now := time.Now()
	for i := 0; i < 50; i++ {
		atomic.AddInt64(&count, 1)
	}
	// duration := time.Since(now)
	// log.Info("BasicTask executed", zap.Duration("duration", duration))
	return Success
}

func (t *BasicTask) Await() TaskStatus {
	log.Error("BasicTask should not called Await()")
	return Failed
}

func (t *BasicTask) Release() {
}

func TestBasicThreadPool(t *testing.T) {
	// 验证线程池的功能，创建一个 threadpool，然后插入一系列 task，检查是否正常执行
	count = 0
	taskScheduler := NewTaskScheduler(NewFIFOTaskQueue())
	for i := 0; i < 1000000000; i++ {
		taskScheduler.Submit(newBasicTask())
	}
	time.Sleep(20 * time.Second)
	taskScheduler.Finish()
	log.Info("final", zap.Any("count", count))
	assert.Equal(t, count, 10000)

}
