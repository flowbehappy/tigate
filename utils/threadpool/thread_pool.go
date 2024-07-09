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
	"time"

	"github.com/pingcap/log"
)

/*
ThreadPool is used to continuously get tasks and execute them in a fixed number of threads.
We can use timeout to control the maximum execution time of each task.
*/
type ThreadPool struct {
	scheduler   *TaskScheduler
	taskQueue   TaskQueue
	threadCount int
	wg          sync.WaitGroup
	timeout     time.Duration
	name        string
}

func NewThreadPool(scheduler *TaskScheduler, taskQueue TaskQueue, threadCount int, timeout int64, name string) *ThreadPool {
	threadPool := ThreadPool{
		scheduler:   scheduler,
		taskQueue:   taskQueue,
		threadCount: threadCount,
		timeout:     time.Duration(timeout) * time.Millisecond,
		name:        name,
	}

	for i := 0; i < threadCount; i++ {
		threadPool.wg.Add(1)
		go threadPool.loop()

	}
	return &threadPool
}

func (p *ThreadPool) loop() {
	for {
		ok, task := p.taskQueue.Take()
		if ok {
			p.handleTask(task)
		} else {
			p.wg.Done()
			return
		}
	}
}

// Execute the task, and submit it to different place after execution.
func (p *ThreadPool) handleTask(task *Task) {
	// TODO(hongyunyan): check whether we need to check task == nil
	if task == nil {
		log.Error("task is nil")
		return
	}

	status := (*task).Execute(p.timeout)
	(*task).SetStatus(status)

	switch status {
	case Running:
		p.scheduler.submitTaskToCPUThreadPool(task)
	case Waiting:
		p.scheduler.submitTaskToWaitReactorThreadPool(task)
	case IO:
		p.scheduler.submitTaskToIOThreadPool(task)
	case Success:
		// do nothing?
	case Failed:
		// TODO: error information? or get error from Execute
		log.Error("task failed")
	}
}

func (p *ThreadPool) submit(task *Task) {
	p.taskQueue.Submit(task)
}

// TODO(hongyunyan):thread pool 结束前清空所有的数据，不同 task 可以选择不一样的方式，可以直接暴力丢弃等，也可以要求全部执行完
// 这个具体的含义到时候再确认一下，感觉怪怪的
func (p *ThreadPool) finish() {
	p.taskQueue.Finish()
}

func (p *ThreadPool) waitForStop() error {
	p.wg.Wait()
	return nil
}
