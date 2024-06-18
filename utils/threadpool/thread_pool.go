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

// enum

type TaskStatus int

const (
	success TaskStatus = iota
	failed
	running
	blocked
	io
)

// 本质是想写一个 thread pool 的基类，可以先按照特例来写，后面再转成 basic 的
// 接受外层的 queue，和 schedule,
// 提供最基础的 提交任务，拿出任务执行的能力
type ThreadPool struct {
	scheduler   *TaskScheduler // 这理论上是个接口，这玩意倒是真不一定
	taskQueue   *TaskQueue     // 接口2
	threadCount int            // 开多少个线程
	wg          sync.WaitGroup
	timeout     time.Duration // 每个 task 最多能跑多少时间，就要被换出来
}

func newThreadPool(scheduler *TaskScheduler, taskQueue *TaskQueue, threadCount int) *ThreadPool {
	threadPool := ThreadPool{
		scheduler:   scheduler,
		taskQueue:   taskQueue,
		threadCount: threadCount,
		timeout:     100 * time.Millisecond,
	}

	for i := 0; i < threadCount; i++ {
		threadPool.wg.Add(1)
		go threadPool.loop(i)

	}
	return &threadPool
}

func (p *ThreadPool) loop(threadNumber int) {
	var task *Task // Task 应该也是个接口
	for (*p.taskQueue).Take(task) {
		p.handleTask(task)
	}
}

func (p *ThreadPool) handleTask(task *Task) {
	// 调用 task，并且根据 task 结束状态决定应该放到哪里去
	status := (*task).execute(p.timeout)
	switch status {
	case running:
		p.scheduler.submitTaskToCPUThreadPool(task) // 仍进去做 cpu 任务
	case blocked:
		p.scheduler.submitTaskToWaitReactorThreadPool(task) //扔进去等 wait reactor
	case io:
		p.scheduler.submitTaskToIOThreadPool(task) // 仍进去做 io 任务
	case success:
		// do nothing?
	case failed:
		// 报错，但是不影响后面的 task
	}
}

func (p *ThreadPool) submit(task *Task) {
	(*p.taskQueue).Submit(task)
}

// thread pool 结束前清空所有的数据，不同 task 可以选择不一样的方式，可以直接暴力丢弃等，也可以要求全部执行完
// 这个具体的含义到时候再确认一下，感觉怪怪的
func (p *ThreadPool) finish() {
	(*p.taskQueue).Finish()
}

func (p *ThreadPool) waitForStop() error {
	log.Info("Thread pool is waiting for stop")
	p.wg.Wait()
	log.Info("Thread pool is stopped")
	return nil
}
