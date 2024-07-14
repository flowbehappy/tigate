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
)

/*
ThreadPool is used to continuously get tasks and execute them in a fixed number of threads.
We can use timeout to control the maximum execution time of each task.
*/
type ThreadPool struct {
	taskType    TaskStatus
	name        string
	threadCount int

	schedule toBeScheduled
	taskChan chan Task

	waitReactor *waitReactor

	stopSignal chan struct{}
	wg         sync.WaitGroup
}

func NewThreadPool(taskType TaskStatus, name string, schedule toBeScheduled, threadCount int) *ThreadPool {
	tp := &ThreadPool{
		taskType:    taskType,
		name:        name,
		threadCount: threadCount,

		schedule:   schedule,
		taskChan:   make(chan Task, threadCount),
		stopSignal: make(chan struct{}),
	}

	tp.waitReactor = newWaitReactor(tp)

	for i := 0; i < threadCount; i++ {
		tp.wg.Add(1)
		go tp.loop(i)

	}
	return tp
}

func (p *ThreadPool) toBeExecuted() chan Task            { return p.taskChan }
func (r *ThreadPool) tobeScheduled() chan *scheduledTask { return r.waitReactor.tobeScheduled() }

func (p *ThreadPool) loop(int) {
	defer p.wg.Done()

	for {
		select {
		case <-p.stopSignal:
			return
		case task := <-p.taskChan:
			p.handleTask(task)
			continue
		}
	}
}

// Execute the task, and push the task back to the correct thread pool if the task is not done.
func (p *ThreadPool) handleTask(task Task) {
	nextStatus, nextTime := task.Execute(p.taskType)
	scheduleTask(task, nextStatus, nextTime, p.schedule)
}

func (p *ThreadPool) finish() {
	p.waitReactor.finish()
	close(p.stopSignal)
}

func (p *ThreadPool) waitForStop() {
	p.waitReactor.waitForStop()
	p.wg.Wait()
}
