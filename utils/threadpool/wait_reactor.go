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

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type WaitReactor struct {
	queue         *WaitingTaskList
	wg            sync.WaitGroup
	threadCount   int
	taskScheduler *TaskScheduler
	name          string
}

func NewWaitReactor(taskScheduler *TaskScheduler, threadCount int, name string) *WaitReactor {
	waitReactor := WaitReactor{
		threadCount:   threadCount,
		taskScheduler: taskScheduler,
		queue:         newWaitingTaskList(),
		name:          name,
	}
	for i := 0; i < threadCount; i++ {
		waitReactor.wg.Add(1)
		go waitReactor.loop(i)
	}
	return &waitReactor
}

func (r *WaitReactor) takeFromWaitingTaskList(waitingTaskList []*Task) (bool, []*Task) {
	if len(waitingTaskList) == 0 {
		return r.queue.Take(waitingTaskList)
	} else {
		return r.queue.TryTake(waitingTaskList)
	}
}

func (r *WaitReactor) react(waitingTasks []*Task) []*Task {
	var newWaitingTasks []*Task
	for _, task := range waitingTasks {
		status := (*task).Await()
		switch status {
		case Running:
			r.taskScheduler.submitTaskToIOThreadPool(task)
		case IO:
			r.taskScheduler.submitTaskToIOThreadPool(task)
		case Waiting:
			newWaitingTasks = append(newWaitingTasks, task)
		case Success:
			// 不应该吧，需要报错
		case Failed:
			// 报错
		default:
			panic("unknown task status")
		}
	}

	return newWaitingTasks
}

func (r *WaitReactor) loop(threadIndex int) {
	var currentTasks []*Task
	for {
		ok, waitingTasks := r.takeFromWaitingTaskList(currentTasks)
		if ok {
			currentTasks = r.react(waitingTasks)
		} else {
			break
		}
	}
	for len(currentTasks) > 0 {
		currentTasks = r.react(currentTasks)
	}
	r.wg.Done()
}

func (r *WaitReactor) submit(task *Task) {
	r.queue.Submit(task)
}

func (r *WaitReactor) finish() {
	r.queue.finish()
}

func (r *WaitReactor) waitForStop() error {
	log.Info("Wait Reactor is waiting for stop", zap.Any("wait reactor name", r.name))
	r.wg.Wait()
	log.Info("Wait Reactor is stopped", zap.Any("wait reactor name", r.name))
	return nil
}
