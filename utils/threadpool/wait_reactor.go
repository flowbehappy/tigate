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

import "sync"

type WaitReactor struct {
	queue         WaitingTaskList
	mutex         sync.Mutex
	wg            sync.WaitGroup
	threadCount   int
	taskScheduler *TaskScheduler
}

func newWaitReactor(threadCount int, taskScheduler *TaskScheduler) *waitReactor {
	waitReactor := WaitReactor{
		threadCount:   threadCount,
		taskScheduler: taskScheduler,
	}
	for i := 0; i < threadCount; i++ {
		waitReactor.wg.Add(1)
		go waitReactor.loop(i)
	}
}

func (r *WaitReactor) takeFromWaitingTaskList(waitingTaskList []*Task) bool {
	if len(waitingTaskList) == 0 {
		return r.queue.Take(waitingTaskList)
	} else {
		return r.queue.TryTake(waitingTaskList)
	}
}

func (r *WaitReactor) react(waitingTasks []*Task) {
	var newWaitingTasks []*Task
	for _, task := range waitingTasks {
		status := (*task).await()
		switch status {
		case running:
			r.taskScheduler.submitTaskToIOThreadPool(task)
		case io:
			r.taskScheduler.submitTaskToIOThreadPool(task)
		case blocked:
			newWaitingTasks = append(newWaitingTasks, task)
		case success:
			// 不应该吧，需要报错
		case failed:
			// 报错
		default:
			panic("unknown task status")
		}
	}

	waitingTasks = newWaitingTasks
}

func (r *WaitReactor) loop(threadIndex int) {
	var waitingTasks []*Task
	for r.takeFromWaitingTaskList(waitingTasks) {
		r.react(waitingTasks)
	}
	for len(waitingTasks) > 0 {
		r.react(waitingTasks)
	}
}

func (r *WaitReactor) submit(task *Task) {
	r.queue.Submit(task)
}
