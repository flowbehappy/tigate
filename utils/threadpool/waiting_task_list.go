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

type WaitingTaskList struct {
	queue      []*Task
	mutex      sync.Mutex
	cond       *sync.Cond
	isFinished bool
}

func newWaitingTaskList() *WaitingTaskList {
	waitingTaskList := WaitingTaskList{
		isFinished: false,
	}
	waitingTaskList.cond = sync.NewCond(&waitingTaskList.mutex)
	return &waitingTaskList
}

func (l *WaitingTaskList) Submit(task *Task) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.isFinished {
		(*task).release()
		return
	}

	l.queue = append(l.queue, task)
	l.cond.Signal()
}

func (l *WaitingTaskList) Finish() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.isFinished = true

	l.cond.Broadcast() // 不确定是否需要，理论上不应该，但是可能会有异常？
}

func (l *WaitingTaskList) Take(taskList []*Task) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	for {
		if len(l.queue) > 0 {
			break
		}

		if l.isFinished {
			return false
		}

		l.cond.Wait()
	}
	taskList = append(taskList, l.queue...)
	l.queue = l.queue[:0]
	return true
}

func (l *WaitingTaskList) TryTake(taskList []*Task) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.isFinished {
		return false
	}

	if len(l.queue) == 0 {
		return false
	}

	taskList = append(taskList, l.queue...)
	l.queue = l.queue[:0]
	return true
}

func (l *WaitingTaskList) finish() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.isFinished = true
	l.cond.Broadcast()
}
