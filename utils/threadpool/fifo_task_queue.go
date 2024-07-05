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

// FIFOTaskQueue is a FIFO task queue.
// TODO(hongyunyan):check whether there is a better way to implement a FIFO task queue.
type FIFOTaskQueue struct {
	queue      []*Task
	mutex      sync.Mutex
	cond       *sync.Cond
	isFinished bool
}

func NewFIFOTaskQueue() *FIFOTaskQueue {
	q := &FIFOTaskQueue{isFinished: false}
	q.cond = sync.NewCond(&q.mutex)
	return q
}

func (q *FIFOTaskQueue) Take() (bool, *Task) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	for {
		if q.isFinished {
			return false, nil
		}

		if len(q.queue) > 0 {
			task := q.queue[0]
			q.queue = q.queue[1:]
			return true, task
		}
		q.cond.Wait()
	}
}

func (q *FIFOTaskQueue) Submit(task *Task) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.isFinished {
		(*task).Release()
		return
	}

	q.queue = append(q.queue, task)
	q.cond.Signal()
}

func (q *FIFOTaskQueue) Finish() {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.isFinished = true

	q.cond.Broadcast() // 不确定是否需要，理论上不应该，但是可能会有异常？
}
