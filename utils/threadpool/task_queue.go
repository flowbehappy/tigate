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

/*
做一个基础的 接口，必须要满足的功能包括了：
 1. 拿出一个 task
 2. 塞进去一个 task
 3. 达到 finish 状态释放没有跑的 task 资源
*/
type TaskQueue interface {
	Take(task *Task) bool
	Submit(task *Task)
	Finish()
}

// 倒霉玩意，先用切片试一下
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

func (q *FIFOTaskQueue) Take(task *Task) bool {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	for {
		if q.isFinished {
			(*task).release() // 释放资源
			return false
		}

		if len(q.queue) > 0 {
			task = q.queue[0]
			return true
		}
		q.cond.Wait()
	}
}

func (q *FIFOTaskQueue) Submit(task *Task) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.isFinished {
		(*task).release() // 释放资源
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
