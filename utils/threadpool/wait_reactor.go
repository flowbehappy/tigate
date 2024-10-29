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

	"github.com/flowbehappy/tigate/utils/heap"
)

type taskAndTime struct {
	task *scheduledTask
	time time.Time
}

type priorityQueue struct {
	taskHeap *heap.Heap[*scheduledTask]
	mutex    sync.Mutex
}

func newPriorityQueue() *priorityQueue {
	return &priorityQueue{taskHeap: heap.NewHeap[*scheduledTask]()}
}

func (q *priorityQueue) len() int {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	return q.taskHeap.Len()
}

func (q *priorityQueue) addOrUpdateTask(st *scheduledTask) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.taskHeap.AddOrUpdate(st)
}

func (q *priorityQueue) peekTopTask() *scheduledTask {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	top, _ := q.taskHeap.PeekTop()
	return top
}

func (q *priorityQueue) popTopTask() *scheduledTask {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	top, _ := q.taskHeap.PopTop()
	return top
}

// waitReactor waits for tasks to be ready for execution.
// It use two goroutines:
//   - One goroutine keeps pullling tasks from newTaskChan and push into waitingQueue
//   - One goroutine keeps checking the waitingQueue and push the ready tasks to thread pool
type waitReactor struct {
	// New tasks are submitted via newTaskChan
	newTaskChan chan taskAndTime
	// Tasks are waiting in the waitingQueue
	waitingQueue *priorityQueue
	// The update signal of waitingQueue
	queueUpdateSignal chan struct{}
	// Tasks ready for execution are pushed to taskRunnar
	threadPool *threadPoolImpl

	stopSignal chan struct{}
	wg         sync.WaitGroup

	// It is only use in test cases, to stop the schedule working.
	blockUntil int
	freeToRun  bool
}

func newWaitReactor(threadPool *threadPoolImpl) *waitReactor {
	waitReactor := waitReactor{
		newTaskChan:       make(chan taskAndTime, 4096),
		waitingQueue:      newPriorityQueue(),
		queueUpdateSignal: make(chan struct{}, 1), // We don't need more than one signal in the channel.
		threadPool:        threadPool,
		stopSignal:        make(chan struct{}),

		freeToRun: true,
	}

	waitReactor.wg.Add(2)
	go waitReactor.scheduleTaskLoop()
	go waitReactor.executeTaskLoop()

	return &waitReactor
}

const (
	maxCheckInterval time.Duration = 50 * time.Millisecond
)

// Don't execute any task before the tasks in the waitingQueue is larger than the given number.
func (r *waitReactor) blockForTest(until int) {
	r.blockUntil = until
	r.freeToRun = false
}

// Push the new task to the waitingQueue.
func (r *waitReactor) scheduleTaskLoop() {
	defer func() {
		r.wg.Done()
	}()

	for {
		select {
		case tt := <-r.newTaskChan:
			// Note that modifying the time of the task is not thread safe.
			// So we need to make sure the task is not modified by other goroutines.
			task := tt.task
			task.time = tt.time
			r.waitingQueue.addOrUpdateTask(task)
			select {
			case r.queueUpdateSignal <- struct{}{}:
				break
			case <-r.stopSignal:
				return
			default:
				break
			}
		case <-r.stopSignal:
			return
		}
	}
}

// Push the ready task to the corresponding thread pool.
func (r *waitReactor) executeTaskLoop() {
	defer func() {
		r.wg.Done()
	}()

	for {
		// Drain the update signal channel. Because we already going to check the waitingQueue,
		// and no need to get the update signal.
		select {
		case <-r.queueUpdateSignal:
		case <-r.stopSignal:
			return
		default:
		}

		if !r.freeToRun {
			for r.waitingQueue.len() < r.blockUntil {
				time.Sleep(10 * time.Millisecond)
			}
			r.freeToRun = true
			continue
		}

		now := time.Now()

		nearestTime := time.Time{}
		ready := false

		// We first peek the top task to see if it is ready.
		// If yes, we pop it and push it to the execute list.
		// We don't pop the task before we believe it is ready.
		// Because pop is more expensive than peek.
		task := r.waitingQueue.peekTopTask()

		if task != nil {
			nearestTime = task.time
			ready = !task.time.After(now)
			if ready {
				task = r.waitingQueue.popTopTask()
				// A task should only be removed by current goroutine.
				// And a task's time can only be set to zero (canceled) if it is in the waitingQueue.
				if task == nil || task.time.After(now) {
					panic("task should not be nil or after now")
				}
			} else {
				task = nil
			}
		}

		if ready {
			// Zero time means the task is done.
			if !task.time.IsZero() {
				select {
				// Wait until the runner to execute the task.
				case r.threadPool.pendingTaskChan <- task:
					break
				case <-r.stopSignal:
					return
				}
			}
		} else {
			var waitTime time.Duration
			if nearestTime.IsZero() {
				waitTime = maxCheckInterval
			} else {
				waitTime = nearestTime.Sub(now)
				if waitTime > maxCheckInterval {
					waitTime = maxCheckInterval
				}
			}
			if waitTime < 0 {
				panic("waitTime should not be negative")
			}

			select {
			case <-time.After(waitTime):
				break
			case <-r.queueUpdateSignal:
				break
			case <-r.stopSignal:
				return
			}
		}
	}
}
