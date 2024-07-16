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
	// "fmt"
	"sync"
	"time"

	"container/heap"
)

type taskHeap []*scheduledTask

// ====================================
// Notice: Don't call those methods below directly! They are only called by heap package.

func (h taskHeap) Len() int           { return len(h) }
func (h taskHeap) Less(i, j int) bool { return h[i].time.Compare(h[j].time) < 0 }
func (h taskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = h[j].index, h[i].index
}
func (h *taskHeap) Push(x interface{}) {
	index := len(*h)
	*h = append(*h, x.(*scheduledTask))
	(*h)[index].index = index
}

// Pop the last element of the slice.
func (h *taskHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[0 : n-1]
	return x
}

// ====================================

func (h *taskHeap) addOrUpdateTask(st *scheduledTask) {
	if st.index >= 0 {
		heap.Fix(h, st.index)
	} else {
		heap.Push(h, st)
	}
}

func (h *taskHeap) popTopTask() *scheduledTask {
	if h.Len() == 0 {
		return nil
	}
	st := heap.Pop(h).(*scheduledTask)
	st.index = -1 // Mark the task is not in the heap
	return st
}
func (h *taskHeap) peekTopTask() *scheduledTask {
	if h.Len() == 0 {
		return nil
	}
	return (*h)[0]
}

func (h *taskHeap) removeTask(st *scheduledTask) {
	if st.index >= 0 {
		heap.Remove(h, st.index)
		st.index = -1 // Mark the task is not in the heap
	}
}

type priorityQueue struct {
	taskHeap taskHeap
	taskMap  *taskMap

	// We reuse the mutex in the taskMap. To save some CPU cycles.
}

func newPriorityQueue(taskMap *taskMap) *priorityQueue {
	return &priorityQueue{taskHeap: make(taskHeap, 0, 4096), taskMap: taskMap}
}

func (q *priorityQueue) len() int {
	q.taskMap.mutex.Lock()
	defer q.taskMap.mutex.Unlock()

	return q.taskHeap.Len()
}

func (q *priorityQueue) pushTask(newTask *scheduledTask) {
	if newTask.index != -1 {
		panic("the task is already in the heap")
	}

	q.taskMap.mutex.Lock()
	defer q.taskMap.mutex.Unlock()

	taskId := newTask.taskId
	if st, ok := q.taskMap.idToTask[taskId]; ok {
		st.time = newTask.time
		st.status = newTask.status

		q.taskHeap.addOrUpdateTask(st)
		// fmt.Printf("updateTask task(duplicate): %d, status: %v\n", taskId, task.status)
		return
	} else {
		_, isEmptyTask := newTask.task.(emptyTask)
		_, isUpdateTask := newTask.task.(updateTask)
		if isEmptyTask || isUpdateTask {
			panic("emptyTask or updateTask should not be submitted to execute")
		}
		q.taskHeap.addOrUpdateTask(newTask)
		q.taskMap.idToTask[taskId] = newTask
	}
}

func (q *priorityQueue) removeTask(taskId TaskId) *scheduledTask {
	q.taskMap.mutex.Lock()
	defer q.taskMap.mutex.Unlock()

	if st, ok := q.taskMap.idToTask[taskId]; ok {
		q.taskHeap.removeTask(st)
		delete(q.taskMap.idToTask, taskId)
		return st
	}
	return nil
}

func (q *priorityQueue) peekTopTask() *scheduledTask {
	q.taskMap.mutex.Lock()
	defer q.taskMap.mutex.Unlock()

	return q.taskHeap.peekTopTask()
}

func (q *priorityQueue) popTopTask() *scheduledTask {
	q.taskMap.mutex.Lock()
	defer q.taskMap.mutex.Unlock()

	return q.taskHeap.popTopTask()
}

// waitReactor waits for tasks to be ready for execution.
// It use two goroutines:
//   - One goroutine keeps pullling tasks from newTaskChan and push into waitingQueue
//   - One goroutine keeps checking the waitingQueue and push the ready tasks to toBeExecuted (thread pool).
type waitReactor struct {
	// New tasks are submitted via newTaskChan
	newTaskChan chan *scheduledTask
	// Tasks are waiting in the waitingQueue
	waitingQueue *priorityQueue
	// The update signal of waitingQueue
	queueUpdateSignal chan struct{}
	// Tasks ready for execution are pushed to taskRunnar
	toBeExecuted toBeExecuted

	stopSignal chan struct{}
	wg         sync.WaitGroup

	// It is only use in test cases, to stop the schedule working.
	blockUntil int
	freeToRun  bool
}

func newWaitReactor(toBeExecuted toBeExecuted, taskMap *taskMap) *waitReactor {
	waitReactor := waitReactor{
		newTaskChan:       make(chan *scheduledTask, 4096),
		waitingQueue:      newPriorityQueue(taskMap),
		queueUpdateSignal: make(chan struct{}, 1), // We don't need more than one signal in the channel.
		toBeExecuted:      toBeExecuted,
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

func (r *waitReactor) removeTaskIfDone(st *scheduledTask) bool {
	if st.status == Done {
		r.waitingQueue.removeTask(st.taskId)
		return true
	}
	return false
}

// Push the new task to the waitingQueue.
func (r *waitReactor) scheduleTaskLoop() {
	defer func() {
		r.wg.Done()
		// fmt.Println("scheduleTaskLoop done")
	}()

	for {
		select {
		case task := <-r.newTaskChan:
			// fmt.Printf("schedule task: %d, status: %v, waitingQueue:%d\n", task.task.TaskId(), task.status, r.waitingQueue.len())
			if r.removeTaskIfDone(task) {
				continue
			}
			r.waitingQueue.pushTask(task)
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
		// fmt.Println("executeTaskLoop done")
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
			// fmt.Println("waitReactor is disabled")
			for r.waitingQueue.len() < r.blockUntil {
				time.Sleep(10 * time.Millisecond)
			}
			r.freeToRun = true
			continue
		}

		// fmt.Printf("waitingQueue:%d\n", r.waitingQueue.len())

		now := time.Now()

		nearestTime := time.Time{}
		ready := false

		// We first peek the top task to see if it is ready.
		// If yes, we pop it and push it to the toBeExecuted.
		// We don't pop the task before we believe it is ready.
		// Because pop is more expensive than peek.
		task := r.waitingQueue.peekTopTask()

		if task != nil && (!r.removeTaskIfDone(task)) {
			nearestTime = task.time
			ready = !task.time.After(now)
			if ready {
				task = r.waitingQueue.popTopTask()
				// Here we do the check again because the task may be updated by other goroutines.
				if task != nil && (!r.removeTaskIfDone(task)) {
					// fmt.Printf("popTask task: %d, status: %d\n", task.task.TaskId(), task.status)

					nearestTime = task.time
					ready = !task.time.After(now)
					if !ready {
						r.waitingQueue.pushTask(task)
						task = nil

						// fmt.Printf("push back task: %d, status: %d\n", task.task.TaskId(), task.status)
					}
				}
			} else {
				task = nil
			}
		}

		// fmt.Println("nearestTime:", nearestTime, "ready:", ready)

		if ready {
			select {
			// Wait until the runner to execute the task.
			case r.toBeExecuted.toBeExecuted(task.status) <- task:
				// fmt.Printf("push to toBeExecuted task: %d, status: %d\n", task.task.TaskId(), task.status)
				break
			case <-r.stopSignal:
				return
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

func (r *waitReactor) tobeScheduled() chan *scheduledTask { return r.newTaskChan }

func (r *waitReactor) stop() { close(r.stopSignal) }

func (r *waitReactor) waitForStop() { r.wg.Wait() }
