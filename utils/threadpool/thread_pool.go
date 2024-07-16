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
)

// threadPool is used to continuously get tasks and execute them in a fixed number of threads.
type threadPool struct {
	taskType    TaskStatus
	name        string
	threadCount int

	schedule toBeScheduled
	taskChan chan *scheduledTask

	stopSignal chan struct{}
	wg         sync.WaitGroup
}

func newthreadPool(taskType TaskStatus, name string, schedule toBeScheduled, threadCount int) *threadPool {
	tp := &threadPool{
		taskType:    taskType,
		name:        name,
		threadCount: threadCount,

		schedule:   schedule,
		taskChan:   make(chan *scheduledTask, threadCount),
		stopSignal: make(chan struct{}),
	}

	for i := 0; i < threadCount; i++ {
		tp.wg.Add(1)
		go tp.loop(i)

	}
	return tp
}

func (p *threadPool) toBeExecuted() chan *scheduledTask { return p.taskChan }

func (p *threadPool) loop(int) {
	defer p.wg.Done()

	for {
		select {
		case <-p.stopSignal:
			return
		case st := <-p.taskChan:
			p.handleTask(st)
			continue
		}
	}
}

// Execute the task, and push the task back to the correct thread pool if the task is not done.
func (p *threadPool) handleTask(st *scheduledTask) {
	{
		if !st.runMutex.TryLock() {
			// The task is running in another thread.
			// And the task will be pushed back to the waitingQueue by the thread that is executing the task.
			return
		}
		defer st.runMutex.Unlock()

		// Only execute the task when it is not done.
		// And for the done task, we push it back to waitReactor to remove it from the taskMap later.
		if TaskStatus(st.status.Load()) != Done {
			nextStatus, nextTime := st.task.Execute()
			if nextStatus == Done {
				st.time = time.Time{}
				// Here we just return and drop the task.
				// Because we know that the task is not in any priorityQueue, and don't need to do any clean up.
				return
			}
			for {
				s := st.status.Load()
				if s == int32(Done) {
					// The task is done, and we will not push it back to the waitingQueue.
					st.time = time.Time{}
					return
				}
				if st.status.CompareAndSwap(s, int32(nextStatus)) {
					break
				}
			}
			st.time = nextTime
		}
	}

	p.schedule.toBeScheduled(TaskStatus(st.status.Load())) <- st
}

func (p *threadPool) stop() {
	close(p.stopSignal)
}

func (p *threadPool) waitForStop() {
	p.wg.Wait()
}
