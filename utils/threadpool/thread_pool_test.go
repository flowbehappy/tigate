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
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func waitForDone(h *TaskHandle) {
	for !h.IsDone() {
		time.Sleep(1 * time.Millisecond)
	}
}

// BasicThreadPool test the basic functionality of threadpool
// Including the functionality of CPU Threadpoool, IO Threadpool, and WaitReactor
func TestBasicThreadPool(t *testing.T) {
	// Test CPU Task
	{
		testCount = 0
		taskScheduler := NewTaskSchedulerDefault("BasicCPUTask")
		n := 10000
		ths := make([]*TaskHandle, 0, n)
		for i := 0; i < n; i++ {
			ths = append(ths, taskScheduler.Submit(newBasicCPUTask(), CPUTask, time.Now()))
		}
		for i := 0; i < n; i++ {
			waitForDone(ths[i])
		}
		taskScheduler.Stop()
		assert.Equal(t, testCount, int64(10000*100))
	}

	// Test IO Task
	{
		testCount = 0
		taskScheduler := NewTaskSchedulerDefault("BasicIOTask")
		n := 1000
		ths := make([]*TaskHandle, 0, n)
		for i := 0; i < n; i++ {
			ths = append(ths, taskScheduler.Submit(newBasicIOTask(), IOTask, time.Now()))
		}
		for i := 0; i < n; i++ {
			waitForDone(ths[i])
		}
		taskScheduler.Stop()
		assert.Equal(t, testCount, int64(1000))
	}

	// Test Wait+CPU Task
	{
		testCount = 0
		taskScheduler := NewTaskSchedulerDefault("BasicWaitAndCPUTask")
		n := 1000
		ths := make([]*TaskHandle, 0, n)
		for i := 0; i < n; i++ {
			ths = append(ths, taskScheduler.Submit(newBasicWaitTask(), CPUTask, time.Now()))
		}
		for i := 0; i < n; i++ {
			waitForDone(ths[i])
		}
		taskScheduler.Stop()
		assert.Equal(t, testCount, int64(1000))
	}
}

type onceTask struct {
	id TaskId

	runType TaskStatus
	msgChan chan TaskId

	t  *testing.T
	wg *sync.WaitGroup
}

func (t *onceTask) TaskId() TaskId { return t.id }

func (t *onceTask) Execute() (TaskStatus, time.Time) {

	t.msgChan <- t.id

	t.wg.Done()
	return Done, time.Time{}
}

type statusAndOrder struct {
	status TaskStatus
	order  int64
}

type statusAndTime struct {
	status TaskStatus
	time   time.Time
}

type longRunTask struct {
	id TaskId

	runs int
	next []statusAndTime

	msgChan chan TaskId

	t  *testing.T
	wg *sync.WaitGroup
}

func (t *longRunTask) TaskId() TaskId { return t.id }

func (t *longRunTask) Execute() (TaskStatus, time.Time) {
	t.msgChan <- t.id
	t.runs++

	if t.next[t.runs].status == Done {
		t.wg.Done()
		return Done, time.Time{}
	}

	return t.next[t.runs].status, t.next[t.runs].time
}

func TestThreadPoolTiming(t *testing.T) {
	msgChan := make(chan TaskId, 1000)
	var wg sync.WaitGroup
	var countDown int = 0

	ts := NewTaskScheduler("TimingTest", 1, 1)
	// To make the scheduler stop executing any tasks
	now := time.Now()

	once := func(runType TaskStatus, taskId uint64, order int64) {
		task := onceTask{
			id:      TaskId(taskId),
			runType: runType,
			msgChan: msgChan,
			t:       t,
			wg:      &wg,
		}
		ts.Submit(&task, runType, now.Add(time.Duration(order)))
		countDown++
	}
	longRun := func(taskId uint64, order []statusAndOrder) {
		ss := make([]statusAndTime, 0)
		for _, s := range order {
			ss = append(ss, statusAndTime{s.status, now.Add(time.Duration(s.order))})
		}

		task := longRunTask{
			id:      TaskId(taskId),
			runs:    0,
			next:    ss,
			msgChan: msgChan,
			t:       t,
			wg:      &wg,
		}
		ts.Submit(&task, ss[0].status, ss[0].time)
		countDown++
	}

	expected := make([]TaskId, 0)

	// Don't execute any task before we put enough tasks into the scheduler.
	ts.blockForTest(6, CPUTask)

	once(CPUTask, 1, 0)
	once(CPUTask, 2, 2)
	once(CPUTask, 3, 1)
	longRun(4, []statusAndOrder{{CPUTask, 3}, {CPUTask, 6}, {CPUTask, 9}, {Done, 0}})
	longRun(5, []statusAndOrder{{CPUTask, 4}, {CPUTask, 7}, {CPUTask, 10}, {Done, 0}})
	longRun(6, []statusAndOrder{{CPUTask, 5}, {CPUTask, 8}, {CPUTask, 11}, {Done, 0}})

	expected = append(expected, 1, 3, 2, 4, 5, 6, 4, 5, 6, 4, 5, 6)
	wg.Add(countDown)

	wg.Wait()
	close(msgChan)

	result := make([]TaskId, 0, len(expected))
	for m := range msgChan {
		result = append(result, m)
	}
	assert.DeepEqual(t, result, expected)

	ts.Stop()
}
