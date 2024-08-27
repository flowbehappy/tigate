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

// BasicThreadPool test the basic functionality of threadpool
// Including the functionality of CPU Threadpoool, IO Threadpool, and WaitReactor
func TestBasicThreadPool(t *testing.T) {
	// Test CPU Task
	{
		testCount = 0
		taskScheduler := NewThreadPoolDefault()
		n := 10000
		wg := &sync.WaitGroup{}
		wg.Add(n)
		for i := 0; i < n; i++ {
			taskScheduler.Submit(newBasicCPUTask(wg), time.Now())
		}
		wg.Wait()
		taskScheduler.Stop()
		assert.Equal(t, testCount, int64(10000*100))
	}
	// Test Wait+CPU Task
	{
		testCount = 0
		taskScheduler := NewThreadPoolDefault()
		n := 1000
		wg := &sync.WaitGroup{}
		wg.Add(n)
		for i := 0; i < n; i++ {
			taskScheduler.Submit(newBasicWaitTask(wg), time.Now())
		}
		wg.Wait()
		taskScheduler.Stop()
		assert.Equal(t, testCount, int64(1000))
	}
}

type onceTask struct {
	id TaskId

	msgChan chan TaskId

	t  *testing.T
	wg *sync.WaitGroup
}

func (t *onceTask) TaskId() TaskId { return t.id }

func (t *onceTask) Execute() time.Time {

	t.msgChan <- t.id

	t.wg.Done()
	return time.Time{}
}

type statusAndOrder struct {
	order int64
}

type statusAndTime struct {
	time time.Time
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

func (t *longRunTask) Execute() time.Time {
	t.msgChan <- t.id
	t.runs++

	if t.next[t.runs].time.IsZero() {
		t.wg.Done()
		return time.Time{}
	}

	return t.next[t.runs].time
}

func TestThreadPoolTiming(t *testing.T) {
	msgChan := make(chan TaskId, 1000)
	var wg sync.WaitGroup
	var countDown int = 0

	ts := newThreadPoolImpl(1)
	// To make the scheduler stop executing any tasks
	now := time.Now()

	once := func(taskId uint64, order int64) {
		task := onceTask{
			id:      TaskId(taskId),
			msgChan: msgChan,
			t:       t,
			wg:      &wg,
		}
		ts.Submit(&task, now.Add(time.Duration(order)))
		countDown++
	}
	longRun := func(taskId uint64, order []statusAndOrder) {
		ss := make([]statusAndTime, 0)
		for _, s := range order {
			if s.order == 0 {
				ss = append(ss, statusAndTime{time.Time{}})
			} else {
				ss = append(ss, statusAndTime{now.Add(time.Duration(s.order))})
			}
		}

		task := longRunTask{
			id:      TaskId(taskId),
			runs:    0,
			next:    ss,
			msgChan: msgChan,
			t:       t,
			wg:      &wg,
		}
		ts.Submit(&task, ss[0].time)
		countDown++
	}

	expected := make([]TaskId, 0)

	// Don't execute any task before we put enough tasks into the scheduler.
	ts.reactor.blockForTest(6)

	once(1, 0)
	once(2, 2)
	once(3, 1)
	longRun(4, []statusAndOrder{{3}, {6}, {9}, {0}})
	longRun(5, []statusAndOrder{{4}, {7}, {10}, {0}})
	longRun(6, []statusAndOrder{{5}, {8}, {11}, {0}})

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

func TestCancelImmediately(t *testing.T) {
	taskScheduler := NewThreadPoolDefault()
	for i := 0; i < 10000; i++ {
		go func() {
			task := NewRepeatedTask(taskScheduler)
			task.Cancel()
		}()
	}
	taskScheduler.Stop()
}

func TestFuncTask(t *testing.T) {
	tp := NewThreadPoolDefault()
	wg := &sync.WaitGroup{}
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		tp.SubmitFunc(func() time.Time {
			time.Sleep(1 * time.Nanosecond)
			wg.Done()
			return time.Time{}
		}, time.Now())
	}

	wg.Add(1000)
	times := 0
	tp.SubmitFunc(func() time.Time {
		wg.Done()
		times++
		if times == 1000 {
			return time.Time{}
		} else {
			return time.Now().Add(1 * time.Nanosecond)
		}
	}, time.Now())

	wg.Wait()
	tp.Stop()
}
