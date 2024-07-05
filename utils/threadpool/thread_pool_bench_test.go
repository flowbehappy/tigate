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
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func shuffle(slice []int) {
	for i := len(slice) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		slice[i], slice[j] = slice[j], slice[i]
	}
}

// BenchmarkCPUThreadPool and BenchmarkCPUThreadPoolWithWaitReactor want to compare the performance of CPU ThreadPool and WaitReactor
// We make a task, that when the chan fetch the value, then do atomic.Add tasls
// We implementation the task in two ways (1. as CPU Task 2. as CPU + Wait Task) to compare the cost time and cpu usage.
func benchmarkCPUThreadPool(addCount int, sleepTime int, b *testing.B) {
	count := 1000
	for k := 0; k < b.N; k++ {
		testCount = 0
		channel := make(chan int, 1)
		chanVec := make([]chan int, count)
		for i := 0; i < count; i++ {
			chanVec[i] = make(chan int, 1)
		}
		taskScheduler := NewTaskScheduler(&DefaultTaskSchedulerConfig, "PrueCPUTask")
		for i := 0; i < count; i++ {
			taskScheduler.Submit(newPureCPUTask(&channel, &chanVec[i], int64(count), addCount))
		}

		index := make([]int, count)
		for i := 0; i < count; i++ {
			index[i] = i
		}
		shuffle(index)

		for i := 0; i < count; i++ {
			chanVec[index[i]] <- 1
			time.Sleep(time.Duration(sleepTime) * time.Microsecond)
		}

		<-channel
		taskScheduler.Finish()
	}
}

func benchmarkCPUThreadPoolWithWaitReactor(addCount int, sleepTime int, b *testing.B) {
	count := 1000
	for k := 0; k < b.N; k++ {
		testCount = 0
		channel := make(chan int, 1)
		chanVec := make([]chan int, count)
		for i := 0; i < count; i++ {
			chanVec[i] = make(chan int, 1)
		}
		taskScheduler := NewTaskScheduler(&DefaultTaskSchedulerConfig, "CPUWithWaitTask")
		for i := 0; i < count; i++ {
			taskScheduler.Submit(newCPUWithWaitTask(&channel, &chanVec[i], int64(count), addCount))
		}
		index := make([]int, count)
		for i := 0; i < count; i++ {
			index[i] = i
		}
		shuffle(index)

		for i := 0; i < count; i++ {
			chanVec[index[i]] <- 1
			time.Sleep(time.Duration(sleepTime) * time.Microsecond)
		}

		<-channel
		taskScheduler.Finish()
	}
}

func BenchmarkCPUThreadPoolWithWaitReactor10000x1(b *testing.B) {
	benchmarkCPUThreadPoolWithWaitReactor(10000, 1, b)
}
func BenchmarkCPUThreadPoolWithWaitReactor10000x10(b *testing.B) {
	benchmarkCPUThreadPoolWithWaitReactor(10000, 10, b)
}
func BenchmarkCPUThreadPoolWithWaitReactor10000x100(b *testing.B) {
	benchmarkCPUThreadPoolWithWaitReactor(10000, 100, b)
}
func BenchmarkCPUThreadPoolWithWaitReactor10000x1000(b *testing.B) {
	benchmarkCPUThreadPoolWithWaitReactor(10000, 1000, b)
}

func BenchmarkCPUThreadPool10000x1(b *testing.B) {
	benchmarkCPUThreadPool(10000, 1, b)
}
func BenchmarkCPUThreadPool10000x10(b *testing.B) {
	benchmarkCPUThreadPool(10000, 10, b)
}
func BenchmarkCPUThreadPool10000x100(b *testing.B) {
	benchmarkCPUThreadPool(10000, 100, b)
}
func BenchmarkCPUThreadPool10000x1000(b *testing.B) {
	benchmarkCPUThreadPool(10000, 1000, b)
}

// These two test cases are used to compare the cost time and cpu usage of ThreadPool and GoRoutine.
func benchmarkCostThreadPool(taskCount int, addCount int, b *testing.B) {
	taskScheduler := NewTaskScheduler(&DefaultTaskSchedulerConfig, "CPUTimeTask")
	for k := 0; k < b.N; k++ {
		testCount = 0
		channel := make(chan int, 1)
		for i := 0; i < taskCount; i++ {
			taskScheduler.Submit(newCPUTimeTask(&channel, addCount, taskCount))
		}
		<-channel
	}
	taskScheduler.Finish()
}

func benchmarkCostGoRoutine(taskCount int, addCount int, b *testing.B) {
	for k := 0; k < b.N; k++ {
		testCount = 0
		channel := make(chan int, 1)
		for i := 0; i < taskCount; i++ {
			go func(finalChan *chan int) {
				for i := 0; i < addCount; i++ {
					atomic.AddInt64(&testCount, 1)
				}
				if atomic.LoadInt64(&testCount) == int64(taskCount*addCount) {
					*finalChan <- 1
				}
			}(&channel)
		}
		<-channel
	}
}

func BenchmarkCostThreadPool1000x1000(b *testing.B) {
	benchmarkCostThreadPool(1000, 1000, b)
}

func BenchmarkCostThreadPool10000x1000(b *testing.B) {
	benchmarkCostThreadPool(10000, 1000, b)
}

func BenchmarkCostThreadPool1000x10000(b *testing.B) {
	benchmarkCostThreadPool(1000, 10000, b)
}

func BenchmarkCostThreadPool100000x1000(b *testing.B) {
	benchmarkCostThreadPool(100000, 1000, b)
}

func BenchmarkCostGoRoutine1000x1000(b *testing.B) {
	benchmarkCostGoRoutine(1000, 1000, b)
}

func BenchmarkCostGoRoutine10000x1000(b *testing.B) {
	benchmarkCostGoRoutine(10000, 1000, b)
}

func BenchmarkCostGoRoutine1000x10000(b *testing.B) {
	benchmarkCostGoRoutine(1000, 10000, b)
}

func BenchmarkCostGoRoutine100000x1000(b *testing.B) {
	benchmarkCostGoRoutine(100000, 1000, b)
}
