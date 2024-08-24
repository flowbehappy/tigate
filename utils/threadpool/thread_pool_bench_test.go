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

func benchmarkCPUThreadPoolWithWaitReactor(addCount int, sleepTime int, b *testing.B) {
	count := 1000
	delay := time.Duration(sleepTime) * time.Microsecond
	for k := 0; k < b.N; k++ {
		testCount = 0
		channel := make(chan int, 1)

		taskScheduler := NewThreadPoolDefault()
		for i := 0; i < count; i++ {
			// We make the scheduler to call the Execute immediately.
			// And the Execute will return the next execution time, i.e. time.Now() + delay
			taskScheduler.Submit(newCPUWithWaitTask(&channel, count, addCount, delay), time.Now())
		}

		<-channel
		taskScheduler.Stop()
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

// These two test cases are used to compare the cost time and cpu usage of ThreadPool and GoRoutine.
func benchmarkCostThreadPool(taskCount int, addCount int, b *testing.B) {
	taskScheduler := NewThreadPoolDefault()
	for k := 0; k < b.N; k++ {
		testCount = 0
		channel := make(chan int, 1)
		for i := 0; i < taskCount; i++ {
			taskScheduler.Submit(newCPUTimeTask(&channel, addCount, taskCount), time.Now())
		}
		<-channel
	}
	taskScheduler.Stop()
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
