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

package scheduler

import (
	"time"

	"github.com/pingcap/ticdc/utils/threadpool"
)

const DefaultCheckInterval = time.Second * 120

const (
	BasicScheduler   = "basic-scheduler"
	BalanceScheduler = "balance-scheduler"
	SplitScheduler   = "split-scheduler"
)

type Scheduler interface {
	threadpool.Task
	Name() string
}

// Scheduler generates operators for the spans, and push them to the operator controller
// it generates add operator for the absent spans, and move operator for the unbalanced replicating spans
// currently, it only supports balance the spans by size
type Controller struct {
	schedulers map[string]Scheduler
}

func NewController(schedulers map[string]Scheduler) *Controller {
	if schedulers[BasicScheduler] == nil {
		panic("basic scheduler is required")
	}
	return &Controller{
		schedulers: schedulers,
	}
}

func (sm *Controller) Start(taskScheduler threadpool.ThreadPool) (handles []*threadpool.TaskHandle) {
	basicScheduler := sm.schedulers[BasicScheduler]
	handles = append(handles, taskScheduler.Submit(basicScheduler, time.Now()))

	checkerSchedulers := []Scheduler{}
	for _, scheduler := range sm.schedulers {
		if scheduler.Name() != BasicScheduler {
			checkerSchedulers = append(checkerSchedulers, scheduler)
		}
	}
	handles = append(handles, taskScheduler.SubmitFunc(
		// Run all checker schedulers in a single goroutine since these schedulers are
		// not critical and a slight delay in their execution is acceptable.
		func() time.Time {
			next := time.Now().Add(DefaultCheckInterval)
			for _, scheduler := range checkerSchedulers {
				nextCheckTime := scheduler.Execute()
				if next.After(nextCheckTime) {
					next = nextCheckTime
				}
			}
			return next
		},
		time.Now(),
	))
	return handles
}

func (sm *Controller) GetSchedulers() (s []Scheduler) {
	for _, scheduler := range sm.schedulers {
		s = append(s, scheduler)
	}
	return s
}

func (sm *Controller) GetScheduler(name string) Scheduler {
	return sm.schedulers[name]
}
