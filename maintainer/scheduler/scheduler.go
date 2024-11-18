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

	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/threadpool"
)

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
	changefeedID common.ChangeFeedID
	schedulers   map[string]Scheduler
}

func NewController(changefeedID common.ChangeFeedID,
	batchSize int,
	oc *operator.Controller,
	db *replica.ReplicationDB,
	nodeManager *watcher.NodeManager,
	balanceInterval time.Duration,
	splitter *split.Splitter,
) *Controller {
	m := &Controller{
		changefeedID: changefeedID,
		schedulers:   make(map[string]Scheduler),
	}

	m.schedulers[BasicScheduler] = newBasicScheduler(changefeedID, batchSize, oc, db, nodeManager)
	m.schedulers[BalanceScheduler] = newbalanceScheduler(changefeedID, batchSize, oc, db, nodeManager, balanceInterval)
	if splitter != nil {
		m.schedulers[SplitScheduler] = newSplitScheduler(changefeedID, batchSize, splitter, oc, db, nodeManager)
	}
	return m
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

func (sm *Controller) UpdateSpan(span *replica.SpanReplication) {
	if s, ok := sm.schedulers[SplitScheduler]; ok {
		s.(*splitScheduler).updateSpan(span)
	}
}
