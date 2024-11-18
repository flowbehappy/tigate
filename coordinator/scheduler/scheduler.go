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
	"math/rand"
	"time"

	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/server/watcher"
)

// Scheduler generates operators for the maintainers, and push them to the operator controller
// it generates add operator for the absent maintainers, and move operator for the unbalanced replicating maintainer
// currently, it only supports balance the maintainers by size
type Scheduler struct {
	batchSize            int
	random               *rand.Rand
	lastRebalanceTime    time.Time
	checkBalanceInterval time.Duration
	// forceBalance forces the scheduler to produce schedule tasks regardless of
	// `checkBalanceInterval`.
	// It is set to true when the last time `Schedule` produces some tasks,
	// and it is likely there are more tasks will be produced in the next
	// `Schedule`.
	// It speeds up rebalance.
	forceBalance bool

	operatorController *operator.Controller
	changefeedDB       *changefeed.ChangefeedDB
	nodeManager        *watcher.NodeManager

	// buffer for the un-scheduled changefeed
	absent []*changefeed.Changefeed
}

func NewScheduler(
	batchSize int,
	oc *operator.Controller,
	db *changefeed.ChangefeedDB,
	nodeManager *watcher.NodeManager,
	balanceInterval time.Duration) *Scheduler {
	return &Scheduler{
		batchSize:            batchSize,
		random:               rand.New(rand.NewSource(time.Now().UnixNano())),
		checkBalanceInterval: balanceInterval,
		operatorController:   oc,
		changefeedDB:         db,
		nodeManager:          nodeManager,
		lastRebalanceTime:    time.Now(),
		absent:               make([]*changefeed.Changefeed, 0, batchSize),
	}
}

// Execute periodically execute the operator
func (s *Scheduler) Execute() time.Time {
	if s.changefeedDB.GetAbsentSize() > 0 {
		availableSize := s.batchSize - s.operatorController.OperatorSize()
		if availableSize <= 0 {
			return time.Now().Add(time.Millisecond * 500)
		}
		// too many running operators, skip
		if availableSize < s.batchSize/2 {
			return time.Now().Add(time.Millisecond * 100)
		}
		absent, nodeSize := s.changefeedDB.GetWaitingSchedulingChangefeeds(s.absent, availableSize)
		// add the absent node to the node size map
		// todo: use the bootstrap nodes
		for id := range s.nodeManager.GetAliveNodes() {
			if _, ok := nodeSize[id]; !ok {
				nodeSize[id] = 0
			}
		}
		scheduler.BasicSchedule(availableSize, absent, nodeSize, func(cf *changefeed.Changefeed, nodeID node.ID) bool {
			return s.operatorController.AddOperator(operator.NewAddMaintainerOperator(s.changefeedDB, cf, nodeID))
		})

		s.absent = absent[:0]
	} else {
		s.balance()
	}
	return time.Now().Add(time.Millisecond * 500)
}

// balance balances the maintainers by size
func (s *Scheduler) balance() {
	if !s.forceBalance && time.Since(s.lastRebalanceTime) < s.checkBalanceInterval {
		// skip balance.
		return
	}
	if s.operatorController.OperatorSize() > 0 {
		// not in stable schedule state, skip balance
		return
	}

	// check the balance status
	moveSize := scheduler.CheckBalanceStatus(s.changefeedDB.GetTaskSizePerNode(), s.nodeManager.GetAliveNodes())
	if moveSize <= 0 {
		// fast check the balance status, no need to do the balance,skip
		return
	}
	// balance changefeeds among the active nodes
	s.forceBalance = scheduler.Balance(s.batchSize, s.random, s.nodeManager.GetAliveNodes(), s.changefeedDB.GetReplicating(),
		func(cf *changefeed.Changefeed, nodeID node.ID) bool {
			return s.operatorController.AddOperator(operator.NewMoveMaintainerOperator(s.changefeedDB, cf, cf.GetNodeID(), nodeID))
		})
	s.lastRebalanceTime = time.Now()
}
