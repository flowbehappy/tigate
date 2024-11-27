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

	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/server/watcher"
)

// balanceScheduler is used to check the balance status of all spans among all nodes
type balanceScheduler struct {
	changefeedID common.ChangeFeedID
	batchSize    int

	operatorController *operator.Controller
	replicationDB      *replica.ReplicationDB
	nodeManager        *watcher.NodeManager

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
}

func newbalanceScheduler(
	changefeedID common.ChangeFeedID, batchSize int,
	oc *operator.Controller, db *replica.ReplicationDB, nodeManager *watcher.NodeManager,
	balanceInterval time.Duration,
) *balanceScheduler {
	return &balanceScheduler{
		changefeedID:         changefeedID,
		batchSize:            batchSize,
		random:               rand.New(rand.NewSource(time.Now().UnixNano())),
		operatorController:   oc,
		replicationDB:        db,
		nodeManager:          nodeManager,
		checkBalanceInterval: balanceInterval,
		lastRebalanceTime:    time.Now(),
	}
}

func (s *balanceScheduler) Execute() time.Time {
	// balances the spans by size
	if !s.forceBalance && time.Since(s.lastRebalanceTime) < s.checkBalanceInterval {
		return s.lastRebalanceTime.Add(s.checkBalanceInterval)
	}
	now := time.Now()
	if s.operatorController.OperatorSize() > 0 || s.replicationDB.GetAbsentSize() > 0 {
		// not in stable schedule state, skip balance
		return now.Add(s.checkBalanceInterval)
	}

	// fast path, check the balance status
	moveSize := scheduler.CheckBalanceStatus(s.replicationDB.GetTaskSizePerNode(), s.nodeManager.GetAliveNodes())
	if moveSize <= 0 {
		// no need to do the balance, skip
		return now.Add(s.checkBalanceInterval)
	}

	groupBalanced, batch := true, s.batchSize
	for _, group := range s.replicationDB.GetGroups() {
		moved := s.schedulerGroup(group, batch)
		if moved != 0 {
			groupBalanced = false
		}
		batch -= moved
		if batch <= 0 {
			break
		}
	}
	if groupBalanced {
		s.schedulerGlobal()
	}
	s.lastRebalanceTime = now
	return now.Add(s.checkBalanceInterval)
}

func (s *balanceScheduler) schedulerGroup(id replica.GroupID, batch int) (consumed int) {
	consumed = scheduler.Balance(batch, s.random, s.nodeManager.GetAliveNodes(),
		s.replicationDB.GetReplicatingByGroup(id), func(replication *replica.SpanReplication, id node.ID) bool {
			op := operator.NewMoveDispatcherOperator(s.replicationDB, replication, replication.GetNodeID(), id)
			return s.operatorController.AddOperator(op)
		})
	return
}

func (s *balanceScheduler) schedulerGlobal() {
	// implement it
}

func (s *balanceScheduler) Name() string {
	return BalanceScheduler
}
