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

	groupReplicatings, moved := s.schedulerGroup()
	if moved == 0 {
		// all groups are balanced, safe to do the global balance
		moved = s.schedulerGlobal(groupReplicatings)
	}

	s.forceBalance = moved >= s.batchSize
	s.lastRebalanceTime = now
	return now.Add(s.checkBalanceInterval)
}

func (s *balanceScheduler) schedulerGroup() (map[replica.GroupID][]*replica.SpanReplication, int) {
	batch, moved := s.batchSize, 0
	groupReplicatings := make(map[replica.GroupID][]*replica.SpanReplication)
	for _, group := range s.replicationDB.GetGroups() {
		replicas := s.replicationDB.GetReplicatingByGroup(group)
		moved += scheduler.Balance(
			batch, s.random, s.nodeManager.GetAliveNodes(), replicas,
			func(replication *replica.SpanReplication, id node.ID) bool {
				op := operator.NewMoveDispatcherOperator(s.replicationDB, replication, replication.GetNodeID(), id)
				return s.operatorController.AddOperator(op)
			},
		)
		if len(replicas) > 0 {
			groupReplicatings[group] = replicas
		}
		if moved >= batch {
			break
		}
	}
	return groupReplicatings, moved
}

func (s *balanceScheduler) schedulerGlobal(groupReplicatings map[replica.GroupID][]*replica.SpanReplication) int {
	// implement it
	activeNodes := s.nodeManager.GetAliveNodes()

	groupNodesTask := make(map[replica.GroupID]map[node.ID]int)
	for group, replicas := range groupReplicatings {
		nodeTasks := make(map[node.ID]*replica.SpanReplication)
		for _, r := range replicas {
			nodeID := r.GetNodeID()
			if _, ok := nodeTasks[nodeID]; !ok {
				nodeTasks[nodeID] = r
			}
		}

	}

	return 0
}

func (s *balanceScheduler) Name() string {
	return BalanceScheduler
}
