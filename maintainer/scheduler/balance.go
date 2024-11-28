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
	"math"
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

	nodes := s.nodeManager.GetAliveNodes()
	moved := s.schedulerGroup(nodes)
	if moved == 0 {
		// all groups are balanced, safe to do the global balance
		moved = s.schedulerGlobal(nodes)
	}

	s.forceBalance = moved >= s.batchSize
	s.lastRebalanceTime = now
	return now.Add(s.checkBalanceInterval)
}

func (s *balanceScheduler) schedulerGroup(nodes map[node.ID]*node.Info) int {
	batch, moved := s.batchSize, 0
	for _, group := range s.replicationDB.GetGroups() {
		// fast path, check the balance status
		moveSize := scheduler.CheckBalanceStatus(s.replicationDB.GetTaskSizePerNodeByGroup(group), nodes)
		if moveSize <= 0 {
			// no need to do the balance, skip
			continue
		}
		replicas := s.replicationDB.GetReplicatingByGroup(group)
		moved += scheduler.Balance(batch, s.random, nodes, replicas, s.doMove)
		if moved >= batch {
			break
		}
	}
	return moved
}

// TODO: refactor and simplify the implementation and limit max group size
func (s *balanceScheduler) schedulerGlobal(nodes map[node.ID]*node.Info) int {
	// fast path, check the balance status
	moveSize := scheduler.CheckBalanceStatus(s.replicationDB.GetTaskSizePerNode(), nodes)
	if moveSize <= 0 {
		// no need to do the balance, skip
		return 0
	}
	groupNodetasks, valid := s.replicationDB.GetImbalanceGroupNodeTask(len(nodes))
	if !valid {
		// no need to do the balance, skip
		return 0
	}

	// complexity note: len(nodes) * len(groups)
	totalTasks := 0
	sizePerNode := make(map[node.ID]int, len(nodes))
	for _, nodeTasks := range groupNodetasks {
		for id, task := range nodeTasks {
			if task != nil {
				totalTasks++
				sizePerNode[id]++
			}
		}
	}
	upperLimitPerNode := int(math.Ceil(float64(totalTasks) / float64(len(nodes))))
	limitCnt := 0
	for _, size := range sizePerNode {
		if size == upperLimitPerNode {
			limitCnt++
		}
	}
	if limitCnt == len(nodes) {
		// all nodes are global balanced
		return 0
	}

	moved := 0
	for _, nodeTasks := range groupNodetasks {
		victims, availableNodes, next := map[node.ID]*replica.SpanReplication{}, []node.ID{}, 0
		for id, task := range nodeTasks {
			if task != nil && sizePerNode[id] > upperLimitPerNode {
				victims[id] = task
			} else if task == nil && sizePerNode[id] < upperLimitPerNode {
				availableNodes = append(availableNodes, id)
			}
		}

		for old, victim := range victims {
			if next >= len(availableNodes) {
				break
			}
			new := availableNodes[next]
			if s.doMove(victim, new) {
				sizePerNode[old]--
				sizePerNode[new]++
				next++
				moved++
			}
		}
	}
	return moved
}

func (s *balanceScheduler) doMove(replication *replica.SpanReplication, id node.ID) bool {
	op := operator.NewMoveDispatcherOperator(s.replicationDB, replication, replication.GetNodeID(), id)
	return s.operatorController.AddOperator(op)
}

func (s *balanceScheduler) Name() string {
	return BalanceScheduler
}
