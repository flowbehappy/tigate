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
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/server/watcher"
)

// Scheduler generates operators for the spans, and push them to the operator controller
// it generates add operator for the absent spans, and move operator for the unbalanced replicating spans
// currently, it only supports balance the spans by size
type Scheduler struct {
	batchSize            int
	changefeedID         string
	random               *rand.Rand
	lastRebalanceTime    time.Time
	checkBalanceInterval time.Duration
	operatorController   *operator.Controller
	replicationDB        *replica.ReplicationDB
	nodeManager          *watcher.NodeManager

	// buffer for the absent spans
	absent []*replica.SpanReplication
}

func NewScheduler(changefeedID string,
	batchSize int,
	oc *operator.Controller,
	db *replica.ReplicationDB,
	nodeManager *watcher.NodeManager,
	balanceInterval time.Duration) *Scheduler {
	return &Scheduler{
		batchSize:            batchSize,
		random:               rand.New(rand.NewSource(time.Now().UnixNano())),
		changefeedID:         changefeedID,
		checkBalanceInterval: balanceInterval,
		operatorController:   oc,
		replicationDB:        db,
		nodeManager:          nodeManager,
		lastRebalanceTime:    time.Now(),
		absent:               make([]*replica.SpanReplication, 0, batchSize),
	}
}

// Execute periodically execute the operator
func (s *Scheduler) Execute() time.Time {
	if s.replicationDB.GetAbsentSize() > 0 {
		availableSize := s.batchSize - s.operatorController.OperatorSize()
		if availableSize <= 0 {
			return time.Now().Add(time.Millisecond * 500)
		}
		// too many running operators, skip
		if availableSize < s.batchSize/2 {
			return time.Now().Add(time.Millisecond * 100)
		}
		absent := s.replicationDB.GetAbsent(s.absent, availableSize)
		nodeSize := s.replicationDB.GetTaskSizePerNode()
		// add the absent node to the node size map
		// todo: use the bootstrap nodes
		for id, _ := range s.nodeManager.GetAliveNodes() {
			if _, ok := nodeSize[id]; !ok {
				nodeSize[id] = 0
			}
		}
		scheduler.BasicSchedule(availableSize, absent, nodeSize, func(replication *replica.SpanReplication, id node.ID) bool {
			return s.operatorController.AddOperator(operator.NewAddDispatcherOperator(s.replicationDB, replication, id))
		})
		s.absent = absent[:0]
	} else {
		s.balance()
	}
	return time.Now().Add(time.Millisecond * 500)
}

// balance balances the spans by size
func (s *Scheduler) balance() {
	if time.Since(s.lastRebalanceTime) < s.checkBalanceInterval {
		return
	}
	if s.operatorController.OperatorSize() > 0 {
		// not in stable schedule state, skip balance
		return
	}
	now := time.Now()
	if now.Sub(s.lastRebalanceTime) < s.checkBalanceInterval {
		// skip balance.
		return
	}

	// check the balance status
	moveSize := scheduler.CheckBalanceStatus(s.replicationDB.GetTaskSizePerNode(), s.nodeManager.GetAliveNodes())
	if moveSize <= 0 {
		// fast check the balance status, no need to do the balance,skip
		return
	}
	scheduler.Balance(s.batchSize, s.random, s.nodeManager.GetAliveNodes(), s.replicationDB.GetReplicating(), func(replication *replica.SpanReplication, id node.ID) bool {
		return s.operatorController.AddOperator(operator.NewMoveDispatcherOperator(s.replicationDB, replication, replication.GetNodeID(), id))
	})
	s.lastRebalanceTime = now
}
