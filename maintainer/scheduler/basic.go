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
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/server/watcher"
)

// basicScheduler generates operators for the spans, and push them to the operator controller
// it generates add operator for the absent spans, and move operator for the unbalanced replicating spans
// currently, it only supports balance the spans by size
type basicScheduler struct {
	changefeedID common.ChangeFeedID
	batchSize    int

	operatorController *operator.Controller
	replicationDB      *replica.ReplicationDB
	nodeManager        *watcher.NodeManager

	// buffer for the absent spans
	absent []*replica.SpanReplication
}

func newBasicScheduler(
	changefeedID common.ChangeFeedID, batchSize int,
	oc *operator.Controller, db *replica.ReplicationDB, nodeManager *watcher.NodeManager,
) *basicScheduler {
	return &basicScheduler{
		batchSize:          batchSize,
		changefeedID:       changefeedID,
		operatorController: oc,
		replicationDB:      db,
		nodeManager:        nodeManager,
		absent:             make([]*replica.SpanReplication, 0, batchSize),
	}
}

// Execute periodically execute the operator
func (s *basicScheduler) Execute() time.Time {
	availableSize := s.batchSize - s.operatorController.OperatorSize()
	if s.replicationDB.GetAbsentSize() <= 0 || availableSize <= 0 {
		// can not schedule more operators, skip
		return time.Now().Add(time.Millisecond * 500)
	}
	if availableSize < s.batchSize/2 {
		// too many running operators, skip
		return time.Now().Add(time.Millisecond * 100)
	}

	absent := s.replicationDB.GetAbsent(s.absent, availableSize)
	nodeSize := s.replicationDB.GetTaskSizePerNode()
	// add the absent node to the node size map
	// todo: use the bootstrap nodes
	for id := range s.nodeManager.GetAliveNodes() {
		if _, ok := nodeSize[id]; !ok {
			nodeSize[id] = 0
		}
	}
	scheduler.BasicSchedule(availableSize, absent, nodeSize, func(replication *replica.SpanReplication, id node.ID) bool {
		return s.operatorController.AddOperator(operator.NewAddDispatcherOperator(s.replicationDB, replication, id))
	})
	s.absent = absent[:0]
	return time.Now().Add(time.Millisecond * 500)
}

func (s *basicScheduler) Name() string {
	return BasicScheduler
}
