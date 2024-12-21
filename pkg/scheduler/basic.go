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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/operator"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/heap"
)

// basicScheduler generates operators for the spans, and push them to the operator controller
// it generates add operator for the absent spans, and move operator for the unbalanced replicating spans
// currently, it only supports balance the spans by size
type basicScheduler[T replica.ReplicationID, S replica.ReplicationStatus, R replica.Replication[T]] struct {
	id        string
	batchSize int

	operatorController operator.Controller[T, S]
	db                 replica.ScheduleGroup[T, R]
	nodeManager        *watcher.NodeManager

	absent         []R                                               // buffer for the absent spans
	newAddOperator func(r R, target node.ID) operator.Operator[T, S] // scheduler r to target node
}

func NewBasicScheduler[T replica.ReplicationID, S replica.ReplicationStatus, R replica.Replication[T]](
	id string, batchSize int,
	oc operator.Controller[T, S], db replica.ScheduleGroup[T, R],
	nodeManager *watcher.NodeManager,
	newAddOperator func(R, node.ID) operator.Operator[T, S],
) *basicScheduler[T, S, R] {
	return &basicScheduler[T, S, R]{
		id:                 id,
		batchSize:          batchSize,
		operatorController: oc,
		db:                 db,
		nodeManager:        nodeManager,
		absent:             make([]R, 0, batchSize),
		newAddOperator:     newAddOperator,
	}
}

// Execute periodically execute the operator
func (s *basicScheduler[T, S, R]) Execute() time.Time {
	availableSize := s.batchSize - s.operatorController.OperatorSize()
	if s.db.GetAbsentSize() <= 0 || availableSize <= 0 {
		// can not schedule more operators, skip
		return time.Now().Add(time.Millisecond * 500)
	}
	if availableSize < s.batchSize/2 {
		// too many running operators, skip
		return time.Now().Add(time.Millisecond * 100)
	}

	for _, id := range s.db.GetGroups() {
		availableSize -= s.schedule(id, availableSize)
		if availableSize <= 0 {
			break
		}
	}
	return time.Now().Add(time.Millisecond * 500)
}

func (s *basicScheduler[T, S, R]) schedule(id replica.GroupID, availableSize int) (scheduled int) {
	absent := s.db.GetAbsentByGroup(id, availableSize)
	nodeSize := s.db.GetTaskSizePerNodeByGroup(id)
	// add the absent node to the node size map
	for id := range s.nodeManager.GetAliveNodes() {
		if _, ok := nodeSize[id]; !ok {
			nodeSize[id] = 0
		}
	}
	// what happens if the some node removed when scheduling?
	BasicSchedule(availableSize, absent, nodeSize, func(replication R, id node.ID) bool {
		op := s.newAddOperator(replication, id)
		return s.operatorController.AddOperator(op)
	})
	scheduled = len(absent)
	s.absent = absent[:0]
	return
}

func (s *basicScheduler[T, S, R]) Name() string {
	return BasicScheduler
}

// BasicSchedule schedules the absent tasks to the available nodes
func BasicSchedule[T replica.ReplicationID, R replica.Replication[T]](
	availableSize int,
	absent []R,
	nodeTasks map[node.ID]int,
	schedule func(R, node.ID) bool) {
	if len(nodeTasks) == 0 {
		log.Warn("scheduler: no node available, skip")
		return
	}
	minPriorityQueue := priorityQueue[T, R]{
		h:    heap.NewHeap[*item[T, R]](),
		less: func(a, b int) bool { return a < b },
	}
	for key, size := range nodeTasks {
		minPriorityQueue.InitItem(key, size, nil)
	}

	taskSize := 0
	for _, cf := range absent {
		item, _ := minPriorityQueue.PeekTop()
		// the operator is pushed successfully
		if schedule(cf, item.Node) {
			// update the task size priority queue
			item.Load++
			taskSize++
		}
		if taskSize >= availableSize {
			break
		}
		minPriorityQueue.AddOrUpdate(item)
	}
}
