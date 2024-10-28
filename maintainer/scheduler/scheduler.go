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

	"github.com/flowbehappy/tigate/maintainer/operator"
	"github.com/flowbehappy/tigate/maintainer/replica"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/pkg/scheduler"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/flowbehappy/tigate/utils/heap"
	"github.com/pingcap/log"
	"go.uber.org/zap"
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
		s.basicSchedule(availableSize, absent, nodeSize)
		s.absent = absent[:0]
	} else {
		s.balance()
	}
	return time.Now().Add(time.Millisecond * 500)
}

// basicSchedule schedule the absent spans to the nodes base on the task size of each node
func (s *Scheduler) basicSchedule(
	availableSize int,
	absent []*replica.SpanReplication,
	nodeTasks map[node.ID]int) {
	if len(nodeTasks) == 0 {
		log.Warn("no node available, skip", zap.String("changefeed", s.changefeedID))
		return
	}
	priorityQueue := heap.NewHeap[*scheduler.Item]()
	for key, size := range nodeTasks {
		priorityQueue.AddOrUpdate(&scheduler.Item{
			Node: key,
			Load: size,
		})
	}

	taskSize := 0
	for _, replicaSet := range absent {
		item, _ := priorityQueue.PeekTop()
		// the operator is pushed successfully
		if s.operatorController.AddOperator(operator.NewAddDispatcherOperator(s.replicationDB, replicaSet, item.Node)) {
			// update the task size priority queue
			item.Load++
			taskSize++
		}
		if taskSize >= availableSize {
			break
		}
		priorityQueue.AddOrUpdate(item)
	}
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
	s.balanceTables()
	s.lastRebalanceTime = now
}

func (s *Scheduler) balanceTables() {
	replicating := s.replicationDB.GetReplicating()
	nodeTasks := make(map[node.ID]map[common.DispatcherID]*replica.SpanReplication)
	for _, cf := range replicating {
		nodeID := cf.GetNodeID()
		if _, ok := nodeTasks[nodeID]; !ok {
			nodeTasks[nodeID] = make(map[common.DispatcherID]*replica.SpanReplication)
		}
		nodeTasks[nodeID][cf.ID] = cf
	}
	// add the absent node to the node size map
	for nodeID, _ := range s.nodeManager.GetAliveNodes() {
		if _, ok := nodeTasks[nodeID]; !ok {
			nodeTasks[nodeID] = make(map[common.DispatcherID]*replica.SpanReplication)
		}
	}

	totalSize := 0
	for _, ts := range nodeTasks {
		totalSize += len(ts)
	}

	upperLimitPerCapture := int(math.Ceil(float64(totalSize) / float64(len(nodeTasks))))
	// victims holds tables which need to be moved
	victims := make([]*replica.SpanReplication, 0)
	priorityQueue := heap.NewHeap[*scheduler.Item]()
	for nodeID, ts := range nodeTasks {
		var stms []*replica.SpanReplication
		for _, value := range ts {
			stms = append(stms, value)
		}

		// Complexity note: Shuffle has O(n), where `n` is the number of tables.
		// Also, during a single call of `Schedule`, Shuffle can be called at most
		// `c` times, where `c` is the number of captures (TiCDC nodes).
		// Only called when a rebalance is triggered, which happens rarely,
		// we do not expect a performance degradation as a result of adding
		// the randomness.
		s.random.Shuffle(len(stms), func(i, j int) {
			stms[i], stms[j] = stms[j], stms[i]
		})

		tableNum2Remove := len(stms) - upperLimitPerCapture
		if tableNum2Remove <= 0 {
			priorityQueue.AddOrUpdate(&scheduler.Item{
				Node: nodeID,
				Load: len(ts),
			})
			continue
		} else {
			priorityQueue.AddOrUpdate(&scheduler.Item{
				Node: nodeID,
				Load: len(ts) - tableNum2Remove,
			})
		}

		for _, cf := range stms {
			if tableNum2Remove <= 0 {
				break
			}
			victims = append(victims, cf)
			tableNum2Remove--
		}
	}
	if len(victims) == 0 {
		return
	}

	movedSize := 0
	// for each victim table, find the target for it
	for idx, cf := range victims {
		if idx >= s.batchSize {
			// We have reached the task limit.
			break
		}

		item, _ := priorityQueue.PeekTop()

		// the operator is pushed successfully
		if s.operatorController.AddOperator(operator.NewMoveDispatcherOperator(s.replicationDB, cf, cf.GetNodeID(), item.Node)) {
			// update the task size priority queue
			item.Load++
			movedSize++
		}
		priorityQueue.AddOrUpdate(item)
	}
	log.Info("balance done",
		zap.String("changefeed", s.changefeedID),
		zap.Int("movedSize", movedSize),
		zap.Int("victims", len(victims)))
}
