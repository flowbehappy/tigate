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

	"github.com/flowbehappy/tigate/coordinator/changefeed"
	"github.com/flowbehappy/tigate/coordinator/operator"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/flowbehappy/tigate/utils/heap"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// Scheduler generates operators for the maintainers, and push them to the operator controller
// it generates add operator for the absent maintainers, and move operator for the unbalanced replicating maintainer
// currently, it only supports balance the maintainers by size
type Scheduler struct {
	batchSize            int
	random               *rand.Rand
	lastRebalanceTime    time.Time
	checkBalanceInterval time.Duration
	operatorController   *operator.Controller
	changefeedDB         *changefeed.ChangefeedDB
	nodeManager          *watcher.NodeManager

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
		absent, nodeSize := s.changefeedDB.GetScheduleSate(s.absent, availableSize)
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

// basicSchedule schedule the absent maintainers to the nodes base on the task size of each node
func (s *Scheduler) basicSchedule(
	availableSize int,
	absent []*changefeed.Changefeed,
	nodeTasks map[node.ID]int) {
	if len(nodeTasks) == 0 {
		log.Warn("no node available, skip")
		return
	}
	priorityQueue := heap.NewHeap[*Item]()
	for key, size := range nodeTasks {
		priorityQueue.AddOrUpdate(&Item{
			Node: key,
			Load: size,
		})
	}

	taskSize := 0
	for _, cf := range absent {
		item, _ := priorityQueue.PeekTop()
		// the operator is pushed successfully
		if s.operatorController.AddOperator(operator.NewAddMaintainerOperator(s.changefeedDB, cf, item.Node)) {
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

// balance balances the maintainers by size
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
	moveSize := checkBalanceStatus(s.changefeedDB.GetTaskSizePerNode(), s.nodeManager.GetAliveNodes())
	if moveSize <= 0 {
		// fast check the balance status, no need to do the balance,skip
		return
	}
	s.balanceTables()
	s.lastRebalanceTime = now
}

func (s *Scheduler) balanceTables() {
	replicating := s.changefeedDB.GetReplicating()
	nodeTasks := make(map[node.ID]map[model.ChangeFeedID]*changefeed.Changefeed)
	for _, cf := range replicating {
		nodeID := cf.GetNodeID()
		if _, ok := nodeTasks[nodeID]; !ok {
			nodeTasks[nodeID] = make(map[model.ChangeFeedID]*changefeed.Changefeed)
		}
		nodeTasks[nodeID][cf.ID] = cf
	}
	// add the absent node to the node size map
	for nodeID, _ := range s.nodeManager.GetAliveNodes() {
		if _, ok := nodeTasks[nodeID]; !ok {
			nodeTasks[nodeID] = make(map[model.ChangeFeedID]*changefeed.Changefeed)
		}
	}

	totalSize := 0
	for _, ts := range nodeTasks {
		totalSize += len(ts)
	}

	upperLimitPerCapture := int(math.Ceil(float64(totalSize) / float64(len(nodeTasks))))
	// victims holds tables which need to be moved
	victims := make([]*changefeed.Changefeed, 0)
	priorityQueue := heap.NewHeap[*Item]()
	for nodeID, ts := range nodeTasks {
		var stms []*changefeed.Changefeed
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
			priorityQueue.AddOrUpdate(&Item{
				Node: nodeID,
				Load: len(ts),
			})
			continue
		} else {
			priorityQueue.AddOrUpdate(&Item{
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
		if s.operatorController.AddOperator(operator.NewMoveMaintainerOperator(s.changefeedDB, cf, cf.GetNodeID(), item.Node)) {
			// update the task size priority queue
			item.Load++
			movedSize++
		}
		priorityQueue.AddOrUpdate(item)
	}
	log.Info("balance done",
		zap.Int("movedSize", movedSize),
		zap.Int("victims", len(victims)))
}

// checkBalanceStatus checks the maintainer scheduling balance status
// returns the table size need to be moved
func checkBalanceStatus(nodeTaskSize map[node.ID]int,
	allNodes map[node.ID]*node.Info) int {
	// add the absent node to the node size map
	for nodeID, _ := range allNodes {
		if _, ok := nodeTaskSize[nodeID]; !ok {
			nodeTaskSize[nodeID] = 0
		}
	}
	totalSize := 0
	for _, ts := range nodeTaskSize {
		totalSize += ts
	}
	upperLimitPerCapture := int(math.Ceil(float64(totalSize) / float64(len(nodeTaskSize))))
	// tables need to be moved
	moveSize := 0
	for _, ts := range nodeTaskSize {
		tableNum2Remove := ts - upperLimitPerCapture
		if tableNum2Remove > 0 {
			moveSize += tableNum2Remove
		}
	}
	return moveSize
}
