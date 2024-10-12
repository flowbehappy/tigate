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

package maintainer

import (
	"math"
	"math/rand"
	"time"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/flowbehappy/tigate/utils/heap"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Scheduler struct {
	batchSize            int
	changefeedID         string
	random               *rand.Rand
	lastRebalanceTime    time.Time
	checkBalanceInterval time.Duration
	oc                   *OperatorController
	db                   *ReplicaSetDB
	nodeManager          *watcher.NodeManager
}

func NewScheduler(batchSize int, changefeedID string,
	oc *OperatorController,
	db *ReplicaSetDB,
	nodeManager *watcher.NodeManager,
	balanceInterval time.Duration) *Scheduler {
	return &Scheduler{
		batchSize:            batchSize,
		random:               rand.New(rand.NewSource(time.Now().UnixNano())),
		changefeedID:         changefeedID,
		checkBalanceInterval: balanceInterval,
		oc:                   oc,
		db:                   db,
		nodeManager:          nodeManager,
		lastRebalanceTime:    time.Now(),
	}
}

// Execute periodically execute the operator
func (s *Scheduler) Execute() time.Time {
	if s.db.GetAbsentSize() > 0 {
		absent, nodeSize := s.db.GetScheduleSate()
		// add the absent node to the node size map
		for id, _ := range s.nodeManager.GetAliveNodes() {
			if _, ok := nodeSize[id]; !ok {
				nodeSize[id] = 0
			}
		}
		s.basicSchedule(absent, nodeSize)
	} else {
		s.Balance()
	}
	return time.Now().Add(time.Millisecond * 500)
}

func (s *Scheduler) basicSchedule(
	absent []*ReplicaSet,
	nodeTasks map[node.ID]int) {
	priorityQueue := heap.NewHeap[*Item]()
	for key, size := range nodeTasks {
		priorityQueue.AddOrUpdate(&Item{
			Node:     key,
			TaskSize: size,
		})
	}

	taskSize := 0
	for _, replicaSet := range absent {
		item, _ := priorityQueue.PeekTop()
		// the operator is pushed successfully
		if s.oc.AddOperator(NewAddDispatcherOperator(replicaSet, item.Node)) {
			// update the task size priority queue
			item.TaskSize++
			taskSize++
			s.db.BindReplicaSetToNode("", item.Node, replicaSet)
		}
		priorityQueue.AddOrUpdate(item)
	}
}

func (s *Scheduler) Balance() {
	if time.Since(s.lastRebalanceTime) < s.checkBalanceInterval {
		return
	}
	if s.oc.OperatorSize() > 0 {
		// not in stable schedule state, skip balance
		return
	}
	now := time.Now()
	if now.Sub(s.lastRebalanceTime) < s.checkBalanceInterval {
		// skip balance.
		return
	}
	s.lastRebalanceTime = now
	s.balanceTables()
}

func (s *Scheduler) balanceTables() {
	workings := s.db.GetWorking()
	nodeTasks := make(map[node.ID]map[common.DispatcherID]*ReplicaSet)
	for _, cf := range workings {
		nodeID := cf.GetNodeID()
		if _, ok := nodeTasks[nodeID]; !ok {
			nodeTasks[nodeID] = make(map[common.DispatcherID]*ReplicaSet)
		}
		nodeTasks[nodeID][cf.ID] = cf
	}

	totalSize := 0
	for _, ts := range nodeTasks {
		totalSize += len(ts)
	}

	upperLimitPerCapture := int(math.Ceil(float64(totalSize) / float64(len(nodeTasks))))
	// victims holds tables which need to be moved
	victims := make([]*ReplicaSet, 0)
	priorityQueue := heap.NewHeap[*Item]()
	for nodeID, ts := range nodeTasks {
		var stms []*ReplicaSet
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
				Node:     nodeID,
				TaskSize: len(ts),
			})
			continue
		} else {
			priorityQueue.AddOrUpdate(&Item{
				Node:     nodeID,
				TaskSize: len(ts) - tableNum2Remove,
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
		if s.oc.AddOperator(NewMoveDispatcherOperator(cf, item.Node)) {
			// update the task size priority queue
			item.TaskSize++
			movedSize++
		}
		priorityQueue.AddOrUpdate(item)
	}
	log.Info("balance done",
		zap.String("changefeed", s.changefeedID),
		zap.Int("movedSize", movedSize),
		zap.Int("victims", len(victims)))
}

type Item struct {
	Node     node.ID
	TaskSize int
	index    int
}

func (i *Item) SetHeapIndex(idx int) {
	i.index = idx
}

func (i *Item) GetHeapIndex() int {
	return i.index
}

func (i *Item) CompareTo(t *Item) int {
	return i.TaskSize - t.TaskSize
}
