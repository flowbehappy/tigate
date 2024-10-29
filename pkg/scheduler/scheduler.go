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

	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/utils/heap"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// CheckBalanceStatus checks the dispatcher scheduling balance status
// returns the table size need to be moved
func CheckBalanceStatus(nodeTaskSize map[node.ID]int, allNodes map[node.ID]*node.Info) int {
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

// Replication is the interface for the replication task, it should implement the GetNodeID method
type Replication interface {
	GetNodeID() node.ID
}

// Balance balances the running task by task size per node
func Balance[T Replication](batchSize int,
	random *rand.Rand,
	activeNodes map[node.ID]*node.Info,
	replicating []T,
	move func(T, node.ID) bool) {
	nodeTasks := make(map[node.ID][]T)
	for _, cf := range replicating {
		nodeID := cf.GetNodeID()
		if _, ok := nodeTasks[nodeID]; !ok {
			nodeTasks[nodeID] = make([]T, 0)
		}
		nodeTasks[nodeID] = append(nodeTasks[nodeID], cf)
	}
	// add the absent node to the node size map
	for nodeID, _ := range activeNodes {
		if _, ok := nodeTasks[nodeID]; !ok {
			nodeTasks[nodeID] = make([]T, 0)
		}
	}

	totalSize := 0
	for _, ts := range nodeTasks {
		totalSize += len(ts)
	}

	upperLimitPerCapture := int(math.Ceil(float64(totalSize) / float64(len(nodeTasks))))
	// victims holds tasks which need to be moved
	victims := make([]T, 0)
	priorityQueue := heap.NewHeap[*Item]()
	for nodeID, ts := range nodeTasks {
		var stms []T
		for _, value := range ts {
			stms = append(stms, value)
		}

		// Complexity note: Shuffle has O(n), where `n` is the number of tables.
		// Also, during a single call of `Schedule`, Shuffle can be called at most
		// `c` times, where `c` is the number of captures (TiCDC nodes).
		// Only called when a rebalance is triggered, which happens rarely,
		// we do not expect a performance degradation as a result of adding
		// the randomness.
		random.Shuffle(len(stms), func(i, j int) {
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
		if idx >= batchSize {
			// We have reached the task limit.
			break
		}

		item, _ := priorityQueue.PeekTop()

		// the operator is pushed successfully
		if move(cf, item.Node) {
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

// BasicSchedule schedules the absent tasks to the available nodes
func BasicSchedule[T any](
	availableSize int,
	absent []T,
	nodeTasks map[node.ID]int,
	schedule func(T, node.ID) bool) {
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
		if schedule(cf, item.Node) {
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
