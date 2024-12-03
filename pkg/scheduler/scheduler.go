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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/heap"
	"go.uber.org/zap"
)

// CheckBalanceStatus checks the dispatcher scheduling balance status
// returns the table size need to be moved
func CheckBalanceStatus(nodeTaskSize map[node.ID]int, allNodes map[node.ID]*node.Info) int {
	// add the absent node to the node size map
	for nodeID := range allNodes {
		if _, ok := nodeTaskSize[nodeID]; !ok {
			nodeTaskSize[nodeID] = 0
		}
	}
	totalSize := 0
	for _, ts := range nodeTaskSize {
		totalSize += ts
	}
	lowerLimitPerCapture := int(math.Floor(float64(totalSize) / float64(len(nodeTaskSize))))
	// tables need to be moved
	moveSize := 0
	for _, ts := range nodeTaskSize {
		tableNum2Add := lowerLimitPerCapture - ts
		if tableNum2Add > 0 {
			moveSize += tableNum2Add
		}
	}
	return moveSize
}

// Replication is the interface for the replication task, it should implement the GetNodeID method
type Replication interface {
	GetNodeID() node.ID
}

// Balance balances the running task by task size per node
func Balance[T Replication](
	batchSize int, random *rand.Rand,
	activeNodes map[node.ID]*node.Info,
	replicating []T, move func(T, node.ID) bool,
) (movedSize int) {
	nodeTasks := make(map[node.ID][]T)
	for _, cf := range replicating {
		nodeID := cf.GetNodeID()
		if _, ok := nodeTasks[nodeID]; !ok {
			nodeTasks[nodeID] = make([]T, 0)
		}
		nodeTasks[nodeID] = append(nodeTasks[nodeID], cf)
	}

	absentNodeCnt := 0
	// add the absent node to the node size map
	for nodeID := range activeNodes {
		if _, ok := nodeTasks[nodeID]; !ok {
			nodeTasks[nodeID] = make([]T, 0)
			absentNodeCnt++
		}
	}

	totalSize := len(replicating)
	lowerLimitPerCapture := int(math.Floor(float64(totalSize) / float64(len(nodeTasks))))
	minPriorityQueue := priorityQueue[T]{
		h:    heap.NewHeap[*Item[T]](),
		less: func(a, b int) bool { return a < b },
		rand: random,
	}
	maxPriorityQueue := priorityQueue[T]{
		h:    heap.NewHeap[*Item[T]](),
		less: func(a, b int) bool { return a > b },
		rand: random,
	}
	totalMoveSize := 0
	for nodeID, tasks := range nodeTasks {
		tableNum2Add := lowerLimitPerCapture - len(tasks)
		if tableNum2Add <= 0 {
			// Complexity note: Shuffle has O(n), where `n` is the number of tables.
			// Also, during a single call of `Schedule`, Shuffle can be called at most
			// `c` times, where `c` is the number of captures (TiCDC nodes).
			// Only called when a rebalance is triggered, which happens rarely,
			// we do not expect a performance degradation as a result of adding
			// the randomness.
			random.Shuffle(len(tasks), func(i, j int) {
				tasks[i], tasks[j] = tasks[j], tasks[i]
			})
			maxPriorityQueue.InitItem(nodeID, len(tasks), tasks)
			continue
		} else {
			minPriorityQueue.InitItem(nodeID, len(tasks), nil)
			totalMoveSize += tableNum2Add
		}
	}
	if totalMoveSize == 0 {
		return 0
	}

	movedSize = 0
	for {
		target, _ := minPriorityQueue.PeekTop()
		if len(target.tasks) >= lowerLimitPerCapture {
			// the minimum workload has reached the lower limit
			break
		}
		victim, _ := maxPriorityQueue.PeekTop()
		task := victim.tasks[0]
		if move(task, target.Node) {
			// update the task size priority queue
			target.load++
			victim.load--
			victim.tasks = victim.tasks[1:]
			movedSize++
			if movedSize >= batchSize {
				break
			}
		}

		minPriorityQueue.AddOrUpdate(target)
		maxPriorityQueue.AddOrUpdate(victim)
	}

	log.Info("balance done",
		zap.Int("movedSize", movedSize),
		zap.Int("victims", totalMoveSize))
	return movedSize
}

// BasicSchedule schedules the absent tasks to the available nodes
func BasicSchedule[T Replication](
	availableSize int,
	absent []T,
	nodeTasks map[node.ID]int,
	schedule func(T, node.ID) bool) {
	if len(nodeTasks) == 0 {
		log.Warn("no node available, skip")
		return
	}
	minPriorityQueue := priorityQueue[T]{
		h:    heap.NewHeap[*Item[T]](),
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
			item.load++
			taskSize++
		}
		if taskSize >= availableSize {
			break
		}
		minPriorityQueue.AddOrUpdate(item)
	}
}
