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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/operator"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/heap"
	"go.uber.org/zap"
)

// balanceScheduler is used to check the balance status of all spans among all nodes
type balanceScheduler[T replica.ReplicationID, S replica.ReplicationStatus, R replica.Replication[T]] struct {
	id        string
	batchSize int

	operatorController operator.Controller[T, S]
	db                 replica.ScheduleGroup[T, R]
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

	newMoveOperator func(r R, source, target node.ID) operator.Operator[T, S]
}

func NewBalanceScheduler[T replica.ReplicationID, S replica.ReplicationStatus, R replica.Replication[T]](
	id string, batchSize int,
	oc operator.Controller[T, S], db replica.ScheduleGroup[T, R],
	nodeManager *watcher.NodeManager, balanceInterval time.Duration,
	newMoveOperator func(R, node.ID, node.ID) operator.Operator[T, S],
) *balanceScheduler[T, S, R] {
	return &balanceScheduler[T, S, R]{
		id:                   id,
		batchSize:            batchSize,
		random:               rand.New(rand.NewSource(time.Now().UnixNano())),
		operatorController:   oc,
		db:                   db,
		nodeManager:          nodeManager,
		checkBalanceInterval: balanceInterval,
		lastRebalanceTime:    time.Now(),
		newMoveOperator:      newMoveOperator,
	}
}

func (s *balanceScheduler[T, S, R]) Execute() time.Time {
	if !s.forceBalance && time.Since(s.lastRebalanceTime) < s.checkBalanceInterval {
		return s.lastRebalanceTime.Add(s.checkBalanceInterval)
	}
	now := time.Now()
	if s.operatorController.OperatorSize() > 0 || s.db.GetAbsentSize() > 0 {
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

func (s *balanceScheduler[T, S, R]) schedulerGroup(nodes map[node.ID]*node.Info) int {
	availableSize, totalMoved := s.batchSize, 0
	for _, group := range s.db.GetGroups() {
		// fast path, check the balance status
		moveSize := CheckBalanceStatus(s.db.GetTaskSizePerNodeByGroup(group), nodes)
		if moveSize <= 0 {
			// no need to do the balance, skip
			continue
		}
		replicas := s.db.GetReplicatingByGroup(group)
		moveSize = Balance(availableSize, s.random, nodes, replicas, s.doMove)
		totalMoved += moveSize
		if totalMoved >= s.batchSize {
			break
		}
		availableSize -= moveSize
	}
	return totalMoved
}

// TODO: refactor and simplify the implementation and limit max group size
func (s *balanceScheduler[T, S, R]) schedulerGlobal(nodes map[node.ID]*node.Info) int {
	var zero R
	// fast path, check the balance status
	moveSize := CheckBalanceStatus(s.db.GetTaskSizePerNode(), nodes)
	if moveSize <= 0 {
		// no need to do the balance, skip
		return 0
	}
	groupNodetasks, valid := s.db.GetImbalanceGroupNodeTask(nodes)
	if !valid {
		// no need to do the balance, skip
		return 0
	}

	// complexity note: len(nodes) * len(groups)
	totalTasks := 0
	sizePerNode := make(map[node.ID]int, len(nodes))
	for _, nodeTasks := range groupNodetasks {
		for id, task := range nodeTasks {
			if task != zero {
				totalTasks++
				sizePerNode[id]++
			}
		}
	}
	lowerLimitPerNode := int(math.Floor(float64(totalTasks) / float64(len(nodes))))

	// fast path check again
	moveSize = CheckBalanceStatus(sizePerNode, nodes)
	if moveSize <= 0 {
		// no need to do the balance, skip
		return 0
	}

	moved := 0
	for _, nodeTasks := range groupNodetasks {
		availableNodes, victims, nextVictim := []node.ID{}, []node.ID{}, 0
		for id, task := range nodeTasks {
			if task != zero && sizePerNode[id] > lowerLimitPerNode {
				victims = append(victims, id)
			} else if task == zero && sizePerNode[id] < lowerLimitPerNode {
				// Notice: do not handle equal case (sizePerNode[id] == lowerLimitPerNode).
				// Otherwise, it will trigger unnecessary and infinite global balance.
				// For example,
				// 			Node A: 1, Node B: 1, Node C: 0, lowerLimitPerNode = 1
				// If Node C is recorded as a available node, a meaningless global balance will be triggered.
				availableNodes = append(availableNodes, id)
			}
		}

		for _, target := range availableNodes {
			if nextVictim >= len(victims) {
				break
			}
			victim := victims[nextVictim]
			if s.doMove(nodeTasks[victim], target) {
				sizePerNode[victim]--
				sizePerNode[target]++
				nextVictim++
				moved++
			}
		}
	}
	log.Info("scheduler: finish global balance", zap.String("id", s.id), zap.Int("moved", moved))
	return moved
}

func (s *balanceScheduler[T, S, R]) doMove(replication R, id node.ID) bool {
	op := s.newMoveOperator(replication, replication.GetNodeID(), id)
	return s.operatorController.AddOperator(op)
}

func (s *balanceScheduler[T, S, R]) Name() string {
	return BalanceScheduler
}

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

// Balance balances the running task by task size per node
func Balance[T replica.ReplicationID, R replica.Replication[T]](
	// id string,
	batchSize int, random *rand.Rand,
	activeNodes map[node.ID]*node.Info,
	replicating []R, move func(R, node.ID) bool,
) (movedSize int) {
	nodeTasks := make(map[node.ID][]R)
	for _, task := range replicating {
		nodeID := task.GetNodeID()
		if _, ok := nodeTasks[nodeID]; !ok {
			nodeTasks[nodeID] = make([]R, 0)
		}
		nodeTasks[nodeID] = append(nodeTasks[nodeID], task)
	}

	absentNodeCnt := 0
	// add the absent node to the node size map
	for nodeID := range activeNodes {
		if _, ok := nodeTasks[nodeID]; !ok {
			nodeTasks[nodeID] = make([]R, 0)
			absentNodeCnt++
		}
	}

	totalSize := len(replicating)
	lowerLimitPerCapture := int(math.Floor(float64(totalSize) / float64(len(nodeTasks))))
	minPriorityQueue := priorityQueue[T, R]{
		h:    heap.NewHeap[*item[T, R]](),
		less: func(a, b int) bool { return a < b },
		rand: random,
	}
	maxPriorityQueue := priorityQueue[T, R]{
		h:    heap.NewHeap[*item[T, R]](),
		less: func(a, b int) bool { return a > b },
		rand: random,
	}
	totalMoveSize := 0
	for nodeID, tasks := range nodeTasks {
		tableNum2Add := lowerLimitPerCapture - len(tasks)
		if tableNum2Add <= 0 {
			// Complexity note: Shuffle has O(n), where `n` is the number of tables.
			// Also, during a single call of `Schedule`, Shuffle can be called at most
			// `c` times, where `c` is the number of captures (RiCDC nodes).
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
		if target.Load >= lowerLimitPerCapture {
			// the minimum workload has reached the lower limit
			break
		}
		victim, _ := maxPriorityQueue.PeekTop()
		task := victim.Tasks[0]
		if move(task, target.Node) {
			// update the task size priority queue
			target.Load++
			victim.Load--
			victim.Tasks = victim.Tasks[1:]
			movedSize++
			if movedSize >= batchSize || movedSize >= totalMoveSize {
				break
			}
		}

		minPriorityQueue.AddOrUpdate(target)
		maxPriorityQueue.AddOrUpdate(victim)
	}

	log.Info("scheduler: balance done",
		// zap.String("id", id),
		zap.Int("movedSize", movedSize),
		zap.Int("victims", totalMoveSize))
	return movedSize
}
