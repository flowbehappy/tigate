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

package replica

import (
	"math"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

type ReplicationID interface {
	comparable
	String() string
}

// Replication is the interface for the replication task, it should implement the GetNodeID method
type Replication[T ReplicationID] interface {
	comparable
	GetID() T
	GetGroupID() GroupID

	GetNodeID() node.ID
	SetNodeID(node.ID)

	ShouldRun() bool
}

// ScheduleGroup define the querying interface for scheduling information.
// Notice: all methods are thread-safe.
type ScheduleGroup[T ReplicationID, R Replication[T]] interface {
	GetAbsentSize() int
	GetAbsent() []R
	GetSchedulingSize() int
	GetScheduling() []R
	GetReplicatingSize() int
	GetReplicating() []R

	// group scheduler interface
	GetGroups() []GroupID
	GetAbsentByGroup(groupID GroupID, batch int) []R
	GetSchedulingByGroup(groupID GroupID) []R
	GetReplicatingByGroup(groupID GroupID) []R
	GetGroupStat() string

	// node scheduler interface
	GetTaskByNodeID(id node.ID) []R
	GetTaskSizeByNodeID(id node.ID) int
	GetTaskSizePerNode() map[node.ID]int
	GetImbalanceGroupNodeTask(nodes map[node.ID]*node.Info) (groups map[GroupID]map[node.ID]R, valid bool)
	GetTaskSizePerNodeByGroup(groupID GroupID) map[node.ID]int
}

type ReplicationDB[T ReplicationID, R Replication[T]] interface {
	ScheduleGroup[T, R]

	// The flowing methods are not thread-safe
	GetReplicatingWithoutLock() []R
	GetSchedulingWithoutLock() []R
	AddAbsentWithoutLock(task R)
	AddReplicatingWithoutLock(task R)

	MarkAbsentWithoutLock(task R)
	MarkSchedulingWithoutLock(task R)
	MarkReplicatingWithoutLock(task R)

	BindReplicaToNodeWithoutLock(old, new node.ID, task R)
	RemoveReplicaWithoutLock(task R)
}

func NewReplicationDB[T ReplicationID, R Replication[T]](
	id string, withRLock func(action func()),
) ReplicationDB[T, R] {
	r := &replicationDB[T, R]{
		id:         id,
		taskGroups: make(map[GroupID]*replicationGroup[T, R]),
		withRLock:  withRLock,
	}
	r.taskGroups[DefaultGroupID] = newReplicationGroup[T, R](id, DefaultGroupID)
	return r
}

type replicationDB[T ReplicationID, R Replication[T]] struct {
	id         string
	withRLock  func(action func())
	taskGroups map[GroupID]*replicationGroup[T, R]
}

func (db *replicationDB[T, R]) GetGroups() []GroupID {
	groups := make([]GroupID, 0, len(db.taskGroups))
	db.withRLock(func() {
		for id := range db.taskGroups {
			groups = append(groups, id)
		}
	})
	return groups
}

func (db *replicationDB[T, R]) GetAbsent() []R {
	var absent = make([]R, 0)
	db.withRLock(func() {
		for _, g := range db.taskGroups {
			absent = append(absent, g.GetAbsent()...)
		}
	})
	return absent
}

func (db *replicationDB[T, R]) GetAbsentSize() int {
	size := 0
	db.withRLock(func() {
		for _, g := range db.taskGroups {
			size += g.GetAbsentSize()
		}
	})
	return size
}

func (db *replicationDB[T, R]) GetAbsentByGroup(id GroupID, batch int) []R {
	buffer := make([]R, 0, batch)
	db.withRLock(func() {
		g := db.mustGetGroup(id)
		for _, stm := range g.GetAbsent() {
			// IsDropped is not implemented yet, it filter the dropped table and
			// prevent unexpected scheduling behavior
			// if !stm.IsDropped() {
			// 	buffer = append(buffer, stm)
			// }
			buffer = append(buffer, stm)
			if len(buffer) >= batch {
				break
			}
		}
	})
	return buffer
}

func (db *replicationDB[T, R]) GetSchedulingByGroup(id GroupID) (ret []R) {
	db.withRLock(func() {
		g := db.mustGetGroup(id)
		ret = g.GetScheduling()
	})
	return
}

// GetReplicating returns the replicating spans
func (db *replicationDB[T, R]) GetReplicating() (ret []R) {
	db.withRLock(func() {
		ret = db.GetReplicatingWithoutLock()
	})
	return
}

func (db *replicationDB[T, R]) GetReplicatingWithoutLock() (ret []R) {
	for _, g := range db.taskGroups {
		ret = append(ret, g.GetReplicating()...)
	}
	return
}

func (db *replicationDB[T, R]) GetReplicatingSize() (size int) {
	db.withRLock(func() {
		for _, g := range db.taskGroups {
			size += g.GetReplicatingSize()
		}
	})
	return
}

func (db *replicationDB[T, R]) GetReplicatingByGroup(id GroupID) (ret []R) {
	db.withRLock(func() {
		g := db.mustGetGroup(id)
		ret = g.GetReplicating()
	})
	return
}

func (db *replicationDB[T, R]) GetScheduling() (ret []R) {
	db.withRLock(func() {
		ret = db.GetSchedulingWithoutLock()
	})
	return
}

func (db *replicationDB[T, R]) GetSchedulingWithoutLock() (ret []R) {
	for _, g := range db.taskGroups {
		ret = append(ret, g.GetScheduling()...)
	}
	return
}

func (db *replicationDB[T, R]) GetSchedulingSize() int {
	size := 0
	for _, g := range db.taskGroups {
		size += g.GetSchedulingSize()
	}
	return size
}

func (db *replicationDB[T, R]) GetImbalanceGroupNodeTask(nodes map[node.ID]*node.Info) (groups map[GroupID]map[node.ID]R, valid bool) {
	groups = make(map[GroupID]map[node.ID]R, len(db.taskGroups))
	nodesNum := len(nodes)
	db.withRLock(func() {
		var zeroR R
		for gid, g := range db.taskGroups {
			if !g.IsStable() {
				groups = nil
				valid = false
				return
			}

			totalSpan, nodesTasks := 0, g.GetNodeTasks()
			for _, tasks := range nodesTasks {
				totalSpan += len(tasks)
			}
			if totalSpan == 0 {
				log.Warn("scheduler: meet empty group", zap.String("schedulerID", db.id), zap.String("group", GetGroupName(gid)))
				db.maybeRemoveGroup(g)
				continue
			}

			// calc imbalance state for stable group
			upperLimitPerNode := int(math.Ceil(float64(totalSpan) / float64(nodesNum)))
			groupMap := make(map[node.ID]R, nodesNum)
			limitCnt := 0
			for nodeID, tasks := range nodesTasks {
				switch len(tasks) {
				case upperLimitPerNode:
					limitCnt++
					for _, stm := range tasks {
						groupMap[nodeID] = stm
						break
					}
				case upperLimitPerNode - 1:
					groupMap[nodeID] = zeroR
				default:
					// len(tasks) > upperLimitPerNode || len(tasks) < upperLimitPerNode-1
					log.Error("scheduler: invalid group state",
						zap.String("schedulerID", db.id),
						zap.String("group", GetGroupName(gid)), zap.Int("totalSpan", totalSpan),
						zap.Int("nodesNum", nodesNum), zap.Int("upperLimitPerNode", upperLimitPerNode),
						zap.String("node", nodeID.String()), zap.Int("nodeTaskSize", len(tasks)))
				}
			}
			if limitCnt < nodesNum {
				for nodeID := range nodes {
					if _, ok := groupMap[nodeID]; !ok {
						groupMap[nodeID] = zeroR
					}
				}
				// only record imbalance group
				groups[gid] = groupMap
			}
		}
	})
	return groups, true
}

// GetTaskSizePerNode returns the size of the task per node
func (db *replicationDB[T, R]) GetTaskSizePerNode() (sizeMap map[node.ID]int) {
	sizeMap = make(map[node.ID]int)
	db.withRLock(func() {
		for _, g := range db.taskGroups {
			for nodeID, tasks := range g.GetNodeTasks() {
				sizeMap[nodeID] += len(tasks)
			}
		}
	})
	return
}

func (db *replicationDB[T, R]) GetTaskByNodeID(id node.ID) (ret []R) {
	db.withRLock(func() {
		for _, g := range db.taskGroups {
			for _, value := range g.GetNodeTasks()[id] {
				ret = append(ret, value)
			}
		}
	})
	return
}

func (db *replicationDB[T, R]) GetTaskSizeByNodeID(id node.ID) (size int) {
	db.withRLock(func() {
		for _, g := range db.taskGroups {
			size += g.GetTaskSizeByNodeID(id)
		}
	})
	return
}

func (db *replicationDB[T, R]) GetTaskSizePerNodeByGroup(id GroupID) (sizeMap map[node.ID]int) {
	db.withRLock(func() {
		sizeMap = db.getTaskSizePerNodeByGroup(id)
	})
	return
}

func (db *replicationDB[T, R]) getTaskSizePerNodeByGroup(id GroupID) (sizeMap map[node.ID]int) {
	sizeMap = make(map[node.ID]int)
	for nodeID, tasks := range db.mustGetGroup(id).GetNodeTasks() {
		sizeMap[nodeID] = len(tasks)
	}
	return
}

func (db *replicationDB[T, R]) GetGroupStat() string {
	distribute := strings.Builder{}
	db.withRLock(func() {
		total := 0
		for _, group := range db.GetGroups() {
			if total > 0 {
				distribute.WriteString(" ")
			}
			distribute.WriteString(GetGroupName(group))
			distribute.WriteString(": [")
			for nodeID, size := range db.getTaskSizePerNodeByGroup(group) {
				distribute.WriteString(nodeID.String())
				distribute.WriteString("->")
				distribute.WriteString(strconv.Itoa(size))
				distribute.WriteString("; ")
			}
			distribute.WriteString("] ")
		}
	})
	return distribute.String()
}

func (db *replicationDB[T, R]) getOrCreateGroup(task R) *replicationGroup[T, R] {
	groupID := task.GetGroupID()
	g, ok := db.taskGroups[groupID]
	if !ok {
		g = newReplicationGroup[T, R](db.id, groupID)
		db.taskGroups[groupID] = g
		log.Info("scheduler: add new task group", zap.String("schedulerID", db.id),
			zap.String("group", GetGroupName(groupID)),
			zap.Stringer("groupType", GroupTpye(groupID)))
	}
	return g
}

func (db *replicationDB[T, R]) maybeRemoveGroup(g *replicationGroup[T, R]) {
	if g.groupID == DefaultGroupID || !g.IsEmpty() {
		return
	}
	delete(db.taskGroups, g.groupID)
	log.Info("scheduler: remove task group", zap.String("schedulerID", db.id),
		zap.String("group", GetGroupName(g.groupID)),
		zap.Stringer("groupType", GroupTpye(g.groupID)))
}

func (db *replicationDB[T, R]) mustGetGroup(groupID GroupID) *replicationGroup[T, R] {
	g, ok := db.taskGroups[groupID]
	if !ok {
		log.Panic("group not found", zap.String("group", GetGroupName(groupID)))
	}
	return g
}

func (db *replicationDB[T, R]) AddReplicatingWithoutLock(task R) {
	g := db.getOrCreateGroup(task)
	g.AddReplicatingReplica(task)
}

func (db *replicationDB[T, R]) AddAbsentWithoutLock(task R) {
	g := db.getOrCreateGroup(task)
	g.AddAbsentReplica(task)
}

func (db *replicationDB[T, R]) MarkAbsentWithoutLock(task R) {
	g := db.mustGetGroup(task.GetGroupID())
	g.MarkReplicaAbsent(task)
}

func (db *replicationDB[T, R]) MarkSchedulingWithoutLock(task R) {
	g := db.mustGetGroup(task.GetGroupID())
	g.MarkReplicaScheduling(task)
}

func (db *replicationDB[T, R]) MarkReplicatingWithoutLock(task R) {
	g := db.mustGetGroup(task.GetGroupID())
	g.MarkReplicaReplicating(task)
}

func (db *replicationDB[T, R]) BindReplicaToNodeWithoutLock(old, new node.ID, replica R) {
	g := db.mustGetGroup(replica.GetGroupID())
	g.BindReplicaToNode(old, new, replica)
}

func (db *replicationDB[T, R]) RemoveReplicaWithoutLock(replica R) {
	g := db.mustGetGroup(replica.GetGroupID())
	g.RemoveReplica(replica)
	db.maybeRemoveGroup(g)
}
