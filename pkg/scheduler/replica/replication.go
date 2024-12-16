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
}

type ReplicationDB[T ReplicationID, R Replication[T]] interface {
	// global scheduler interface
	ScheduleGroup[T, R]
	GetImbalanceGroupNodeTask(nodes map[node.ID]*node.Info) (groups map[GroupID]map[node.ID]R, valid bool)
	// group scheduler interface
	GetGroups() []GroupID
	GetAbsentByGroup(groupID GroupID, batch int) []R
	GetSchedulingByGroup(groupID GroupID) []R
	GetReplicatingByGroup(groupID GroupID) []R
	GetTaskSizePerNodeByGroup(groupID GroupID) map[node.ID]int
}

func NewReplicationGroups[T ReplicationID, R Replication[T]](id string) *ReplicationGroups[T, R] {
	r := &ReplicationGroups[T, R]{
		id:         id,
		taskGroups: make(map[GroupID]*ReplicationGroup[T, R]),
	}
	r.taskGroups[DefaultGroupID] = NewDefaultReplicationGroup[T, R](id)
	return r
}

type ReplicationGroups[T ReplicationID, R Replication[T]] struct {
	id         string
	taskGroups map[GroupID]*ReplicationGroup[T, R]
}

func (db ReplicationGroups[T, R]) GetGroups() []GroupID {
	groups := make([]GroupID, 0, len(db.taskGroups))
	for id := range db.taskGroups {
		groups = append(groups, id)
	}
	return groups
}

func (db ReplicationGroups[T, R]) GetAbsent() []R {
	var absent = make([]R, 0)
	for _, g := range db.taskGroups {
		absent = append(absent, g.GetAbsent()...)
	}
	return absent
}

func (db ReplicationGroups[T, R]) GetAbsentSize() int {
	size := 0
	for _, g := range db.taskGroups {
		size += g.GetAbsentSize()
	}
	return size
}

func (db ReplicationGroups[T, R]) GetAbsentByGroup(id GroupID, batch int) []R {
	buffer := make([]R, 0, batch)
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
	return buffer
}

func (db ReplicationGroups[T, R]) GetSchedulingByGroup(id GroupID) []R {
	g := db.mustGetGroup(id)
	return g.GetScheduling()
}

// GetReplicating returns the replicating spans
func (db ReplicationGroups[T, R]) GetReplicating() []R {
	var replicating = make([]R, 0)
	for _, g := range db.taskGroups {
		replicating = append(replicating, g.GetReplicating()...)
	}
	return replicating
}

func (db ReplicationGroups[T, R]) GetReplicatingSize() int {
	size := 0
	for _, g := range db.taskGroups {
		size += g.GetReplicatingSize()
	}
	return size
}

func (db ReplicationGroups[T, R]) GetReplicatingByGroup(id GroupID) []R {
	g := db.mustGetGroup(id)
	var replicating = make([]R, 0, len(g.replicating))
	for _, stm := range g.replicating {
		replicating = append(replicating, stm)
	}
	return replicating
}

func (db ReplicationGroups[T, R]) GetScheduling() []R {
	var scheduling = make([]R, 0)
	for _, g := range db.taskGroups {
		scheduling = append(scheduling, g.GetScheduling()...)
	}
	return scheduling
}

func (db ReplicationGroups[T, R]) GetSchedulingSize() int {
	size := 0
	for _, g := range db.taskGroups {
		size += g.GetSchedulingSize()
	}
	return size
}

func (db ReplicationGroups[T, R]) GetImbalanceGroupNodeTask(nodes map[node.ID]*node.Info) (groups map[GroupID]map[node.ID]R, valid bool) {
	groups = make(map[GroupID]map[node.ID]R, len(db.taskGroups))
	nodesNum := len(nodes)

	var zeroR R
	for gid, g := range db.taskGroups {
		if !g.IsStable() {
			return nil, false
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
	return groups, true
}

// GetTaskSizePerNode returns the size of the task per node
func (db ReplicationGroups[T, R]) GetTaskSizePerNode() map[node.ID]int {
	sizeMap := make(map[node.ID]int)
	for _, g := range db.taskGroups {
		for nodeID, tasks := range g.GetNodeTasks() {
			sizeMap[nodeID] += len(tasks)
		}
	}
	return sizeMap
}

func (db ReplicationGroups[T, R]) GetTaskByNodeID(id node.ID) []R {
	var stms []R
	for _, g := range db.taskGroups {
		for _, value := range g.GetNodeTasks()[id] {
			stms = append(stms, value)
		}
	}
	return stms
}

func (db ReplicationGroups[T, R]) GetTaskSizeByNodeID(id node.ID) int {
	size := 0
	for _, g := range db.taskGroups {
		size += len(g.GetNodeTasks()[id])
	}
	return size
}

func (db ReplicationGroups[T, R]) GetTaskSizePerNodeByGroup(id GroupID) map[node.ID]int {
	g := db.mustGetGroup(id)
	sizeMap := make(map[node.ID]int, len(g.GetNodeTasks()))
	for nodeID, tasks := range g.GetNodeTasks() {
		sizeMap[nodeID] += len(tasks)
	}
	return sizeMap
}

func (db ReplicationGroups[T, R]) GetGroupStat() string {
	distribute := strings.Builder{}
	total := 0
	for _, group := range db.GetGroups() {
		if total > 0 {
			distribute.WriteString(" ")
		}
		distribute.WriteString(GetGroupName(group))
		distribute.WriteString(": [")
		for nodeID, size := range db.GetTaskSizePerNodeByGroup(group) {
			distribute.WriteString(nodeID.String())
			distribute.WriteString("->")
			distribute.WriteString(strconv.Itoa(size))
			distribute.WriteString("; ")
		}
		distribute.WriteString("] ")
	}
	return distribute.String()
}

func (db ReplicationGroups[T, R]) getOrCreateGroup(task R) *ReplicationGroup[T, R] {
	groupID := task.GetGroupID()
	g, ok := db.taskGroups[groupID]
	if !ok {
		g = NewReplicationGroup[T, R](db.id, groupID)
		db.taskGroups[groupID] = g
		log.Info("scheduler: add new task group", zap.String("schedulerID", db.id),
			zap.String("group", GetGroupName(groupID)),
			zap.Stringer("groupType", GroupTpye(groupID)))
	}
	return g
}

func (db ReplicationGroups[T, R]) maybeRemoveGroup(g *ReplicationGroup[T, R]) {
	if g.groupID == DefaultGroupID || !g.IsEmpty() {
		return
	}
	delete(db.taskGroups, g.groupID)
	log.Info("scheduler: remove task group", zap.String("schedulerID", db.id),
		zap.String("group", GetGroupName(g.groupID)),
		zap.Stringer("groupType", GroupTpye(g.groupID)))
}

func (db ReplicationGroups[T, R]) mustGetGroup(groupID GroupID) *ReplicationGroup[T, R] {
	g, ok := db.taskGroups[groupID]
	if !ok {
		log.Panic("group not found", zap.String("group", GetGroupName(groupID)))
	}
	return g
}

func (db ReplicationGroups[T, R]) AddReplicatingReplica(task R) {
	g := db.getOrCreateGroup(task)
	g.AddReplicatingReplica(task)
}

func (db ReplicationGroups[T, R]) AddAbsentReplica(task R) {
	g := db.getOrCreateGroup(task)
	g.AddAbsentReplica(task)
}

func (db ReplicationGroups[T, R]) MarkReplicaAbsent(task R) {
	g := db.getOrCreateGroup(task)
	g.MarkReplicaAbsent(task)
}

func (db ReplicationGroups[T, R]) MarkReplicaScheduling(task R) {
	g := db.getOrCreateGroup(task)
	g.MarkReplicaScheduling(task)
}

func (db ReplicationGroups[T, R]) MarkReplicaReplicating(task R) {
	g := db.getOrCreateGroup(task)
	g.MarkReplicaReplicating(task)
}

func (db ReplicationGroups[T, R]) BindReplicaToNode(old, new node.ID, replica R) {
	g := db.getOrCreateGroup(replica)
	g.BindReplicaToNode(old, new, replica)
}

func (db ReplicationGroups[T, R]) RemoveReplica(task R) {
	g := db.mustGetGroup(task.GetGroupID())
	g.RemoveReplica(task)
	db.maybeRemoveGroup(g)
}
