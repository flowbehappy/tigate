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
	"github.com/pingcap/ticdc/pkg/common"
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

type groupedReplicationDB[T ReplicationID, R Replication[T]] struct {
	taskGroups map[GroupID]*ReplicationGroup[T, R]
}

func (db groupedReplicationDB[T, R]) GetGroups() []replica.GroupID {
	db.lock.RLock()
	defer db.lock.RUnlock()

	groups := make([]replica.GroupID, 0, len(db.taskGroups))
	for id := range db.taskGroups {
		groups = append(groups, id)
	}
	return groups
}

func (db groupedReplicationDB[T, R]) GetAbsent() []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var absent = make([]*SpanReplication, 0)
	for _, g := range db.taskGroups {
		absent = append(absent, g.GetAbsent()...)
	}
	return absent
}

func (db groupedReplicationDB[T, R]) GetAbsentByGroup(id replica.GroupID, maxSize int) []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	buffer := make([]*SpanReplication, 0, maxSize)
	g := db.mustGetGroup(id)
	for _, stm := range g.absent {
		if !stm.IsDropped() {
			buffer = append(buffer, stm)
		}
		if len(buffer) >= maxSize {
			break
		}
	}
	return buffer
}

func (db groupedReplicationDB[T, R]) GetSchedulingByGroup(id replica.GroupID) []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	g := db.mustGetGroup(id)
	var scheduling = make([]*SpanReplication, 0, len(g.scheduling))
	for _, stm := range g.scheduling {
		scheduling = append(scheduling, stm)
	}
	return scheduling
}

// GetReplicating returns the replicating spans
func (db groupedReplicationDB[T, R]) GetReplicating() []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var replicating = make([]*SpanReplication, 0)
	for _, g := range db.taskGroups {
		replicating = append(replicating, g.GetReplicating()...)
	}
	return replicating
}

func (db groupedReplicationDB[T, R]) GetReplicatingByGroup(id replica.GroupID) []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	g := db.mustGetGroup(id)
	var replicating = make([]*SpanReplication, 0, len(g.replicating))
	for _, stm := range g.replicating {
		replicating = append(replicating, stm)
	}
	return replicating
}

func (db groupedReplicationDB[T, R]) GetScheduling() []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var scheduling = make([]*SpanReplication, 0)
	for _, g := range db.taskGroups {
		scheduling = append(scheduling, g.GetScheduling()...)
	}
	return scheduling
}

func (db groupedReplicationDB[T, R]) GetImbalanceGroupNodeTask(nodes map[node.ID]*node.Info) (groups map[replica.GroupID]map[node.ID]*SpanReplication, valid bool) {
	groups = make(map[replica.GroupID]map[node.ID]*SpanReplication, len(db.taskGroups))
	nodesNum := len(nodes)

	db.lock.RLock()
	defer db.lock.RUnlock()
	for gid, g := range db.taskGroups {
		if !g.IsStable() {
			return nil, false
		}

		totalSpan, nodesTasks := 0, g.GetNodeTasks()
		for _, tasks := range nodesTasks {
			totalSpan += len(tasks)
		}
		if totalSpan == 0 {
			log.Warn("meet empty group", zap.Stringer("changefeed", db.changefeedID), zap.String("group", replica.GetGroupName(gid)))
			db.maybeRemoveGroup(g)
			continue
		}

		// calc imbalance state for stable group
		upperLimitPerNode := int(math.Ceil(float64(totalSpan) / float64(nodesNum)))
		groupMap := make(map[node.ID]*SpanReplication, nodesNum)
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
				groupMap[nodeID] = nil
			default:
				// len(tasks) > upperLimitPerNode || len(tasks) < upperLimitPerNode-1
				log.Error("invalid group state",
					zap.String("changefeed", db.changefeedID.String()),
					zap.String("group", replica.GetGroupName(gid)), zap.Int("totalSpan", totalSpan),
					zap.Int("nodesNum", nodesNum), zap.Int("upperLimitPerNode", upperLimitPerNode),
					zap.String("node", nodeID.String()), zap.Int("nodeTaskSize", len(tasks)))
			}
		}
		if limitCnt < nodesNum {
			for nodeID := range nodes {
				if _, ok := groupMap[nodeID]; !ok {
					groupMap[nodeID] = nil
				}
			}
			// only record imbalance group
			groups[gid] = groupMap
		}
	}
	return groups, true
}

// GetTaskSizePerNode returns the size of the task per node
func (db groupedReplicationDB[T, R]) GetTaskSizePerNode() map[node.ID]int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	sizeMap := make(map[node.ID]int)
	for _, g := range db.taskGroups {
		for nodeID, tasks := range g.GetNodeTasks() {
			sizeMap[nodeID] += len(tasks)
		}
	}
	return sizeMap
}

func (db groupedReplicationDB[T, R]) GetTaskSizePerNodeByGroup(id replica.GroupID) map[node.ID]int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	g := db.mustGetGroup(id)
	sizeMap := make(map[node.ID]int, len(g.GetNodeTasks()))
	for nodeID, tasks := range g.GetNodeTasks() {
		sizeMap[nodeID] += len(tasks)
	}
	return sizeMap
}

func (db groupedReplicationDB[T, R]) GetGroupStat() string {
	distribute := strings.Builder{}
	total := 0
	for _, group := range db.GetGroups() {
		if total > 0 {
			distribute.WriteString(" ")
		}
		distribute.WriteString(replica.GetGroupName(group))
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

func (db groupedReplicationDB[T, R]) getOrCreateGroup(task *SpanReplication) *replica.ReplicationGroup[common.DispatcherID, *SpanReplication] {
	groupID := task.groupID
	g, ok := db.taskGroups[groupID]
	if !ok {
		g = replica.NewReplicationGroup(db.changefeedID.String(), groupID)
		db.taskGroups[groupID] = g
		log.Info("create new task group", zap.Stringer("groupType", replica.GroupTpye(groupID)),
			zap.Int64("tableID", task.Span.TableID))
	}
	return g
}

func (db groupedReplicationDB[T, R]) maybeRemoveGroup(g *replica.ReplicationGroup[common.DispatcherID, *SpanReplication]) {
	if g.GetReplicatingSize() == 0 && g.GetSchedulingSize() == 0 && g.GetAbsentSize() == 0 {
		delete(db.taskGroups, g.GetID())
		log.Info("remove task group", zap.Stringer("groupType", replica.GroupTpye(g.GetID())))
	}
}

func (db groupedReplicationDB[T, R]) mustGetGroup(groupID replica.GroupID) *replicationTaskGroup {
	g, ok := db.taskGroups[groupID]
	if !ok {
		log.Panic("group not found", zap.String("group", printreplica.GroupID(groupID)))
	}
	return g
}
