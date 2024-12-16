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
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

type (
	GroupID   = int64
	GroupTpye int8
)

const DefaultGroupID GroupID = 0

const (
	GroupDefault GroupTpye = iota
	GroupTable
	// add more group strategy later
	// groupHotLevel1
)

// replicationGroup maintains a group of replication tasks.
// All methods are not thread-safe.
type replicationGroup[T ReplicationID, R Replication[T]] struct {
	id        string
	groupID   GroupID
	groupName string

	nodeTasks map[node.ID]map[T]R // group the tasks by the node id

	// maps that maintained base on the replica scheduling status
	replicating map[T]R
	scheduling  map[T]R
	absent      map[T]R

	// checker []replica.Checker
}

func newReplicationGroup[T ReplicationID, R Replication[T]](id string, groupID GroupID) *replicationGroup[T, R] {
	return &replicationGroup[T, R]{
		id:          id,
		groupID:     groupID,
		groupName:   GetGroupName(groupID),
		nodeTasks:   make(map[node.ID]map[T]R),
		replicating: make(map[T]R),
		scheduling:  make(map[T]R),
		absent:      make(map[T]R),
	}
}

func (g *replicationGroup[T, R]) mustVerifyGroupID(id GroupID) {
	if g.groupID != id {
		log.Panic("scheduler: group id not match", zap.Int64("group", g.groupID), zap.Int64("id", id))
	}
}

// MarkReplicaAbsent move the replica to the absent status
func (g *replicationGroup[T, R]) MarkReplicaAbsent(replica R) {
	g.mustVerifyGroupID(replica.GetGroupID())
	log.Info("scheduler: marking replica absent",
		zap.String("schedulerID", g.id),
		zap.String("group", g.groupName),
		zap.String("replicaID", replica.GetID().String()),
		zap.String("node", replica.GetNodeID().String()))

	id := replica.GetID()
	delete(g.scheduling, id)
	delete(g.replicating, id)
	g.absent[id] = replica
	originNodeID := replica.GetNodeID()
	replica.SetNodeID("")
	g.updateNodeMap(originNodeID, "", replica)
}

// MarkReplicaScheduling move the replica to the scheduling map
func (g *replicationGroup[T, R]) MarkReplicaScheduling(replica R) {
	g.mustVerifyGroupID(replica.GetGroupID())
	log.Info("scheduler: marking replica scheduling",
		zap.String("schedulerID", g.id),
		zap.String("group", g.groupName),
		zap.String("replica", replica.GetID().String()))

	delete(g.absent, replica.GetID())
	delete(g.replicating, replica.GetID())
	g.scheduling[replica.GetID()] = replica
}

// AddReplicatingReplica adds a replicating the replicating map, that means the task is already scheduled to a dispatcher
func (g *replicationGroup[T, R]) AddReplicatingReplica(replica R) {
	g.mustVerifyGroupID(replica.GetGroupID())
	nodeID := replica.GetNodeID()
	log.Info("scheduler: add an replicating replica",
		zap.String("schedulerID", g.id),
		zap.String("group", g.groupName),
		zap.String("nodeID", nodeID.String()),
		zap.String("replica", replica.GetID().String()))
	g.replicating[replica.GetID()] = replica
	g.updateNodeMap("", nodeID, replica)
}

// MarkReplicaReplicating move the replica to the replicating map
func (g *replicationGroup[T, R]) MarkReplicaReplicating(replica R) {
	g.mustVerifyGroupID(replica.GetGroupID())
	log.Info("scheduler: marking replica replicating",
		zap.String("schedulerID", g.id),
		zap.String("group", g.groupName),
		zap.String("replica", replica.GetID().String()))

	delete(g.absent, replica.GetID())
	delete(g.scheduling, replica.GetID())
	g.replicating[replica.GetID()] = replica
}

func (g *replicationGroup[T, R]) BindReplicaToNode(old, new node.ID, replica R) {
	g.mustVerifyGroupID(replica.GetGroupID())
	log.Info("scheduler: bind replica to node",
		zap.String("schedulerID", g.id),
		zap.String("group", g.groupName),
		zap.String("replica", replica.GetID().String()),
		zap.String("oldNode", old.String()),
		zap.String("node", new.String()))

	replica.SetNodeID(new)
	delete(g.absent, replica.GetID())
	delete(g.replicating, replica.GetID())
	g.scheduling[replica.GetID()] = replica
	g.updateNodeMap(old, new, replica)
}

// updateNodeMap updates the node map, it will remove the task from the old node and add it to the new node
func (g *replicationGroup[T, R]) updateNodeMap(old, new node.ID, replica R) {
	//clear from the old node
	if old != "" {
		oldMap, ok := g.nodeTasks[old]
		if ok {
			delete(oldMap, replica.GetID())
			if len(oldMap) == 0 {
				delete(g.nodeTasks, old)
			}
		}
	}
	// add to the new node if the new node is not empty
	if new != "" {
		newMap, ok := g.nodeTasks[new]
		if !ok {
			newMap = make(map[T]R)
			g.nodeTasks[new] = newMap
		}
		newMap[replica.GetID()] = replica
	}
}

func (g *replicationGroup[T, R]) AddAbsentReplica(replica R) {
	g.mustVerifyGroupID(replica.GetGroupID())
	g.absent[replica.GetID()] = replica
}

func (g *replicationGroup[T, R]) RemoveReplica(replica R) {
	g.mustVerifyGroupID(replica.GetGroupID())
	log.Info("scheduler: remove replica",
		zap.String("schedulerID", g.id),
		zap.String("group", g.groupName),
		zap.String("replica", replica.GetID().String()))
	delete(g.absent, replica.GetID())
	delete(g.replicating, replica.GetID())
	delete(g.scheduling, replica.GetID())
	nodeMap := g.nodeTasks[replica.GetNodeID()]
	delete(nodeMap, replica.GetID())
	if len(nodeMap) == 0 {
		delete(g.nodeTasks, replica.GetNodeID())
	}
}

func (g *replicationGroup[T, R]) IsEmpty() bool {
	return g.IsStable() && len(g.replicating) == 0
}

func (g *replicationGroup[T, R]) IsStable() bool {
	return len(g.scheduling) == 0 && len(g.absent) == 0
}

func (g *replicationGroup[T, R]) GetTaskSizeByNodeID(nodeID node.ID) int {
	return len(g.nodeTasks[nodeID])
}

func (g *replicationGroup[T, R]) GetNodeTasks() map[node.ID]map[T]R {
	return g.nodeTasks
}

func (g *replicationGroup[T, R]) GetAbsentSize() int {
	return len(g.absent)
}

func (g *replicationGroup[T, R]) GetAbsent() []R {
	res := make([]R, 0, len(g.absent))
	for _, r := range g.absent {
		if !r.ShouldRun() {
			continue
		}
		res = append(res, r)
	}
	return res
}

func (g *replicationGroup[T, R]) GetSchedulingSize() int {
	return len(g.scheduling)
}

func (g *replicationGroup[T, R]) GetScheduling() []R {
	res := make([]R, 0, len(g.scheduling))
	for _, r := range g.scheduling {
		res = append(res, r)
	}
	return res
}

func (g *replicationGroup[T, R]) GetReplicatingSize() int {
	return len(g.replicating)
}

func (g *replicationGroup[T, R]) GetReplicating() []R {
	res := make([]R, 0, len(g.replicating))
	for _, r := range g.replicating {
		res = append(res, r)
	}
	return res
}

func (g *replicationGroup[T, R]) GetTaskSizePerNode() map[node.ID]int {
	res := make(map[node.ID]int)
	for nodeID, tasks := range g.nodeTasks {
		res[nodeID] = len(tasks)
	}
	return res
}

func GetGroupName(id GroupID) string {
	gt := GroupTpye(id >> 56)
	if gt == GroupTable {
		return fmt.Sprintf("%s-%d", gt.String(), id&0x00FFFFFFFFFFFFFF)
	}
	return gt.String()
}

func (gt GroupTpye) Less(other GroupTpye) bool {
	return gt < other
}

func (gt GroupTpye) String() string {
	switch gt {
	case GroupDefault:
		return "default"
	case GroupTable:
		return "table"
	default:
		// return "HotLevel" + strconv.Itoa(int(gt-groupHotLevel1))
		panic("unreachable")
	}
}

func GenGroupID(gt GroupTpye, tableID int64) GroupID {
	// use high 8 bits to store the group type
	id := int64(gt) << 56
	if gt == GroupTable {
		return id | tableID
	}
	return id
}

func GetGroupType(id GroupID) GroupTpye {
	return GroupTpye(id >> 56)
}
