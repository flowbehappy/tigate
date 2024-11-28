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
	"strconv"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

type groupTpye int8

const (
	groupDefault groupTpye = iota
	groupTable
	groupHotLevel1
)

func (gt groupTpye) Less(other groupTpye) bool {
	return gt < other
}

func (gt groupTpye) String() string {
	switch gt {
	case groupDefault:
		return "default"
	case groupTable:
		return "table"
	default:
		return "HotLevel" + strconv.Itoa(int(gt-groupHotLevel1))
	}
}

type GroupID = int64

const defaultGroupID GroupID = 0

func getGroupID(gt groupTpye, tableID int64) GroupID {
	// use high 8 bits to store the group type
	id := int64(gt) << 56
	if gt == groupTable {
		return id | tableID
	}
	return id
}

func getGroupType(id GroupID) groupTpye {
	return groupTpye(id >> 56)
}

func printGroupID(id GroupID) string {
	gt := groupTpye(id >> 56)
	if gt == groupTable {
		return fmt.Sprintf("%s-%d", gt.String(), id&0x00FFFFFFFFFFFFFF)
	}
	return gt.String()
}

func (db *ReplicationDB) GetGroups() []GroupID {
	db.lock.RLock()
	defer db.lock.RUnlock()

	groups := make([]GroupID, 0, len(db.taskGroups))
	for id := range db.taskGroups {
		groups = append(groups, id)
	}
	return groups
}

// GetAbsent returns the absent spans with the maxSize, push the spans to the buffer
func (db *ReplicationDB) GetAbsent(buffer []*SpanReplication, maxSize int) []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	buffer = buffer[:0]
	for _, g := range db.taskGroups {
		for _, stm := range g.absent {
			buffer = append(buffer, stm)
			if len(buffer) >= maxSize {
				break
			}
		}
	}
	return buffer
}

func (db *ReplicationDB) GetAbsentByGroup(id GroupID, buffer []*SpanReplication, maxSize int) []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	g := db.mustGetGroup(id)
	for _, stm := range g.absent {
		buffer = append(buffer, stm)
		if len(buffer) >= maxSize {
			break
		}
	}
	return buffer
}

// GetReplicating returns the replicating spans
func (db *ReplicationDB) GetReplicating() []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var replicating = make([]*SpanReplication, 0)
	for _, g := range db.taskGroups {
		for _, stm := range g.replicating {
			replicating = append(replicating, stm)
		}
	}
	return replicating
}

func (db *ReplicationDB) GetReplicatingByGroup(id GroupID) []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	g := db.mustGetGroup(id)
	var replicating = make([]*SpanReplication, 0, len(g.replicating))
	for _, stm := range g.replicating {
		replicating = append(replicating, stm)
	}
	return replicating
}

// GetTaskSizePerNode returns the size of the task per node
func (db *ReplicationDB) GetTaskSizePerNode() map[node.ID]int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	sizeMap := make(map[node.ID]int, len(db.mustGetGroup(defaultGroupID).GetNodeTasks()))
	for _, g := range db.taskGroups {
		for nodeID, tasks := range g.GetNodeTasks() {
			sizeMap[nodeID] += len(tasks)
		}
	}
	return sizeMap
}

func (db *ReplicationDB) GetTaskSizePerNodeByGroup(id GroupID) map[node.ID]int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	g := db.mustGetGroup(id)
	sizeMap := make(map[node.ID]int, len(g.GetNodeTasks()))
	for nodeID, tasks := range g.GetNodeTasks() {
		sizeMap[nodeID] += len(tasks)
	}
	return sizeMap
}

func (db *ReplicationDB) GetGroupStat() string {
	distribute := strings.Builder{}
	total := 0
	for _, group := range db.GetGroups() {
		if total > 0 {
			distribute.WriteString(" ")
		}
		distribute.WriteString(printGroupID(group))
		distribute.WriteString(": [")
		for nodeID, size := range db.GetTaskSizePerNodeByGroup(group) {
			distribute.WriteString(nodeID.String())
			distribute.WriteString("->")
			distribute.WriteString(strconv.Itoa(size))
			distribute.WriteString("; ")
		}
		distribute.WriteString("]")
	}
	return distribute.String()
}

type replicationTaskGroup struct {
	changefeedID common.ChangeFeedID
	groupID      GroupID
	groupName    string
	// group the tasks by the node id for
	nodeTasks map[node.ID]map[common.DispatcherID]*SpanReplication
	// maps that maintained base on the span scheduling status
	replicating map[common.DispatcherID]*SpanReplication
	scheduling  map[common.DispatcherID]*SpanReplication
	absent      map[common.DispatcherID]*SpanReplication
}

func newReplicationTaskGroup(cfID common.ChangeFeedID, id GroupID) *replicationTaskGroup {
	return &replicationTaskGroup{
		changefeedID: cfID,
		groupID:      id,
		groupName:    printGroupID(id),
		nodeTasks:    make(map[node.ID]map[common.DispatcherID]*SpanReplication),
		replicating:  make(map[common.DispatcherID]*SpanReplication),
		scheduling:   make(map[common.DispatcherID]*SpanReplication),
		absent:       make(map[common.DispatcherID]*SpanReplication),
	}
}

func (g *replicationTaskGroup) mustVerifyGroupID(id GroupID) {
	if g.groupID != id {
		log.Panic("group id not match", zap.Int64("group", g.groupID), zap.Int64("id", id))
	}
}

// MarkSpanAbsent move the span to the absent status
func (g *replicationTaskGroup) MarkSpanAbsent(span *SpanReplication) {
	g.mustVerifyGroupID(span.groupID)
	log.Info("marking span absent",
		zap.String("changefeed", g.changefeedID.Name()),
		zap.String("group", g.groupName),
		zap.String("span", span.ID.String()),
		zap.String("node", span.GetNodeID().String()))

	delete(g.scheduling, span.ID)
	delete(g.replicating, span.ID)
	g.absent[span.ID] = span
	originNodeID := span.GetNodeID()
	span.SetNodeID("")
	g.updateNodeMap(originNodeID, "", span)
}

// MarkSpanScheduling move the span to the scheduling map
func (g *replicationTaskGroup) MarkSpanScheduling(span *SpanReplication) {
	g.mustVerifyGroupID(span.groupID)
	log.Info("marking span scheduling",
		zap.String("changefeed", g.changefeedID.Name()),
		zap.String("group", g.groupName),
		zap.String("span", span.ID.String()))

	delete(g.absent, span.ID)
	delete(g.replicating, span.ID)
	g.scheduling[span.ID] = span
}

// AddReplicatingSpan adds a replicating the replicating map, that means the task is already scheduled to a dispatcher
func (g *replicationTaskGroup) AddReplicatingSpan(span *SpanReplication) {
	g.mustVerifyGroupID(span.groupID)
	nodeID := span.GetNodeID()
	log.Info("add an replicating span",
		zap.String("changefeed", g.changefeedID.Name()),
		zap.String("group", g.groupName),
		zap.String("nodeID", nodeID.String()),
		zap.String("span", span.ID.String()))
	g.replicating[span.ID] = span
	g.updateNodeMap("", nodeID, span)
}

// MarkSpanReplicating move the span to the replicating map
func (g *replicationTaskGroup) MarkSpanReplicating(span *SpanReplication) {
	g.mustVerifyGroupID(span.groupID)
	log.Info("marking span replicating",
		zap.String("changefeed", g.changefeedID.Name()),
		zap.String("group", g.groupName),
		zap.String("span", span.ID.String()))

	delete(g.absent, span.ID)
	delete(g.scheduling, span.ID)
	g.replicating[span.ID] = span
}

func (g *replicationTaskGroup) BindSpanToNode(old, new node.ID, span *SpanReplication) {
	g.mustVerifyGroupID(span.groupID)
	log.Info("bind span to node",
		zap.String("changefeed", g.changefeedID.Name()),
		zap.String("group", g.groupName),
		zap.String("span", span.ID.String()),
		zap.String("oldNode", old.String()),
		zap.String("node", new.String()))

	span.SetNodeID(new)
	delete(g.absent, span.ID)
	delete(g.replicating, span.ID)
	g.scheduling[span.ID] = span
	g.updateNodeMap(old, new, span)
}

// updateNodeMap updates the node map, it will remove the task from the old node and add it to the new node
func (g *replicationTaskGroup) updateNodeMap(old, new node.ID, span *SpanReplication) {
	//clear from the old node
	if old != "" {
		oldMap, ok := g.nodeTasks[old]
		if ok {
			delete(oldMap, span.ID)
			if len(oldMap) == 0 {
				delete(g.nodeTasks, old)
			}
		}
	}
	// add to the new node if the new node is not empty
	if new != "" {
		newMap, ok := g.nodeTasks[new]
		if !ok {
			newMap = make(map[common.DispatcherID]*SpanReplication)
			g.nodeTasks[new] = newMap
		}
		newMap[span.ID] = span
	}
}

// addAbsentReplicaSetUnLock adds the replica set to the absent map
func (g *replicationTaskGroup) AddAbsentReplicaSet(span *SpanReplication) {
	g.mustVerifyGroupID(span.groupID)
	g.absent[span.ID] = span
}

func (g *replicationTaskGroup) RemoveSpan(span *SpanReplication) {
	g.mustVerifyGroupID(span.groupID)
	log.Info("remove span",
		zap.String("changefeed", g.changefeedID.Name()),
		zap.String("group", g.groupName),
		zap.Int64("table", span.Span.TableID),
		zap.String("span", span.ID.String()))
	delete(g.absent, span.ID)
	delete(g.replicating, span.ID)
	delete(g.scheduling, span.ID)
	nodeMap := g.nodeTasks[span.GetNodeID()]
	delete(nodeMap, span.ID)
	if len(nodeMap) == 0 {
		delete(g.nodeTasks, span.GetNodeID())
	}
}

func (g *replicationTaskGroup) IsEmpty() bool {
	return len(g.replicating)+len(g.scheduling)+len(g.absent) == 0
}

func (g *replicationTaskGroup) GetTaskSizeByNodeID(nodeID node.ID) int {
	return len(g.nodeTasks[nodeID])
}

func (g *replicationTaskGroup) GetNodeTasks() map[node.ID]map[common.DispatcherID]*SpanReplication {
	return g.nodeTasks
}
