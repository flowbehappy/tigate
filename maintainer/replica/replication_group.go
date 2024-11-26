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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

type groupTpye int8

const (
	groupDefault groupTpye = iota
	groupTableHeader
	groupHotLevel1
)

func (gt groupTpye) Less(other groupTpye) bool {
	return gt < other
}

func (gt groupTpye) String() string {
	switch gt {
	case groupDefault:
		return "default"
	case groupTableHeader:
		return "tableHeader"
	default:
		return "groupHotLevel" + strconv.Itoa(int(gt-groupHotLevel1))
	}
}

type groupID = int64

const defaultGroupID groupID = 0

func getGroupID(gt groupTpye, tableID int64) groupID {
	// use high 8 bits to store the group type
	id := int64(gt) << 56
	if gt == groupTableHeader {
		return id | tableID
	}
	return id
}

func printGroupID(id groupID) string {
	gt := groupTpye(id >> 56)
	if gt == groupTableHeader {
		return fmt.Sprintf("%s-%d", gt.String(), id&0x00FFFFFFFFFFFFFF)
	}
	return gt.String()
}

type replicationTaskGroup struct {
	changefeedID common.ChangeFeedID
	groupID      groupID
	groupName    string
	// group the tasks by the node id for
	nodeTasks map[node.ID]map[common.DispatcherID]*SpanReplication
	// maps that maintained base on the span scheduling status
	replicating map[common.DispatcherID]*SpanReplication
	scheduling  map[common.DispatcherID]*SpanReplication
	absent      map[common.DispatcherID]*SpanReplication
}

func newReplicationTaskGroup(cfID common.ChangeFeedID, id groupID) *replicationTaskGroup {
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

func (g *replicationTaskGroup) mustVerifyGroupID(id groupID) {
	if g.groupID != id {
		log.Panic("group id not match", zap.Int64("group", g.groupID), zap.Int64("id", id))
	}
}

func (g *replicationTaskGroup) addTask(task *SpanReplication) {
	g.mustVerifyGroupID(task.groupID)
	g.absent[task.ID] = task
}

// MarkSpanAbsent move the span to the absent status
func (g *replicationTaskGroup) MarkSpanAbsent(span *SpanReplication) {
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
	log.Info("marking span scheduling",
		zap.String("changefeed", g.changefeedID.Name()),
		zap.String("group", g.groupName),
		zap.String("span", span.ID.String()))

	delete(g.absent, span.ID)
	delete(g.replicating, span.ID)
	g.scheduling[span.ID] = span
}

// AddReplicatingSpan adds a replicating the replicating map, that means the task is already scheduled to a dispatcher
func (g *replicationTaskGroup) AddReplicatingSpan(task *SpanReplication) {
	nodeID := task.GetNodeID()
	log.Info("add an replicating span",
		zap.String("changefeed", g.changefeedID.Name()),
		zap.String("group", g.groupName),
		zap.String("nodeID", nodeID.String()),
		zap.String("span", task.ID.String()))
	g.replicating[task.ID] = task
	g.updateNodeMap("", nodeID, task)
}

// MarkSpanReplicating move the span to the replicating map
func (g *replicationTaskGroup) MarkSpanReplicating(span *SpanReplication) {
	log.Info("marking span replicating",
		zap.String("changefeed", g.changefeedID.Name()),
		zap.String("group", g.groupName),
		zap.String("span", span.ID.String()))

	delete(g.absent, span.ID)
	delete(g.scheduling, span.ID)
	g.replicating[span.ID] = span
}

func (g *replicationTaskGroup) BindSpanToNode(old, new node.ID, task *SpanReplication) {
	log.Info("bind span to node",
		zap.String("changefeed", g.changefeedID.Name()),
		zap.String("group", g.groupName),
		zap.String("span", task.ID.String()),
		zap.String("oldNode", old.String()),
		zap.String("node", new.String()))

	task.SetNodeID(new)
	delete(g.absent, task.ID)
	delete(g.replicating, task.ID)
	g.scheduling[task.ID] = task
	g.updateNodeMap(old, new, task)
}

// updateNodeMap updates the node map, it will remove the task from the old node and add it to the new node
func (g *replicationTaskGroup) updateNodeMap(old, new node.ID, task *SpanReplication) {
	//clear from the old node
	if old != "" {
		oldMap, ok := g.nodeTasks[old]
		if ok {
			delete(oldMap, task.ID)
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
		newMap[task.ID] = task
	}
}

// addAbsentReplicaSetUnLock adds the replica set to the absent map
func (g *replicationTaskGroup) AddAbsentReplicaSet(tasks ...*SpanReplication) {
	for _, task := range tasks {
		g.absent[task.ID] = task
	}
}

func (g *replicationTaskGroup) RemoveSpan(span *SpanReplication) {
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

func (g *replicationTaskGroup) GetTaskSizeByNodeID(nodeID node.ID) int {
	return len(g.nodeTasks[nodeID])
}
