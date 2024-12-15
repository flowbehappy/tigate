// // Copyright 2024 PingCAP, Inc.
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // See the License for the specific language governing permissions and
// // limitations under the License.

package replica

type (
	GroupID   = int64
	GroupTpye int8
)

const (
	defaultGroupID GroupID = 0

	groupDefault GroupTpye = iota
	groupTable
	groupHotLevel1
)

// type ScheduleGroupImpl[T comparable, R Replication[T]] struct {
// 	id        string
// 	groupID   GroupID
// 	groupName string
// 	nodeTasks map[node.ID]map[T]R // group the tasks by the node id

// 	// maps that maintained base on the span scheduling status
// 	replicating map[T]R
// 	scheduling  map[T]R
// 	absent      map[T]R

// 	// checker []replica.Checker
// }

// func newScheduleGroupImpl[T comparable, R Replication[T]](id string, groupID GroupID) *ScheduleGroupImpl[T, R] {
// 	return &ScheduleGroupImpl[T, R]{
// 		id:          id,
// 		groupID:     groupID,
// 		groupName:   GetGroupName(groupID),
// 		nodeTasks:   make(map[node.ID]map[T]R),
// 		replicating: make(map[T]R),
// 		scheduling:  make(map[T]R),
// 		absent:      make(map[T]R),
// 	}
// }

// func (g *ScheduleGroupImpl[T, R]) mustVerifyGroupID(id GroupID) {
// 	if g.groupID != id {
// 		log.Panic("group id not match", zap.Int64("group", g.groupID), zap.Int64("id", id))
// 	}
// }

// // MarkSpanAbsent move the span to the absent status
// func (g *ScheduleGroupImpl[T, R]) MarkSpanAbsent(span R) {
// 	g.mustVerifyGroupID(span.GetGroupID())
// 	log.Info("marking span absent",
// 		zap.String("schedulerID", g.id),
// 		zap.String("group", g.groupName),
// 		zap.String("replicaID", span.GetID().String()),
// 		zap.String("node", span.GetNodeID().String()))

// 	delete(g.scheduling, span.ID)
// 	delete(g.replicating, span.ID)
// 	g.absent[span.ID] = span
// 	originNodeID := span.GetNodeID()
// 	span.SetNodeID("")
// 	g.updateNodeMap(originNodeID, "", span)
// }

// // MarkSpanScheduling move the span to the scheduling map
// func (g *ScheduleGroupImpl[T, R]) MarkSpanScheduling(span R) {
// 	g.mustVerifyGroupID(span.GetGroupID())
// 	log.Info("marking span scheduling",
// 		zap.String("changefeed", g.changefeedID.Name()),
// 		zap.String("group", g.groupName),
// 		zap.String("span", span.ID.String()))

// 	delete(g.absent, span.ID)
// 	delete(g.replicating, span.ID)
// 	g.scheduling[span.ID] = span
// }

// // AddReplicatingSpan adds a replicating the replicating map, that means the task is already scheduled to a dispatcher
// func (g *ScheduleGroupImpl[T, R]) AddReplicatingSpan(span R) {
// 	g.mustVerifyGroupID(span.GetGroupID())
// 	nodeID := span.GetNodeID()
// 	log.Info("add an replicating span",
// 		zap.String("changefeed", g.changefeedID.Name()),
// 		zap.String("group", g.groupName),
// 		zap.String("nodeID", nodeID.String()),
// 		zap.String("span", span.ID.String()))
// 	g.replicating[span.ID] = span
// 	g.updateNodeMap("", nodeID, span)
// }

// // MarkSpanReplicating move the span to the replicating map
// func (g *ScheduleGroupImpl[T, R]) MarkSpanReplicating(span R) {
// 	g.mustVerifyGroupID(span.GetGroupID())
// 	log.Info("marking span replicating",
// 		zap.String("changefeed", g.changefeedID.Name()),
// 		zap.String("group", g.groupName),
// 		zap.String("span", span.ID.String()))

// 	delete(g.absent, span.ID)
// 	delete(g.scheduling, span.ID)
// 	g.replicating[span.ID] = span
// }

// func (g *ScheduleGroupImpl[T, R]) BindSpanToNode(old, new node.ID, span R) {
// 	g.mustVerifyGroupID(span.GetGroupID())
// 	log.Info("bind span to node",
// 		zap.String("changefeed", g.changefeedID.Name()),
// 		zap.String("group", g.groupName),
// 		zap.String("span", span.ID.String()),
// 		zap.String("oldNode", old.String()),
// 		zap.String("node", new.String()))

// 	span.SetNodeID(new)
// 	delete(g.absent, span.ID)
// 	delete(g.replicating, span.ID)
// 	g.scheduling[span.ID] = span
// 	g.updateNodeMap(old, new, span)
// }

// // updateNodeMap updates the node map, it will remove the task from the old node and add it to the new node
// func (g *ScheduleGroupImpl[T, R]) updateNodeMap(old, new node.ID, span R) {
// 	//clear from the old node
// 	if old != "" {
// 		oldMap, ok := g.nodeTasks[old]
// 		if ok {
// 			delete(oldMap, span.ID)
// 			if len(oldMap) == 0 {
// 				delete(g.nodeTasks, old)
// 			}
// 		}
// 	}
// 	// add to the new node if the new node is not empty
// 	if new != "" {
// 		newMap, ok := g.nodeTasks[new]
// 		if !ok {
// 			newMap = make(map[T]R)
// 			g.nodeTasks[new] = newMap
// 		}
// 		newMap[span.ID] = span
// 	}
// }

// // addAbsentReplicaSetUnLock adds the replica set to the absent map
// func (g *ScheduleGroupImpl[T, R]) AddAbsentReplicaSet(span R) {
// 	g.mustVerifyGroupID(span.GetGroupID())
// 	g.absent[span.ID] = span
// }

// func (g *ScheduleGroupImpl[T, R]) RemoveSpan(span R) {
// 	g.mustVerifyGroupID(span.GetGroupID())
// 	log.Info("remove span",
// 		zap.String("changefeed", g.changefeedID.Name()),
// 		zap.String("group", g.groupName),
// 		zap.Int64("table", span.Span.TableID),
// 		zap.String("span", span.ID.String()))
// 	delete(g.absent, span.ID)
// 	delete(g.replicating, span.ID)
// 	delete(g.scheduling, span.ID)
// 	nodeMap := g.nodeTasks[span.GetNodeID()]
// 	delete(nodeMap, span.ID)
// 	if len(nodeMap) == 0 {
// 		delete(g.nodeTasks, span.GetNodeID())
// 	}
// }

// func (g *ScheduleGroupImpl[T, R]) IsEmpty() bool {
// 	return g.IsStable() && len(g.replicating) == 0
// }

// func (g *ScheduleGroupImpl[T, R]) IsStable() bool {
// 	return len(g.scheduling) == 0 && len(g.absent) == 0
// }

// func (g *ScheduleGroupImpl[T, R]) GetTaskSizeByNodeID(nodeID node.ID) int {
// 	return len(g.nodeTasks[nodeID])
// }

// func (g *ScheduleGroupImpl[T, R]) GetNodeTasks() map[node.ID]map[T]R {
// 	return g.nodeTasks
// }

// func GetGroupName(id GroupID) string {
// 	gt := GroupTpye(id >> 56)
// 	if gt == groupTable {
// 		return fmt.Sprintf("%s-%d", gt.String(), id&0x00FFFFFFFFFFFFFF)
// 	}
// 	return gt.String()
// }

// func (gt GroupTpye) Less(other GroupTpye) bool {
// 	return gt < other
// }

// func (gt GroupTpye) String() string {
// 	switch gt {
// 	case groupDefault:
// 		return "default"
// 	case groupTable:
// 		return "table"
// 	default:
// 		return "HotLevel" + strconv.Itoa(int(gt-groupHotLevel1))
// 	}
// }

// func getGroupID(gt GroupTpye, tableID int64) GroupID {
// 	// use high 8 bits to store the group type
// 	id := int64(gt) << 56
// 	if gt == groupTable {
// 		return id | tableID
// 	}
// 	return id
// }

// func getGroupType(id GroupID) GroupTpye {
// 	return GroupTpye(id >> 56)
// }
