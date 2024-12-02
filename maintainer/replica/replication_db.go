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
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

// ReplicationDB is an in memory data struct that maintains the replication spans
type ReplicationDB struct {
	// for log ID
	changefeedID common.ChangeFeedID
	// allTasks maintains all the span tasks, it included the table trigger
	allTasks map[common.DispatcherID]*SpanReplication

	// group the tasks by the node id， schema id, and table id for fast access
	nodeTasks   map[node.ID]map[common.DispatcherID]*SpanReplication
	schemaTasks map[int64]map[common.DispatcherID]*SpanReplication
	tableTasks  map[int64]map[common.DispatcherID]*SpanReplication

	// maps that maintained base on the span scheduling status
	replicating map[common.DispatcherID]*SpanReplication
	scheduling  map[common.DispatcherID]*SpanReplication
	absent      map[common.DispatcherID]*SpanReplication

	ddlSpan *SpanReplication

	// LOCK protects the above maps
	lock sync.RWMutex
}

// NewReplicaSetDB creates a new ReplicationDB and initializes the maps
func NewReplicaSetDB(changefeedID common.ChangeFeedID, ddlSpan *SpanReplication) *ReplicationDB {
	db := &ReplicationDB{changefeedID: changefeedID, ddlSpan: ddlSpan}
	db.reset()
	db.putDDLDispatcher(db.ddlSpan)
	return db
}

// GetTaskByID returns the replica set by the id, it will search the replicating, scheduling and absent map
func (db *ReplicationDB) GetTaskByID(id common.DispatcherID) *SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return db.allTasks[id]
}

// TaskSize returns the total task size in the db, it includes replicating, scheduling and absent tasks
func (db *ReplicationDB) TaskSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	// the ddl span is a special span, we don't need to schedule it
	return len(db.allTasks)
}

// TryRemoveAll removes non-scheduled tasks from the db and return the scheduled tasks
func (db *ReplicationDB) TryRemoveAll() []*SpanReplication {
	db.lock.Lock()
	defer db.lock.Unlock()

	var tasks = make([]*SpanReplication, 0, len(db.replicating)+len(db.scheduling))
	addMapToList := func(m map[common.DispatcherID]*SpanReplication) {
		for _, stm := range m {
			tasks = append(tasks, stm)
		}
	}
	// we need to add the replicating and scheduling tasks to the list, and then reset the db
	addMapToList(db.replicating)
	addMapToList(db.scheduling)
	db.reset()
	db.putDDLDispatcher(db.ddlSpan)
	return tasks
}

// TryRemoveByTableIDs removes non-scheduled tasks from the db and return the scheduled tasks
func (db *ReplicationDB) TryRemoveByTableIDs(tableIDs ...int64) []*SpanReplication {
	db.lock.Lock()
	defer db.lock.Unlock()

	var tasks = make([]*SpanReplication, 0)
	for _, tblID := range tableIDs {
		for _, stm := range db.tableTasks[tblID] {
			db.removeSpanUnLock(stm)
			// the task is scheduled
			if stm.GetNodeID() != "" {
				tasks = append(tasks, stm)
			}
		}
	}
	return tasks
}

// TryRemoveBySchemaID removes non-scheduled tasks from the db and return the scheduled tasks
func (db *ReplicationDB) TryRemoveBySchemaID(schemaID int64) []*SpanReplication {
	db.lock.Lock()
	defer db.lock.Unlock()

	var tasks = make([]*SpanReplication, 0)
	for _, stm := range db.schemaTasks[schemaID] {
		db.removeSpanUnLock(stm)
		// the task is scheduled
		if stm.GetNodeID() != "" {
			tasks = append(tasks, stm)
		}
	}
	return tasks
}

// GetAbsentSize returns the size of the absent map
func (db *ReplicationDB) GetAbsentSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return len(db.absent)
}

// GetSchedulingSize returns the size of the absent map
func (db *ReplicationDB) GetSchedulingSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return len(db.scheduling)
}

// GetReplicatingSize returns the absent spans
func (db *ReplicationDB) GetReplicatingSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return len(db.replicating)
}

// GetReplicating returns the replicating spans
func (db *ReplicationDB) GetReplicating() []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var replicating = make([]*SpanReplication, 0, len(db.replicating))
	for _, stm := range db.replicating {
		replicating = append(replicating, stm)
	}
	return replicating
}

// GetAbsent returns the absent spans with the maxSize, push the spans to the buffer
func (db *ReplicationDB) GetAbsent(buffer []*SpanReplication, maxSize int) []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	size := 0
	for _, stm := range db.absent {
		buffer = append(buffer, stm)
		size++
		if size >= maxSize {
			break
		}
	}
	return buffer
}

// GetTasksByTableIDs returns the spans by the table ids
func (db *ReplicationDB) GetTasksByTableIDs(tableIDs ...int64) []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var stms []*SpanReplication
	for _, tableID := range tableIDs {
		for _, stm := range db.tableTasks[tableID] {
			stms = append(stms, stm)
		}
	}
	return stms
}

// GetAllTasks returns all the spans in the db, it's used when the block event type is all, it will return the ddl span
func (db *ReplicationDB) GetAllTasks() []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var stms = make([]*SpanReplication, 0, len(db.allTasks))
	for _, stm := range db.allTasks {
		stms = append(stms, stm)
	}
	return stms
}

// IsTableExists checks if the table exists in the db
func (db *ReplicationDB) IsTableExists(tableID int64) bool {
	db.lock.RLock()
	defer db.lock.RUnlock()

	tm, ok := db.tableTasks[tableID]
	return ok && len(tm) > 0
}

// GetTaskByNodeID returns all the tasks that are maintained by the node
func (db *ReplicationDB) GetTaskByNodeID(id node.ID) []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	stmMap, ok := db.nodeTasks[id]
	if !ok {
		log.Info("node is not maintained by controller, ignore",
			zap.String("changefeed", db.changefeedID.Name()),
			zap.Stringer("node", id))
		return nil
	}
	var stms = make([]*SpanReplication, 0, len(stmMap))
	for _, value := range stmMap {
		stms = append(stms, value)
	}
	return stms
}

// GetTaskSizePerNode returns the size of the task per node
func (db *ReplicationDB) GetTaskSizePerNode() map[node.ID]int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	sizeMap := make(map[node.ID]int, len(db.nodeTasks))
	for nodeID, stmMap := range db.nodeTasks {
		sizeMap[nodeID] = len(stmMap)
	}
	return sizeMap
}

// GetTaskSizeBySchemaID returns the size of the task by the schema id
func (db *ReplicationDB) GetTaskSizeBySchemaID(schemaID int64) int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	sm, ok := db.schemaTasks[schemaID]
	if ok {
		return len(sm)
	}
	return 0
}

// GetTasksBySchemaID returns the spans by the schema id
func (db *ReplicationDB) GetTasksBySchemaID(schemaID int64) []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	sm, ok := db.schemaTasks[schemaID]
	if !ok {
		return nil
	}
	var replicaSets = make([]*SpanReplication, 0, len(sm))
	for _, v := range sm {
		replicaSets = append(replicaSets, v)
	}
	return replicaSets
}

// GetTaskSizeByNodeID returns the size of the task by the node id
func (db *ReplicationDB) GetTaskSizeByNodeID(id node.ID) int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	sm, ok := db.nodeTasks[id]
	if ok {
		return len(sm)
	}
	return 0
}

// AddAbsentReplicaSet adds the replica set to the absent map
func (db *ReplicationDB) AddAbsentReplicaSet(tasks ...*SpanReplication) {
	db.lock.Lock()
	defer db.lock.Unlock()

	db.addAbsentReplicaSetUnLock(tasks...)
}

// ReplaceReplicaSet replaces the old replica set with the new spans
func (db *ReplicationDB) ReplaceReplicaSet(old *SpanReplication, newSpans []*heartbeatpb.TableSpan, checkpointTs uint64) bool {
	db.lock.Lock()
	defer db.lock.Unlock()

	// first check  the old replica set exists, if not, return false
	if _, ok := db.allTasks[old.ID]; !ok {
		log.Warn("old replica set not found, skip",
			zap.String("changefeed", db.changefeedID.Name()),
			zap.String("span", old.ID.String()))
		return false
	}

	var news []*SpanReplication
	for _, span := range newSpans {
		news = append(news,
			NewReplicaSet(
				old.ChangefeedID,
				common.NewDispatcherID(),
				old.GetTsoClient(),
				old.GetSchemaID(),
				span, checkpointTs))
	}

	// remove and insert the new replica set
	db.removeSpanUnLock(old)
	db.addAbsentReplicaSetUnLock(news...)
	return true
}

// AddReplicatingSpan adds a replicating the replicating map, that means the task is already scheduled to a dispatcher
func (db *ReplicationDB) AddReplicatingSpan(task *SpanReplication) {
	db.lock.Lock()
	defer db.lock.Unlock()
	nodeID := task.GetNodeID()

	log.Info("add an replicating span",
		zap.String("changefeed", db.changefeedID.Name()),
		zap.String("nodeID", nodeID.String()),
		zap.String("span", task.ID.String()))

	db.allTasks[task.ID] = task
	db.replicating[task.ID] = task
	db.updateNodeMap("", nodeID, task)
	db.addToSchemaAndTableMap(task)
}

// MarkSpanAbsent move the span to the absent status
func (db *ReplicationDB) MarkSpanAbsent(span *SpanReplication) {
	db.lock.Lock()
	defer db.lock.Unlock()

	log.Info("marking span absent",
		zap.String("changefeed", db.changefeedID.Name()),
		zap.String("span", span.ID.String()),
		zap.String("node", span.GetNodeID().String()))

	delete(db.scheduling, span.ID)
	delete(db.replicating, span.ID)
	db.absent[span.ID] = span
	originNodeID := span.GetNodeID()
	span.SetNodeID("")
	db.updateNodeMap(originNodeID, "", span)
}

// MarkSpanScheduling move the span to the scheduling map
func (db *ReplicationDB) MarkSpanScheduling(span *SpanReplication) {
	db.lock.Lock()
	defer db.lock.Unlock()

	log.Info("marking span scheduling",
		zap.String("changefeed", db.changefeedID.Name()),
		zap.String("span", span.ID.String()))

	delete(db.absent, span.ID)
	delete(db.replicating, span.ID)
	db.scheduling[span.ID] = span
}

// MarkSpanReplicating move the span to the replicating map
func (db *ReplicationDB) MarkSpanReplicating(span *SpanReplication) {
	db.lock.Lock()
	defer db.lock.Unlock()
	log.Info("marking span replicating",
		zap.String("changefeed", db.changefeedID.Name()),
		zap.String("span", span.ID.String()))

	delete(db.absent, span.ID)
	delete(db.scheduling, span.ID)
	db.replicating[span.ID] = span
}

// ForceRemove remove the span from the db
func (db *ReplicationDB) ForceRemove(id common.DispatcherID) {
	db.lock.Lock()
	defer db.lock.Unlock()
	span, ok := db.allTasks[id]
	if !ok {
		log.Warn("span not found, ignore remove action",
			zap.String("changefeed", db.changefeedID.Name()),
			zap.String("span", id.String()))
		return
	}

	log.Info("remove span",
		zap.String("changefeed", db.changefeedID.Name()),
		zap.String("span", id.String()))
	db.removeSpanUnLock(span)
}

// UpdateSchemaID will update the schema id of the table, and move the task to the new schema map
// it called when rename a table to another schema
func (db *ReplicationDB) UpdateSchemaID(tableID, newSchemaID int64) {
	db.lock.Lock()
	defer db.lock.Unlock()

	for _, replicaSet := range db.tableTasks[tableID] {
		oldSchemaID := replicaSet.GetSchemaID()
		// update schemaID
		replicaSet.SetSchemaID(newSchemaID)

		//update schema map
		schemaMap, ok := db.schemaTasks[oldSchemaID]
		if ok {
			delete(schemaMap, replicaSet.ID)
			//clear the map if empty
			if len(schemaMap) == 0 {
				delete(db.schemaTasks, oldSchemaID)
			}
		}
		// add it to new schema map
		newMap, ok := db.schemaTasks[newSchemaID]
		if !ok {
			newMap = make(map[common.DispatcherID]*SpanReplication)
			db.schemaTasks[newSchemaID] = newMap
		}
		newMap[replicaSet.ID] = replicaSet
	}
}

// BindSpanToNode binds the span to the node, it will remove the task from the old node and add it to the new node
// ,and it also marks the task as scheduling
func (db *ReplicationDB) BindSpanToNode(old, new node.ID, task *SpanReplication) {
	db.lock.Lock()
	defer db.lock.Unlock()

	log.Info("bind span to node",
		zap.String("changefeed", db.changefeedID.Name()),
		zap.String("span", task.ID.String()),
		zap.String("oldNode", old.String()),
		zap.String("node", new.String()))

	task.SetNodeID(new)
	delete(db.absent, task.ID)
	delete(db.replicating, task.ID)
	db.scheduling[task.ID] = task
	db.updateNodeMap(old, new, task)
}

// addAbsentReplicaSetUnLock adds the replica set to the absent map
func (db *ReplicationDB) addAbsentReplicaSetUnLock(tasks ...*SpanReplication) {
	for _, task := range tasks {
		db.allTasks[task.ID] = task
		db.absent[task.ID] = task
		db.addToSchemaAndTableMap(task)
	}
}

// removeSpanUnLock removes the replica set from the db without lock
func (db *ReplicationDB) removeSpanUnLock(spans ...*SpanReplication) {
	for _, span := range spans {
		log.Info("remove span",
			zap.String("changefeed", db.changefeedID.Name()),
			zap.Int64("table", span.Span.TableID),
			zap.String("span", span.ID.String()))
		tableID := span.Span.TableID
		schemaID := span.GetSchemaID()
		nodeID := span.GetNodeID()

		delete(db.absent, span.ID)
		delete(db.scheduling, span.ID)
		delete(db.replicating, span.ID)
		delete(db.schemaTasks[schemaID], span.ID)
		delete(db.tableTasks[tableID], span.ID)
		if len(db.schemaTasks[schemaID]) == 0 {
			delete(db.schemaTasks, schemaID)
		}
		if len(db.tableTasks[tableID]) == 0 {
			delete(db.tableTasks, tableID)
		}
		nodeMap := db.nodeTasks[nodeID]
		delete(nodeMap, span.ID)
		if len(nodeMap) == 0 {
			delete(db.nodeTasks, nodeID)
		}
		delete(db.allTasks, span.ID)
	}
}

// addToSchemaAndTableMap adds the task to the schema and table map
func (db *ReplicationDB) addToSchemaAndTableMap(task *SpanReplication) {
	tableID := task.Span.TableID
	schemaID := task.GetSchemaID()
	// modify the schema map
	schemaMap, ok := db.schemaTasks[schemaID]
	if !ok {
		schemaMap = make(map[common.DispatcherID]*SpanReplication)
		db.schemaTasks[schemaID] = schemaMap
	}
	schemaMap[task.ID] = task

	// modify the table map
	tableMap, ok := db.tableTasks[tableID]
	if !ok {
		tableMap = make(map[common.DispatcherID]*SpanReplication)
		db.tableTasks[tableID] = tableMap
	}
	tableMap[task.ID] = task
}

// updateNodeMap updates the node map, it will remove the task from the old node and add it to the new node
func (db *ReplicationDB) updateNodeMap(old, new node.ID, task *SpanReplication) {
	//clear from the old node
	if old != "" {
		oldMap, ok := db.nodeTasks[old]
		if ok {
			delete(oldMap, task.ID)
			if len(oldMap) == 0 {
				delete(db.nodeTasks, old)
			}
		}
	}
	// add to the new node if the new node is not empty
	if new != "" {
		newMap, ok := db.nodeTasks[new]
		if !ok {
			newMap = make(map[common.DispatcherID]*SpanReplication)
			db.nodeTasks[new] = newMap
		}
		newMap[task.ID] = task
	}
}

// reset resets the maps of ReplicationDB
func (db *ReplicationDB) reset() {
	db.nodeTasks = make(map[node.ID]map[common.DispatcherID]*SpanReplication)
	db.schemaTasks = make(map[int64]map[common.DispatcherID]*SpanReplication)
	db.tableTasks = make(map[int64]map[common.DispatcherID]*SpanReplication)
	db.allTasks = make(map[common.DispatcherID]*SpanReplication)
	db.replicating = make(map[common.DispatcherID]*SpanReplication)
	db.scheduling = make(map[common.DispatcherID]*SpanReplication)
	db.absent = make(map[common.DispatcherID]*SpanReplication)
}

// putDDLDispatcher
func (db *ReplicationDB) putDDLDispatcher(ddlSpan *SpanReplication) {
	// we don't need to schedule the ddl span, but added it to the allTasks map, so we can query it by id
	db.allTasks[ddlSpan.ID] = ddlSpan
	// dispatcher will report a block event with table ID 0,
	// so we need to add it to the table map
	db.tableTasks[ddlSpan.Span.TableID] = map[common.DispatcherID]*SpanReplication{
		ddlSpan.ID: ddlSpan,
	}
	// also put it to the schema map
	db.schemaTasks[ddlSpan.schemaID] = map[common.DispatcherID]*SpanReplication{
		ddlSpan.ID: ddlSpan,
	}
}
