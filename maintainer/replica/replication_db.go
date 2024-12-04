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
	// group the tasks by the schema id, and table id for fast access
	schemaTasks map[int64]map[common.DispatcherID]*SpanReplication
	tableTasks  map[int64]map[common.DispatcherID]*SpanReplication
	// task group is used for tracking scheduling status, the ddl dispatcher is
	// not included since it doesn't need to be scheduled
	taskGroups map[GroupID]*replicationTaskGroup

	ddlSpan *SpanReplication

	// LOCK protects the above maps
	lock sync.RWMutex

	hotSpans *hotSpans
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

	var tasks = make([]*SpanReplication, 0)
	addMapToList := func(m map[common.DispatcherID]*SpanReplication) {
		for _, stm := range m {
			tasks = append(tasks, stm)
		}
	}
	for _, g := range db.taskGroups {
		// we need to add the replicating and scheduling tasks to the list, and then reset the db
		addMapToList(g.replicating)
		addMapToList(g.scheduling)
	}

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

	sum := 0
	for _, g := range db.taskGroups {
		sum += len(g.absent)
	}
	return sum
}

// GetSchedulingSize returns the size of the absent map
func (db *ReplicationDB) GetSchedulingSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	sum := 0
	for _, g := range db.taskGroups {
		sum += len(g.scheduling)
	}
	return sum
}

// GetReplicatingSize returns the absent spans
func (db *ReplicationDB) GetReplicatingSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	sum := 0
	for _, g := range db.taskGroups {
		sum += len(g.replicating)
	}
	return sum
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

	var stms []*SpanReplication
	for _, g := range db.taskGroups {
		for _, value := range g.GetNodeTasks()[id] {
			stms = append(stms, value)
		}
	}
	if len(stms) == 0 {
		log.Info("node is not maintained by controller, ignore",
			zap.String("changefeed", db.changefeedID.Name()),
			zap.Stringer("node", id))
	}
	return stms
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

	sum := 0
	for _, g := range db.taskGroups {
		sum += g.GetTaskSizeByNodeID(id)
	}
	return sum
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
		new := NewReplicaSet(
			old.ChangefeedID,
			common.NewDispatcherID(),
			old.GetTsoClient(),
			old.GetSchemaID(),
			span, checkpointTs)
		news = append(news, new)
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
	db.allTasks[task.ID] = task
	db.addToSchemaAndTableMap(task)
	g := db.getOrCreateGroup(task)
	g.AddReplicatingSpan(task)
}

// AddAbsentReplicaSet adds the replica set to the absent map
func (db *ReplicationDB) AddAbsentReplicaSet(tasks ...*SpanReplication) {
	db.lock.Lock()
	defer db.lock.Unlock()

	db.addAbsentReplicaSetUnLock(tasks...)
}

// MarkSpanAbsent move the span to the absent status
func (db *ReplicationDB) MarkSpanAbsent(span *SpanReplication) {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.mustGetGroup(span.groupID).MarkSpanAbsent(span)
}

// MarkSpanScheduling move the span to the scheduling map
func (db *ReplicationDB) MarkSpanScheduling(span *SpanReplication) {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.mustGetGroup(span.groupID).MarkSpanScheduling(span)
}

// MarkSpanReplicating move the span to the replicating map
func (db *ReplicationDB) MarkSpanReplicating(span *SpanReplication) {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.mustGetGroup(span.groupID).MarkSpanReplicating(span)
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
	db.mustGetGroup(task.groupID).BindSpanToNode(old, new, task)
}

// addAbsentReplicaSetUnLock adds the replica set to the absent map
func (db *ReplicationDB) addAbsentReplicaSetUnLock(tasks ...*SpanReplication) {
	for _, task := range tasks {
		db.allTasks[task.ID] = task
		g := db.getOrCreateGroup(task)
		g.AddAbsentReplicaSet(task)
		db.addToSchemaAndTableMap(task)
	}
}

// removeSpanUnLock removes the replica set from the db without lock
func (db *ReplicationDB) removeSpanUnLock(spans ...*SpanReplication) {
	for _, span := range spans {
		g := db.mustGetGroup(span.groupID)
		g.RemoveSpan(span)
		db.maybeRemoveGroup(g)

		tableID := span.Span.TableID
		schemaID := span.GetSchemaID()
		delete(db.schemaTasks[schemaID], span.ID)
		delete(db.tableTasks[tableID], span.ID)
		if len(db.schemaTasks[schemaID]) == 0 {
			delete(db.schemaTasks, schemaID)
		}
		if len(db.tableTasks[tableID]) == 0 {
			delete(db.tableTasks, tableID)
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

// reset resets the maps of ReplicationDB
func (db *ReplicationDB) reset() {
	db.schemaTasks = make(map[int64]map[common.DispatcherID]*SpanReplication)
	db.tableTasks = make(map[int64]map[common.DispatcherID]*SpanReplication)
	db.allTasks = make(map[common.DispatcherID]*SpanReplication)
	db.taskGroups = make(map[GroupID]*replicationTaskGroup)
	db.taskGroups[defaultGroupID] = newReplicationTaskGroup(db.changefeedID, defaultGroupID)
	db.hotSpans = NewHotSpans()
}

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
