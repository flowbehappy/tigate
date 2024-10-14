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

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type ReplicaSetDB struct {
	changefeedID string
	nodeTasks    map[node.ID]map[common.DispatcherID]*ReplicaSet
	// group the tasks by schema id
	schemaTasks map[int64]map[common.DispatcherID]*ReplicaSet
	// group the tasks by table id
	tableTasks map[int64]map[common.DispatcherID]*ReplicaSet

	// maps that maintained base on the task scheduling status
	workingMap map[common.DispatcherID]*ReplicaSet
	scheduling map[common.DispatcherID]*ReplicaSet
	absentMap  map[common.DispatcherID]*ReplicaSet

	lock sync.RWMutex
}

func NewReplicaSetDB(changefeedID string) *ReplicaSetDB {
	db := &ReplicaSetDB{
		changefeedID: changefeedID,
		nodeTasks:    make(map[node.ID]map[common.DispatcherID]*ReplicaSet),
		schemaTasks:  make(map[int64]map[common.DispatcherID]*ReplicaSet),
		tableTasks:   make(map[int64]map[common.DispatcherID]*ReplicaSet),

		workingMap: make(map[common.DispatcherID]*ReplicaSet),
		scheduling: make(map[common.DispatcherID]*ReplicaSet),
		absentMap:  make(map[common.DispatcherID]*ReplicaSet),
	}
	return db
}

func (db *ReplicaSetDB) GetTaskByID(id common.DispatcherID) *ReplicaSet {
	db.lock.RLock()
	defer db.lock.RUnlock()

	r, ok := db.workingMap[id]
	if ok {
		return r
	}
	r, ok = db.scheduling[id]
	if ok {
		return r
	}
	return db.absentMap[id]
}

func (db *ReplicaSetDB) TaskSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return len(db.workingMap) + len(db.absentMap) + len(db.scheduling)
}

// TryRemoveAll removes non-scheduled tasks from the db and return the scheduled tasks
func (db *ReplicaSetDB) TryRemoveAll() []*ReplicaSet {
	db.lock.Lock()
	defer db.lock.Unlock()

	for _, stm := range db.absentMap {
		db.removeReplicaSetUnLock(stm)
	}
	var tasks = make([]*ReplicaSet, 0, len(db.workingMap)+len(db.absentMap)+len(db.scheduling))
	addMapToList := func(m map[common.DispatcherID]*ReplicaSet) {
		for _, stm := range m {
			tasks = append(tasks, stm)
		}
	}
	addMapToList(db.workingMap)
	addMapToList(db.scheduling)
	return tasks
}

// TryRemoveByTableIDs removes non-scheduled tasks from the db and return the scheduled tasks
func (db *ReplicaSetDB) TryRemoveByTableIDs(tableIDs ...int64) []*ReplicaSet {
	db.lock.Lock()
	defer db.lock.Unlock()

	var tasks = make([]*ReplicaSet, 0)
	for _, tblID := range tableIDs {
		for _, stm := range db.tableTasks[tblID] {
			if stm.GetNodeID() == "" {
				db.removeReplicaSetUnLock(stm)
			} else {
				tasks = append(tasks, stm)
			}
		}
	}
	return tasks
}

// TryRemoveBySchemaID removes non-scheduled tasks from the db and return the scheduled tasks
func (db *ReplicaSetDB) TryRemoveBySchemaID(schemaID int64) []*ReplicaSet {
	db.lock.Lock()
	defer db.lock.Unlock()

	var tasks = make([]*ReplicaSet, 0)
	for _, stm := range db.schemaTasks[schemaID] {
		if stm.GetNodeID() == "" {
			db.removeReplicaSetUnLock(stm)
		} else {
			tasks = append(tasks, stm)
		}
	}
	return tasks
}

func (db *ReplicaSetDB) GetAbsentSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return len(db.absentMap)
}

func (db *ReplicaSetDB) GetWorkingSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return len(db.workingMap)
}

func (db *ReplicaSetDB) GetWorking() []*ReplicaSet {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var working = make([]*ReplicaSet, 0, len(db.workingMap))
	for _, stm := range db.workingMap {
		working = append(working, stm)
	}
	return working
}

func (db *ReplicaSetDB) GetScheduleSate(absent []*ReplicaSet, maxSize int) ([]*ReplicaSet, map[node.ID]int) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	size := 0
	for _, stm := range db.absentMap {
		absent = append(absent, stm)
		size++
		if size >= maxSize {
			break
		}
	}
	var workingState = make(map[node.ID]int, len(db.nodeTasks))
	for nodeID, stmMap := range db.nodeTasks {
		workingState[nodeID] = len(stmMap)
	}
	return absent, workingState
}

func (db *ReplicaSetDB) GetTasksByTableIDs(tableIDs ...int64) []*ReplicaSet {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var stms []*ReplicaSet
	for _, tableID := range tableIDs {
		for _, stm := range db.tableTasks[tableID] {
			stms = append(stms, stm)
		}
	}
	return stms
}

func (db *ReplicaSetDB) IsTableExists(tableID int64) bool {
	db.lock.RLock()
	defer db.lock.RUnlock()

	tm, ok := db.tableTasks[tableID]
	return ok && len(tm) > 0
}

func (db *ReplicaSetDB) RemoveNode(id node.ID) {
	db.lock.Lock()
	defer db.lock.Unlock()

	stmMap, ok := db.nodeTasks[id]
	if !ok {
		log.Info("node is not maintained by controller, ignore",
			zap.String("changefeed", db.changefeedID),
			zap.Stringer("node", id))
		return
	}
	// move to absent node
	for key, value := range stmMap {
		log.Info("move replica set to absent",
			zap.String("changefeed", db.changefeedID),
			zap.String("node", id.String()),
			zap.String("replicaSet", key.String()))
		db.absentMap[key] = value
		value.SetNodeID("")
	}
	delete(db.nodeTasks, id)
}

func (db *ReplicaSetDB) GetTaskSizeBySchemaID(schemaID int64) int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	sm, ok := db.schemaTasks[schemaID]
	if ok {
		return len(sm)
	}
	return 0
}

func (db *ReplicaSetDB) GetTasksBySchemaID(schemaID int64) []*ReplicaSet {
	db.lock.RLock()
	defer db.lock.RUnlock()

	sm, ok := db.schemaTasks[schemaID]
	if !ok {
		return nil
	}
	var replicaSets []*ReplicaSet = make([]*ReplicaSet, 0, len(sm))
	for _, v := range sm {
		replicaSets = append(replicaSets, v)
	}
	return replicaSets
}

func (db *ReplicaSetDB) GetTaskSizeByNodeID(id node.ID) int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	sm, ok := db.nodeTasks[id]
	if ok {
		return len(sm)
	}
	return 0
}

func (db *ReplicaSetDB) AddAbsentReplicaSet(tasks ...*ReplicaSet) {
	db.lock.Lock()
	defer db.lock.Unlock()

	for _, task := range tasks {
		db.absentMap[task.ID] = task
		db.addToSchemaAndTableMap(task)
	}
}

func (db *ReplicaSetDB) AddWorkingSpan(task *ReplicaSet) {
	db.lock.Lock()
	defer db.lock.Unlock()
	nodeID := task.GetNodeID()

	log.Info("add an working replica set",
		zap.String("changefeed", db.changefeedID),
		zap.String("nodeID", nodeID.String()),
		zap.String("replicaSet", task.ID.String()))

	db.workingMap[task.ID] = task
	db.updateNodeMap("", nodeID, task)
	db.addToSchemaAndTableMap(task)
}

func (db *ReplicaSetDB) RemoveReplicaSet(tasks ...*ReplicaSet) {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.removeReplicaSetUnLock(tasks...)
}

func (db *ReplicaSetDB) removeReplicaSetUnLock(tasks ...*ReplicaSet) {
	for _, task := range tasks {
		log.Info("remove replica set",
			zap.String("changefeed", db.changefeedID),
			zap.Int64("table", task.Span.TableID),
			zap.String("replicaSet", task.ID.String()))
		tableID := task.Span.TableID
		schemaID := task.GetSchemaID()
		nodeID := task.GetNodeID()

		delete(db.absentMap, task.ID)
		delete(db.scheduling, task.ID)
		delete(db.workingMap, task.ID)
		delete(db.schemaTasks[schemaID], task.ID)
		delete(db.tableTasks[tableID], task.ID)
		if len(db.schemaTasks[schemaID]) == 0 {
			delete(db.schemaTasks, schemaID)
		}
		if len(db.tableTasks[tableID]) == 0 {
			delete(db.tableTasks, tableID)
		}
		nodeMap := db.nodeTasks[nodeID]
		delete(nodeMap, task.ID)
		if len(nodeMap) == 0 {
			delete(db.nodeTasks, nodeID)
		}
	}
}

func (db *ReplicaSetDB) MarkReplicaSetScheduling(task *ReplicaSet) {
	db.lock.Lock()
	defer db.lock.Unlock()

	log.Info("marking replica set scheduling",
		zap.String("changefeed", db.changefeedID),
		zap.String("replicaSet", task.ID.String()))

	delete(db.absentMap, task.ID)
	delete(db.workingMap, task.ID)
	db.scheduling[task.ID] = task
}

func (db *ReplicaSetDB) MarkReplicaSetWorking(task *ReplicaSet) {
	db.lock.Lock()
	defer db.lock.Unlock()
	log.Info("marking replica set working",
		zap.String("changefeed", db.changefeedID),
		zap.String("replicaSet", task.ID.String()))

	delete(db.absentMap, task.ID)
	delete(db.scheduling, task.ID)
	db.workingMap[task.ID] = task
}

// UpdateSchemaID will update the schema id of the table, and move the task to the new schema map
// it called when rename a table to another schema
func (db *ReplicaSetDB) UpdateSchemaID(tableID, newSchemaID int64) {
	db.lock.Lock()
	defer db.lock.Unlock()

	for _, replicaSet := range db.tableTasks[tableID] {
		oldSchemaID := replicaSet.SchemaID
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
			newMap = make(map[common.DispatcherID]*ReplicaSet)
			db.schemaTasks[newSchemaID] = newMap
		}
		newMap[replicaSet.ID] = replicaSet
	}
}

func (db *ReplicaSetDB) BindReplicaSetToNode(old, new node.ID, task *ReplicaSet) {
	db.lock.Lock()
	defer db.lock.Unlock()
	log.Info("bind replica set to node",
		zap.String("changefeed", db.changefeedID),
		zap.String("replicaSet", task.ID.String()),
		zap.String("oldNode", old.String()),
		zap.String("node", new.String()))

	task.SetNodeID(new)
	delete(db.absentMap, task.ID)
	delete(db.workingMap, task.ID)
	db.scheduling[task.ID] = task
	db.updateNodeMap(old, new, task)
}

func (db *ReplicaSetDB) addToSchemaAndTableMap(task *ReplicaSet) {
	tableID := task.Span.TableID
	schemaID := task.GetSchemaID()
	// modify the schema map
	schemaMap, ok := db.schemaTasks[schemaID]
	if !ok {
		schemaMap = make(map[common.DispatcherID]*ReplicaSet)
		db.schemaTasks[schemaID] = schemaMap
	}
	schemaMap[task.ID] = task

	// modify the table map
	tableMap, ok := db.tableTasks[tableID]
	if !ok {
		tableMap = make(map[common.DispatcherID]*ReplicaSet)
		db.tableTasks[tableID] = tableMap
	}
	tableMap[task.ID] = task
}

func (db *ReplicaSetDB) updateNodeMap(old, new node.ID, task *ReplicaSet) {
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
			newMap = make(map[common.DispatcherID]*ReplicaSet)
			db.nodeTasks[new] = newMap
		}
		newMap[task.ID] = task
	}
}
