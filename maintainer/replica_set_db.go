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

package maintainer

import (
	"sync"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type ReplicaSetDB struct {
	nodeTasks map[node.ID]map[common.DispatcherID]*ReplicaSet
	// group the tasks by schema id
	schemaTasks map[int64]map[common.DispatcherID]*ReplicaSet
	// tables
	tableTasks map[int64]map[common.DispatcherID]*ReplicaSet

	workingMap map[common.DispatcherID]*ReplicaSet
	absentMap  map[common.DispatcherID]*ReplicaSet

	lock         sync.RWMutex
	changefeedID string
}

func NewReplicaSetDB() *ReplicaSetDB {
	db := &ReplicaSetDB{
		nodeTasks:   make(map[node.ID]map[common.DispatcherID]*ReplicaSet),
		schemaTasks: make(map[int64]map[common.DispatcherID]*ReplicaSet),
		tableTasks:  make(map[int64]map[common.DispatcherID]*ReplicaSet),

		workingMap: make(map[common.DispatcherID]*ReplicaSet),
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
	return db.absentMap[id]
}

func (db *ReplicaSetDB) TaskSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return len(db.workingMap) + len(db.absentMap)
}

func (db *ReplicaSetDB) GetAbsentSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return len(db.absentMap)
}

func (db *ReplicaSetDB) GetWorking() []*ReplicaSet {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var working = make([]*ReplicaSet, 0, len(db.workingMap))
	for _, stm := range db.absentMap {
		working = append(working, stm)
	}
	return working
}

func (db *ReplicaSetDB) GetScheduleSate() ([]*ReplicaSet, map[node.ID]int) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var absent = make([]*ReplicaSet, 0, len(db.absentMap))
	for _, stm := range db.absentMap {
		absent = append(absent, stm)
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
	return !ok || len(tm) == 0
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

	db.workingMap[task.ID] = task
	nodeID := task.GetNodeID()
	db.updateNodeMap("", nodeID, task)
	db.addToSchemaAndTableMap(task)
}

func (db *ReplicaSetDB) RemoveReplicaSet(tasks ...*ReplicaSet) {
	db.lock.Lock()
	defer db.lock.Unlock()

	for _, task := range tasks {
		log.Info("remove replica set",
			zap.String("changefeed", db.changefeedID),
			zap.String("replicaSet", task.ID.String()))
		tableID := task.Span.TableID
		schemaID := task.GetSchemaID()

		delete(db.absentMap, task.ID)
		delete(db.workingMap, task.ID)
		delete(db.schemaTasks[schemaID], task.ID)
		delete(db.tableTasks[tableID], task.ID)
		if len(db.schemaTasks[schemaID]) == 0 {
			delete(db.schemaTasks, schemaID)
		}
		if len(db.tableTasks[tableID]) == 0 {
			delete(db.tableTasks, tableID)
		}
	}
}

func (db *ReplicaSetDB) MarkReplicaSetWorking(task *ReplicaSet) {
	db.lock.Lock()
	defer db.lock.Unlock()
	log.Info("marking replica set working",
		zap.String("changefeed", db.changefeedID),
		zap.String("replicaSet", task.ID.String()))

	delete(db.absentMap, task.ID)
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

	task.SetNodeID(new)
	delete(db.absentMap, task.ID)
	db.workingMap[task.ID] = task
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
		if !ok {
			delete(db.nodeTasks, old)
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
