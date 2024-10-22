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

package changefeed

import (
	"math"
	"sync"

	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// ChangefeedDB is an in memory data struct that maintains all changefeeds
type ChangefeedDB struct {
	changefeeds map[model.ChangeFeedID]*Changefeed

	nodeTasks   map[node.ID]map[model.ChangeFeedID]*Changefeed
	absent      map[model.ChangeFeedID]*Changefeed
	scheduling  map[model.ChangeFeedID]*Changefeed
	replicating map[model.ChangeFeedID]*Changefeed

	// stopped changefeeds that failed, stopped or finished
	stopped map[model.ChangeFeedID]*Changefeed
	lock    sync.RWMutex
}

func NewChangefeedDB() *ChangefeedDB {
	db := &ChangefeedDB{
		changefeeds: make(map[model.ChangeFeedID]*Changefeed),

		nodeTasks:   make(map[node.ID]map[model.ChangeFeedID]*Changefeed),
		absent:      make(map[model.ChangeFeedID]*Changefeed),
		scheduling:  make(map[model.ChangeFeedID]*Changefeed),
		replicating: make(map[model.ChangeFeedID]*Changefeed),
		stopped:     make(map[model.ChangeFeedID]*Changefeed),
	}
	return db
}

// AddAbsentChangefeed adds the changefeed to the absent map
func (db *ChangefeedDB) AddAbsentChangefeed(tasks ...*Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()

	db.addAbsentChangefeedUnLock(tasks...)
}

// AddStoppedChangefeed adds the changefeed to the stop map
func (db *ChangefeedDB) AddStoppedChangefeed(task *Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()

	db.changefeeds[task.ID] = task
	db.stopped[task.ID] = task
}

// AddReplicatingMaintainer adds a replicating the replicating map, that means the task is already scheduled to a maintainer
func (db *ChangefeedDB) AddReplicatingMaintainer(task *Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()

	nodeID := task.GetNodeID()

	log.Info("add an replicating maintainer",
		zap.String("nodeID", nodeID.String()),
		zap.String("changefeed", task.ID.String()))

	db.changefeeds[task.ID] = task
	db.replicating[task.ID] = task
	db.updateNodeMap("", nodeID, task)
}

// RemoveByChangefeedID removes task from the db, if the changefeed is scheduled, it will return the task
func (db *ChangefeedDB) RemoveByChangefeedID(cfID model.ChangeFeedID) *Changefeed {
	db.lock.Lock()
	defer db.lock.Unlock()

	cf, ok := db.changefeeds[cfID]
	if ok {
		delete(db.changefeeds, cfID)
		db.removeChangefeedUnLock(cf)
		if cf.GetNodeID() == "" {
			log.Info("changefeed is not scheduled, delete directly")
			return nil
		}
		return cf
	}
	return nil
}

// StopByChangefeedID moves task from to stopped map, if the changefeed is scheduled, it will return the task
func (db *ChangefeedDB) StopByChangefeedID(cfID model.ChangeFeedID) *Changefeed {
	db.lock.Lock()
	defer db.lock.Unlock()

	cf, ok := db.changefeeds[cfID]
	if ok {
		delete(db.changefeeds, cfID)
		db.removeChangefeedUnLock(cf)
		// push bash to stopped
		db.changefeeds[cfID] = cf
		db.stopped[cfID] = cf

		if cf.GetNodeID() == "" {
			log.Info("changefeed is not scheduled, delete directly")
		}
		return cf
	}
	return nil
}

// GetSize returns the size of the all chagnefeeds
func (db *ChangefeedDB) GetSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return len(db.changefeeds)
}

// GetSchedulingSize returns the size of the schedulling changefeeds
func (db *ChangefeedDB) GetSchedulingSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return len(db.scheduling)
}

// GetStoppedSize returns the size of the stopped changefeeds
func (db *ChangefeedDB) GetStoppedSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return len(db.stopped)
}

func (db *ChangefeedDB) GetAbsentSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return len(db.absent)
}

func (db *ChangefeedDB) GetReplicatingSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return len(db.replicating)
}

func (db *ChangefeedDB) GetReplicating() []*Changefeed {
	db.lock.RLock()
	defer db.lock.RUnlock()

	cfs := make([]*Changefeed, 0, len(db.replicating))
	for _, cf := range db.replicating {
		cfs = append(cfs, cf)
	}
	return cfs
}

// GetTaskSizePerNode returns the size of the task per node
func (db *ChangefeedDB) GetTaskSizePerNode() map[node.ID]int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	sizeMap := make(map[node.ID]int, len(db.nodeTasks))
	for nodeID, stmMap := range db.nodeTasks {
		sizeMap[nodeID] = len(stmMap)
	}
	return sizeMap
}

// BindChangefeedToNode binds the changefeed to the node, it will remove the task from the old node and add it to the new node
// ,and it also marks the task as scheduling
func (db *ChangefeedDB) BindChangefeedToNode(old, new node.ID, task *Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()
	log.Info("bind changefeed to node",
		zap.String("changefeed", task.ID.String()),
		zap.String("oldNode", old.String()),
		zap.String("node", new.String()))

	task.SetNodeID(new)
	delete(db.absent, task.ID)
	delete(db.replicating, task.ID)
	db.scheduling[task.ID] = task
	db.updateNodeMap(old, new, task)
}

// MarkMaintainerReplicating move the maintainer to the replicating map
func (db *ChangefeedDB) MarkMaintainerReplicating(task *Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()

	log.Info("marking changefeed replicating",
		zap.String("changefeed", task.ID.String()))

	delete(db.absent, task.ID)
	delete(db.scheduling, task.ID)
	db.replicating[task.ID] = task
}

// GetScheduleSate returns the absent maintainers and the working state of each node
func (db *ChangefeedDB) GetScheduleSate(absent []*Changefeed, maxSize int) ([]*Changefeed, map[node.ID]int) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	size := 0
	for _, stm := range db.absent {
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

func (db *ChangefeedDB) GetByID(id model.ChangeFeedID) *Changefeed {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return db.changefeeds[id]
}

// Resume moves a changefeed to the absent map, and waiting for scheduling
func (db *ChangefeedDB) Resume(id model.ChangeFeedID) {
	db.lock.Lock()
	defer db.lock.Unlock()

	cf := db.changefeeds[id]
	if cf != nil {
		delete(db.stopped, id)
		db.absent[id] = cf
		log.Info("resume changefeed", zap.String("changefeed", id.String()))
	}
}

func (db *ChangefeedDB) GetByNodeID(id node.ID) []*Changefeed {
	db.lock.RLock()
	defer db.lock.RUnlock()

	stmMap, ok := db.nodeTasks[id]
	if !ok {
		log.Info("node is not maintained by coordinator, ignore",
			zap.Stringer("node", id))
		return nil
	}
	var stms = make([]*Changefeed, 0, len(stmMap))
	for _, value := range stmMap {
		stms = append(stms, value)
	}
	return stms
}

// MarkMaintainerAbsent move the maintainer to the absent Status
func (db *ChangefeedDB) MarkMaintainerAbsent(cf *Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()

	log.Info("marking changefeed absent",
		zap.String("changefeed", cf.ID.String()),
		zap.String("node", cf.GetNodeID().String()))

	delete(db.scheduling, cf.ID)
	delete(db.replicating, cf.ID)
	db.absent[cf.ID] = cf
	originNodeID := cf.GetNodeID()
	cf.SetNodeID("")
	db.updateNodeMap(originNodeID, "", cf)
}

// MarkMaintainerScheduling move the maintainer to the scheduling map
func (db *ChangefeedDB) MarkMaintainerScheduling(cf *Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()

	log.Info("marking changefeed scheduling",
		zap.String("ChangefeedDB", cf.ID.String()))

	delete(db.absent, cf.ID)
	delete(db.replicating, cf.ID)
	db.scheduling[cf.ID] = cf
}

// CalculateGCSafepoint calculates the minimum checkpointTs of all changefeeds that replicating the upstream TiDB cluster.
func (db *ChangefeedDB) CalculateGCSafepoint() uint64 {
	var minCpts uint64 = math.MaxUint64

	for _, cf := range db.changefeeds {
		if cf.Info == nil || !cf.Info.NeedBlockGC() {
			continue
		}
		checkpointTs := cf.GetLastSavedCheckPointTs()
		if minCpts > checkpointTs {
			minCpts = checkpointTs
		}
	}
	return minCpts
}

// ReplaceStoppedChangefeed updates the stopped changefeed
func (db *ChangefeedDB) ReplaceStoppedChangefeed(cf *model.ChangeFeedInfo) {
	db.lock.Lock()
	defer db.lock.Unlock()

	id := model.ChangeFeedID{
		Namespace: cf.Namespace,
		ID:        cf.ID,
	}
	oldCf := db.stopped[id]
	if oldCf == nil {
		log.Warn("changefeed is not stopped, can not be updated", zap.String("changefeed", id.String()))
		return
	}
	newCf := NewChangefeed(id, cf, oldCf.GetLastSavedCheckPointTs())
	db.stopped[id] = newCf
}

// updateNodeMap updates the node map, it will remove the task from the old node and add it to the new node
func (db *ChangefeedDB) updateNodeMap(old, new node.ID, task *Changefeed) {
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
			newMap = make(map[model.ChangeFeedID]*Changefeed)
			db.nodeTasks[new] = newMap
		}
		newMap[task.ID] = task
	}
}

// addAbsentChangefeedUnLock adds the replica set to the absent map
func (db *ChangefeedDB) addAbsentChangefeedUnLock(tasks ...*Changefeed) {
	for _, task := range tasks {
		db.changefeeds[task.ID] = task
		db.absent[task.ID] = task
	}
}

// removeChangefeedUnLock removes the changefeed from the db without lock
func (db *ChangefeedDB) removeChangefeedUnLock(cf *Changefeed) {
	log.Info("remove changefeed",
		zap.String("changefeed", cf.ID.String()))
	nodeID := cf.GetNodeID()

	delete(db.absent, cf.ID)
	delete(db.scheduling, cf.ID)
	delete(db.replicating, cf.ID)
	delete(db.stopped, cf.ID)
	nodeMap := db.nodeTasks[nodeID]
	delete(nodeMap, cf.ID)
	if len(nodeMap) == 0 {
		delete(db.nodeTasks, nodeID)
	}
	delete(db.changefeeds, cf.ID)
}
