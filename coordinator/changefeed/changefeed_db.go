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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

// ChangefeedDB is an in memory data struct that maintains all changefeeds
type ChangefeedDB struct {
	changefeeds            map[common.ChangeFeedID]*Changefeed
	changefeedDisplayNames map[common.ChangeFeedDisplayName]common.ChangeFeedID

	nodeTasks   map[node.ID]map[common.ChangeFeedID]*Changefeed
	absent      map[common.ChangeFeedID]*Changefeed
	scheduling  map[common.ChangeFeedID]*Changefeed
	replicating map[common.ChangeFeedID]*Changefeed

	// stopped changefeeds that failed, stopped or finished
	stopped map[common.ChangeFeedID]*Changefeed
	lock    sync.RWMutex
}

func NewChangefeedDB() *ChangefeedDB {
	db := &ChangefeedDB{
		changefeeds:            make(map[common.ChangeFeedID]*Changefeed),
		changefeedDisplayNames: make(map[common.ChangeFeedDisplayName]common.ChangeFeedID),

		nodeTasks:   make(map[node.ID]map[common.ChangeFeedID]*Changefeed),
		absent:      make(map[common.ChangeFeedID]*Changefeed),
		scheduling:  make(map[common.ChangeFeedID]*Changefeed),
		replicating: make(map[common.ChangeFeedID]*Changefeed),
		stopped:     make(map[common.ChangeFeedID]*Changefeed),
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
	db.changefeedDisplayNames[task.ID.DisplayName] = task.ID
}

// AddReplicatingMaintainer adds a replicating the replicating map, that means the task is already scheduled to a maintainer
func (db *ChangefeedDB) AddReplicatingMaintainer(task *Changefeed, nodeID node.ID) {
	db.lock.Lock()
	defer db.lock.Unlock()

	task.setNodeID(nodeID)
	log.Info("add an replicating maintainer",
		zap.String("nodeID", nodeID.String()),
		zap.String("changefeed", task.ID.String()))

	db.changefeeds[task.ID] = task
	db.replicating[task.ID] = task
	db.changefeedDisplayNames[task.ID.DisplayName] = task.ID
	db.updateNodeMap("", nodeID, task)
}

// StopByChangefeedID stop a changefeed by the changefeed id
// if remove is true, it will remove the changefeed from the chagnefeed DB
// if remove is false, moves task to stopped map
// if the changefeed is scheduled, it will return the scheduled node
func (db *ChangefeedDB) StopByChangefeedID(cfID common.ChangeFeedID, remove bool) node.ID {
	db.lock.Lock()
	defer db.lock.Unlock()

	cf, ok := db.changefeeds[cfID]
	if ok {
		delete(db.changefeeds, cfID)
		db.removeChangefeedUnLock(cf)

		if !remove {
			// push back to stopped
			db.changefeeds[cfID] = cf
			db.stopped[cfID] = cf
		}
		log.Info("stop changefeed", zap.String("changefeed", cfID.String()))

		nodeID := cf.GetNodeID()
		if cf.GetNodeID() == "" {
			log.Info("changefeed is not scheduled, delete directly")
			return ""
		}
		cf.setNodeID("")
		return nodeID
	}
	return ""
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

// GetAllChangefeeds returns all changefeeds
func (db *ChangefeedDB) GetAllChangefeeds() []*Changefeed {
	db.lock.RLock()
	defer db.lock.RUnlock()

	cfs := make([]*Changefeed, 0, len(db.changefeeds))
	for _, cf := range db.changefeeds {
		cfs = append(cfs, cf)
	}
	return cfs
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

	task.setNodeID(new)
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

// GetWaitingSchedulingChangefeeds returns the absent maintainers and the working state of each node
func (db *ChangefeedDB) GetWaitingSchedulingChangefeeds(absent []*Changefeed, maxSize int) ([]*Changefeed, map[node.ID]int) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	size := 0
	for _, stm := range db.absent {
		if !stm.backoff.ShouldRun() {
			continue
		}
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

func (db *ChangefeedDB) GetByID(id common.ChangeFeedID) *Changefeed {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return db.changefeeds[id]
}

func (db *ChangefeedDB) GetByChangefeedDisplayName(displayName common.ChangeFeedDisplayName) *Changefeed {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return db.changefeeds[db.changefeedDisplayNames[displayName]]
}

// Resume moves a changefeed to the absent map, and waiting for scheduling
func (db *ChangefeedDB) Resume(id common.ChangeFeedID, resetBackoff bool) {
	db.lock.Lock()
	defer db.lock.Unlock()

	cf := db.changefeeds[id]
	if cf != nil {
		// force retry the changefeed,
		// it reset the backoff and set the state to normal, it's called when we resume a changefeed using cli
		if resetBackoff {
			cf.backoff.resetErrRetry()
		}
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
	cf.setNodeID("")
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
	db.lock.RLock()
	defer db.lock.RUnlock()

	var minCpts uint64 = math.MaxUint64

	for _, cf := range db.changefeeds {
		info := cf.GetInfo()
		if info == nil || !info.NeedBlockGC() {
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
func (db *ChangefeedDB) ReplaceStoppedChangefeed(cf *config.ChangeFeedInfo) {
	db.lock.Lock()
	defer db.lock.Unlock()

	oldCf := db.stopped[cf.ChangefeedID]
	if oldCf == nil {
		log.Warn("changefeed is not stopped, can not be updated", zap.String("changefeed", cf.ChangefeedID.String()))
		return
	}
	newCf := NewChangefeed(cf.ChangefeedID, cf, oldCf.GetStatus().CheckpointTs)
	db.stopped[cf.ChangefeedID] = newCf
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
			newMap = make(map[common.ChangeFeedID]*Changefeed)
			db.nodeTasks[new] = newMap
		}
		newMap[task.ID] = task
	}
}

// addAbsentChangefeedUnLock adds the replica set to the absent map
func (db *ChangefeedDB) addAbsentChangefeedUnLock(tasks ...*Changefeed) {
	for _, task := range tasks {
		db.changefeedDisplayNames[task.ID.DisplayName] = task.ID
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
