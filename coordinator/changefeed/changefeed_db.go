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
	"sync"

	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

type ChangefeedDB struct {
	changefeeds map[model.ChangeFeedID]*Changefeed

	nodeTasks   map[node.ID]map[model.ChangeFeedID]*Changefeed
	absent      map[model.ChangeFeedID]*Changefeed
	scheduling  map[model.ChangeFeedID]*Changefeed
	replicating map[model.ChangeFeedID]*Changefeed

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

// AddReplicatingSpan adds a replicating the replicating map, that means the task is already scheduled to a dispatcher
func (db *ChangefeedDB) AddReplicatingSpan(task *Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()

	nodeID := task.GetNodeID()

	log.Info("add an replicating span",
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

// BindSpanToNode binds the span to the node, it will remove the task from the old node and add it to the new node
// ,and it also marks the task as scheduling
func (db *ChangefeedDB) BindSpanToNode(old, new node.ID, task *Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()
	log.Info("bind span to node",
		zap.String("changefeed", task.ID.String()),
		zap.String("oldNode", old.String()),
		zap.String("node", new.String()))

	task.SetNodeID(new)
	delete(db.absent, task.ID)
	delete(db.replicating, task.ID)
	db.scheduling[task.ID] = task
	db.updateNodeMap(old, new, task)
}

// MarkSpanReplicating move the span to the replicating map
func (db *ChangefeedDB) MarkSpanReplicating(task *Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()

	log.Info("marking changefeed replicating",
		zap.String("changefeed", task.ID.String()))

	delete(db.absent, task.ID)
	delete(db.scheduling, task.ID)
	db.replicating[task.ID] = task
}

// GetScheduleSate returns the absent spans and the working state of each node
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

// MarkSpanAbsent move the span to the absent Status
func (db *ChangefeedDB) MarkSpanAbsent(span *Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()

	log.Info("marking span absent",
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
func (db *ChangefeedDB) MarkSpanScheduling(span *Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()

	log.Info("marking changefeed scheduling",
		zap.String("ChangefeedDB", span.ID.String()))

	delete(db.absent, span.ID)
	delete(db.replicating, span.ID)
	db.scheduling[span.ID] = span
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

// removeSpanUnLock removes the changefeed from the db without lock
func (db *ChangefeedDB) removeChangefeedUnLock(cf *Changefeed) {
	log.Info("remove changefeed",
		zap.String("changefeed", cf.ID.String()))
	nodeID := cf.GetNodeID()

	delete(db.absent, cf.ID)
	delete(db.scheduling, cf.ID)
	delete(db.replicating, cf.ID)
	nodeMap := db.nodeTasks[nodeID]
	delete(nodeMap, cf.ID)
	if len(nodeMap) == 0 {
		delete(db.nodeTasks, nodeID)
	}
	delete(db.changefeeds, cf.ID)
}
