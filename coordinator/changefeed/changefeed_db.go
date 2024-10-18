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
	lock        sync.RWMutex
}

func NewChangefeedDB() *ChangefeedDB {
	db := &ChangefeedDB{
		changefeeds: make(map[model.ChangeFeedID]*Changefeed),
		nodeTasks:   make(map[node.ID]map[model.ChangeFeedID]*Changefeed),
		absent:      make(map[model.ChangeFeedID]*Changefeed),
		scheduling:  make(map[model.ChangeFeedID]*Changefeed),
		replicating: make(map[model.ChangeFeedID]*Changefeed),
	}
	return db
}

func (db *ChangefeedDB) GetAbsentSize() int {
	db.lock.RUnlock()
	defer db.lock.RUnlock()

	return len(db.absent)
}

func (db *ChangefeedDB) GetReplicatingSize() int {
	db.lock.RUnlock()
	defer db.lock.RUnlock()

	return len(db.replicating)
}

func (db *ChangefeedDB) GetReplicating() []*Changefeed {
	db.lock.RUnlock()
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

// MarkSpanAbsent move the span to the absent status
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
