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
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
)

func (db *ReplicationDB) GetGroups() []replica.GroupID {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.taskGroups.GetGroups()
}

func (db *ReplicationDB) GetAbsent() []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.taskGroups.GetAbsent()
}

func (db *ReplicationDB) GetAbsentByGroup(id replica.GroupID, maxSize int) []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.taskGroups.GetAbsentByGroup(id, maxSize)
}

func (db *ReplicationDB) GetSchedulingByGroup(id replica.GroupID) []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.taskGroups.GetSchedulingByGroup(id)
}

// GetReplicating returns the replicating spans
func (db *ReplicationDB) GetReplicating() []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.taskGroups.GetReplicating()
}

func (db *ReplicationDB) GetReplicatingByGroup(id replica.GroupID) []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.taskGroups.GetReplicatingByGroup(id)
}

func (db *ReplicationDB) GetScheduling() []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.taskGroups.GetScheduling()
}

func (db *ReplicationDB) GetImbalanceGroupNodeTask(nodes map[node.ID]*node.Info) (groups map[replica.GroupID]map[node.ID]*SpanReplication, valid bool) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.taskGroups.GetImbalanceGroupNodeTask(nodes)
}

func (db *ReplicationDB) GetTaskSizePerNode() map[node.ID]int {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.taskGroups.GetTaskSizePerNode()
}

func (db *ReplicationDB) GetTaskSizePerNodeByGroup(id replica.GroupID) map[node.ID]int {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.taskGroups.GetTaskSizePerNodeByGroup(id)
}

func (db *ReplicationDB) GetGroupStat() string {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.taskGroups.GetGroupStat()
}

// GetAbsentSize returns the size of the absent map
func (db *ReplicationDB) GetAbsentSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.taskGroups.GetAbsentSize()
}

// GetSchedulingSize returns the size of the absent map
func (db *ReplicationDB) GetSchedulingSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.taskGroups.GetSchedulingSize()
}

// GetReplicatingSize returns the absent spans
func (db *ReplicationDB) GetReplicatingSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.taskGroups.GetReplicatingSize()
}

// GetTaskByNodeID returns all the tasks that are maintained by the node
func (db *ReplicationDB) GetTaskByNodeID(id node.ID) []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.taskGroups.GetTaskByNodeID(id)
}

// GetTaskSizeByNodeID returns the size of the task by the node id
func (db *ReplicationDB) GetTaskSizeByNodeID(id node.ID) int {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.taskGroups.GetTaskSizeByNodeID(id)
}
