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
)

type ReplicationID interface {
	comparable
	String() string
}

// Replication is the interface for the replication task, it should implement the GetNodeID method
type Replication[T ReplicationID] interface {
	comparable
	GetID() T
	GetNodeID() node.ID
	GetGroupID() GroupID
}

type ReplicationDB[T ReplicationID, R Replication[T]] interface {
	// global scheduler interface
	ScheduleGroup[T, R]
	GetImbalanceGroupNodeTask(nodes map[node.ID]*node.Info) (groups map[GroupID]map[node.ID]R, valid bool)
	// group scheduler interface
	GetGroups() []GroupID
	GetAbsentByGroup(groupID GroupID, batch int) []R
	GetSchedulingByGroup(groupID GroupID) []R
	GetReplicatingByGroup(groupID GroupID) []R
	GetTaskSizePerNodeByGroup(groupID GroupID) map[node.ID]int
}

type ScheduleGroup[T ReplicationID, R Replication[T]] interface {
	GetAbsentSize() int
	GetAbsent() []R
	GetSchedulingSize() int
	GetScheduling() []R
	GetReplicatingSize() int
	GetReplicating() []R

	GetTaskSizePerNode() map[node.ID]int
}
