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
	"fmt"
	"strconv"

	"github.com/pingcap/ticdc/pkg/node"
)

// Replication is the interface for the replication task, it should implement the GetNodeID method
type Replication interface {
	comparable
	GetNodeID() node.ID
}

type ReplicationDB[R Replication] interface {
	// global scheduler interface
	ScheduleGroup[R]
	GetImbalanceGroupNodeTask(nodes map[node.ID]*node.Info) (groups map[GroupID]map[node.ID]R, valid bool)
	// group scheduler interface
	GetGroups() []GroupID
	GetAbsentByGroup(groupID GroupID, batch int) []R
	GetSchedulingByGroup(groupID GroupID) []R
	GetReplicatingByGroup(groupID GroupID) []R
	GetTaskSizePerNodeByGroup(groupID GroupID) map[node.ID]int
}

type ScheduleGroup[R Replication] interface {
	GetAbsentSize() int
	GetAbsent() []R
	GetSchedulingSize() int
	GetScheduling() []R
	GetReplicatingSize() int
	GetReplicating() []R

	GetTaskSizePerNode() map[node.ID]int
}

type groupTpye int8

const (
	groupDefault groupTpye = iota
	groupTable
	groupHotLevel1
)

func (gt groupTpye) Less(other groupTpye) bool {
	return gt < other
}

func (gt groupTpye) String() string {
	switch gt {
	case groupDefault:
		return "default"
	case groupTable:
		return "table"
	default:
		return "HotLevel" + strconv.Itoa(int(gt-groupHotLevel1))
	}
}

type GroupID = int64

const defaultGroupID GroupID = 0

func getGroupID(gt groupTpye, tableID int64) GroupID {
	// use high 8 bits to store the group type
	id := int64(gt) << 56
	if gt == groupTable {
		return id | tableID
	}
	return id
}

func getGroupType(id GroupID) groupTpye {
	return groupTpye(id >> 56)
}

func printGroupID(id GroupID) string {
	gt := groupTpye(id >> 56)
	if gt == groupTable {
		return fmt.Sprintf("%s-%d", gt.String(), id&0x00FFFFFFFFFFFFFF)
	}
	return gt.String()
}
