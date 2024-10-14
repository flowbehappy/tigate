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

package operator

import (
	"fmt"
	"sync/atomic"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/maintainer/replica"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
)

type SplitDispatcherOperator struct {
	replicaSet   *replica.ReplicaSet
	originNode   node.ID
	schemaID     int64
	splitSpans   []*replica.ReplicaSet
	checkpointTs uint64
	finished     atomic.Bool
	removing     atomic.Bool

	originalReplicaseRemoved atomic.Bool
	db                       *replica.ReplicaSetDB
}

func (m *SplitDispatcherOperator) OnNodeRemove(n node.ID) {

}

func (m *SplitDispatcherOperator) ID() common.DispatcherID {
	return m.replicaSet.ID
}

func (m *SplitDispatcherOperator) IsFinished() bool {
	return m.finished.Load() || m.originalReplicaseRemoved.Load()
}

func (m *SplitDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	if from == m.originNode && status.ComponentStatus != heartbeatpb.ComponentState_Working {
		if status.CheckpointTs > m.checkpointTs {
			m.checkpointTs = status.CheckpointTs
		}
		m.finished.Store(true)
	}
}

func (m *SplitDispatcherOperator) Schedule() *messaging.TargetMessage {
	return m.replicaSet.NewRemoveInferiorMessage(m.originNode)
}

// OnTaskRemoved is called when the task is removed by ddl
func (m *SplitDispatcherOperator) OnTaskRemoved() {
	m.finished.Store(true)
	m.originalReplicaseRemoved.Store(true)
}

func (m *SplitDispatcherOperator) PostFinished() {
	if m.originalReplicaseRemoved.Load() {
		return
	}
	// todo set checkpoint ts
	m.db.AddAbsentReplicaSet(m.splitSpans...)
}

func (m *SplitDispatcherOperator) Start() {
}

func (m *SplitDispatcherOperator) String() string {
	// todo add split region span
	return fmt.Sprintf("move dispatcher operator: %s, dest:%v ",
		m.replicaSet.ID, m.splitSpans)
}
