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
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/maintainer/replica"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
)

// SplitDispatcherOperator is an operator to remove a table span from a dispatcher
// and then added some new spans to the replication db
type SplitDispatcherOperator struct {
	db           *replica.ReplicationDB
	replicaSet   *replica.SpanReplication
	originNode   node.ID
	splitSpans   []*heartbeatpb.TableSpan
	checkpointTs uint64

	finished    atomic.Bool
	taskRemoved atomic.Bool

	splitSpanInfo string

	lck sync.Mutex
}

// NewSplitDispatcherOperator creates a new SplitDispatcherOperator
func NewSplitDispatcherOperator(db *replica.ReplicationDB,
	replicaSet *replica.SpanReplication,
	originNode node.ID,
	splitSpans []*heartbeatpb.TableSpan) *SplitDispatcherOperator {
	spansInfo := ""
	for _, span := range splitSpans {
		spansInfo += fmt.Sprintf("[%s,%s]",
			hex.EncodeToString(span.StartKey), hex.EncodeToString(span.EndKey))
	}
	op := &SplitDispatcherOperator{
		replicaSet:    replicaSet,
		originNode:    originNode,
		splitSpans:    splitSpans,
		db:            db,
		splitSpanInfo: spansInfo,
	}
	return op
}

func (m *SplitDispatcherOperator) Start() {
	m.lck.Lock()
	defer m.lck.Unlock()

	m.db.MarkSpanScheduling(m.replicaSet)
}

func (m *SplitDispatcherOperator) OnNodeRemove(n node.ID) {
	m.lck.Lock()
	defer m.lck.Unlock()

	if n == m.originNode {
		m.finished.Store(true)
	}
}

func (m *SplitDispatcherOperator) ID() common.DispatcherID {
	return m.replicaSet.ID
}

func (m *SplitDispatcherOperator) IsFinished() bool {
	return m.finished.Load()
}

func (m *SplitDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	m.lck.Lock()
	defer m.lck.Unlock()

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
	m.lck.Lock()
	defer m.lck.Unlock()

	m.taskRemoved.Store(true)
	m.finished.Store(true)
}

func (m *SplitDispatcherOperator) PostFinish() {
	m.lck.Lock()
	defer m.lck.Unlock()

	if m.taskRemoved.Load() {
		return
	}
	var newSpan []*replica.SpanReplication
	for _, span := range m.splitSpans {
		newSpan = append(newSpan,
			replica.NewReplicaSet(
				m.replicaSet.ChangefeedID,
				common.NewDispatcherID(),
				m.replicaSet.GetSchemaID(),
				span, m.checkpointTs))
	}
	m.db.ReplaceReplicaSet([]*replica.SpanReplication{m.replicaSet}, newSpan)
}

func (m *SplitDispatcherOperator) String() string {
	return fmt.Sprintf("move dispatcher operator: %s, splitSpans:%s",
		m.replicaSet.ID, m.splitSpanInfo)
}
