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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

// MergeSplitDispatcherOperator is an operator to remove a table span from a dispatcher
// and then added some new spans to the replication db
type MergeSplitDispatcherOperator struct {
	db               *replica.ReplicationDB
	originNode       node.ID
	originReplicaSet *replica.SpanReplication
	checkpointTs     uint64

	primary             common.DispatcherID
	affectedReplicaSets []*replica.SpanReplication
	totalRemoved        atomic.Int64
	splitSpans          []*heartbeatpb.TableSpan
	splitSpanInfo       string

	finished   atomic.Bool
	onFinished func()

	removed atomic.Bool
	lck     sync.Mutex
}

// NewMergeSplitDispatcherOperator creates a new MergeSplitDispatcherOperator
func NewMergeSplitDispatcherOperator(
	db *replica.ReplicationDB,
	primary common.DispatcherID,
	originReplicaSet *replica.SpanReplication,
	affectedReplicaSets []*replica.SpanReplication,
	splitSpans []*heartbeatpb.TableSpan,
	onFinished func(),
) *MergeSplitDispatcherOperator {
	spansInfo := ""
	for _, span := range splitSpans {
		spansInfo += fmt.Sprintf("[%s,%s]",
			hex.EncodeToString(span.StartKey), hex.EncodeToString(span.EndKey))
	}
	op := &MergeSplitDispatcherOperator{
		db:                  db,
		originNode:          originReplicaSet.GetNodeID(),
		originReplicaSet:    originReplicaSet,
		checkpointTs:        originReplicaSet.GetStatus().GetCheckpointTs(),
		primary:             primary,
		affectedReplicaSets: affectedReplicaSets,
		totalRemoved:        atomic.Int64{},
		splitSpans:          splitSpans,
		splitSpanInfo:       spansInfo,
		onFinished:          onFinished,
	}
	if op.primary == originReplicaSet.ID {
		op.onFinished = op.increaseTotalRemoved
	}
	return op
}

func (m *MergeSplitDispatcherOperator) Start() {
	m.lck.Lock()
	defer m.lck.Unlock()

	m.db.MarkSpanScheduling(m.originReplicaSet)
}

func (m *MergeSplitDispatcherOperator) OnNodeRemove(n node.ID) {
	m.lck.Lock()
	defer m.lck.Unlock()

	if n == m.originNode {
		log.Info("origin node is removed",
			zap.String("replicaSet", m.originReplicaSet.ID.String()))
		m.markFinished()
	}
}

func (m *MergeSplitDispatcherOperator) increaseTotalRemoved() {
	m.totalRemoved.Add(1)
}

func (m *MergeSplitDispatcherOperator) markFinished() {
	m.finished.Store(true)
	m.onFinished()
}

func (m *MergeSplitDispatcherOperator) AffectedNodes() []node.ID {
	return []node.ID{m.originNode}
}

func (m *MergeSplitDispatcherOperator) ID() common.DispatcherID {
	return m.originReplicaSet.ID
}

func (m *MergeSplitDispatcherOperator) IsFinished() bool {
	if m.removed.Load() {
		return true
	}

	if m.originReplicaSet.ID == m.primary {
		// primary operator wait for all affected replica sets to be removed, since it
		// is responsible for relpace them with new spans.
		return m.finished.Load() && int(m.totalRemoved.Load()) == len(m.affectedReplicaSets)
	}
	return m.finished.Load()
}

func (m *MergeSplitDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	m.lck.Lock()
	defer m.lck.Unlock()

	if from == m.originNode && status.ComponentStatus != heartbeatpb.ComponentState_Working {
		if status.CheckpointTs > m.checkpointTs {
			m.checkpointTs = status.CheckpointTs
		}
		m.originReplicaSet.UpdateStatus(status)
		log.Info("replica set removed from origin node",
			zap.Uint64("checkpointTs", m.checkpointTs),
			zap.String("replicaSet", m.originReplicaSet.ID.String()))
		m.markFinished()
	}
}

func (m *MergeSplitDispatcherOperator) Schedule() *messaging.TargetMessage {
	return m.originReplicaSet.NewRemoveDispatcherMessage(m.originNode)
}

// OnTaskRemoved is called when the task is removed by ddl
func (m *MergeSplitDispatcherOperator) OnTaskRemoved() {
	m.lck.Lock()
	defer m.lck.Unlock()

	log.Info("task removed", zap.String("replicaSet", m.originReplicaSet.ID.String()))
	m.removed.Store(true)
}

func (m *MergeSplitDispatcherOperator) PostFinish() {
	m.lck.Lock()
	defer m.lck.Unlock()

	if m.originReplicaSet.ID == m.primary {
		log.Info("merge-split dispatcher operator finished[primary]", zap.String("id", m.originReplicaSet.ID.String()))
		m.db.ReplaceReplicaSet(m.affectedReplicaSets, m.splitSpans, m.checkpointTs)
		return
	}
	log.Info("merge-split dispatcher operator finished[secondary]", zap.String("id", m.originReplicaSet.ID.String()))
}

func (m *MergeSplitDispatcherOperator) String() string {
	if m.originReplicaSet.ID == m.primary {
		return fmt.Sprintf("merge-split dispatcher operator[primary]: %s, totalAffected: %d, finished: %d, splitSpans:%s",
			m.originReplicaSet.ID, len(m.affectedReplicaSets), m.totalRemoved, m.splitSpanInfo)
	}
	return fmt.Sprintf("merge-split dispatcher operator[secondary]: %s, primary: %s", m.originReplicaSet.ID, m.primary)
}

func (m *MergeSplitDispatcherOperator) Type() string {
	return "merge-split"
}
