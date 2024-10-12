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
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/maintainer/replica"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/pingcap/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Operator is the interface for the maintainer schedule dispatchers
type Operator interface {
	// ID returns the dispatcher ID
	ID() common.DispatcherID
	// Check checks when the new status comes, returns true if the operator is finished
	Check(from node.ID, status *heartbeatpb.TableSpanStatus)
	// IsFinished returns true if the operator is finished
	IsFinished() bool
	// PostFinished is called after the operator is finished and before remove from the task tracker
	PostFinished()
	// SchedulerMessage returns the message to be sent to the scheduler
	SchedulerMessage() *messaging.TargetMessage
	// OnNodeRemove is called when node offline
	OnNodeRemove(node.ID)
	// OnTaskRemoved is called when the task is removed by ddl
	OnTaskRemoved()
}

type AddDispatcherOperator struct {
	replicaSet *replica.ReplicaSet
	dest       node.ID
	finished   atomic.Bool
}

func NewAddDispatcherOperator(replicaSet *replica.ReplicaSet, dest node.ID) *AddDispatcherOperator {
	return &AddDispatcherOperator{
		replicaSet: replicaSet,
		dest:       dest,
	}
}

func (m *AddDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	if from == m.dest && status.ComponentStatus == heartbeatpb.ComponentState_Working {
		m.finished.Store(true)
	}
}

func (m *AddDispatcherOperator) SchedulerMessage() *messaging.TargetMessage {
	return m.replicaSet.NewAddInferiorMessage(m.dest)
}

// OnNodeRemove is called when node offline, and the replicaset must already move to absent status and will be scheduled again
func (m *AddDispatcherOperator) OnNodeRemove(n node.ID) {
	if n == m.dest {
		m.finished.Store(true)
	}
}
func (m *AddDispatcherOperator) ID() common.DispatcherID { return m.replicaSet.ID }
func (m *AddDispatcherOperator) IsFinished() bool        { return m.finished.Load() }
func (m *AddDispatcherOperator) OnTaskRemoved()          { m.finished.Store(true) }
func (m *AddDispatcherOperator) PostFinished() {
	log.Info("add dispatcher operator finished",
		zap.String("replicaSet", m.replicaSet.ID.String()),
		zap.String("changefeed", m.replicaSet.ChangefeedID.String()))
}

type RemoveDispatcherOperator struct {
	replicaSet *replica.ReplicaSet
	finished   atomic.Bool
}

func NewRemoveDispatcherOperator(replicaSet *replica.ReplicaSet) *RemoveDispatcherOperator {
	return &RemoveDispatcherOperator{
		replicaSet: replicaSet,
	}
}

func (m *RemoveDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	if from == m.replicaSet.GetNodeID() && status.ComponentStatus == heartbeatpb.ComponentState_Working {
		m.finished.Store(true)
	}
}

func (m *RemoveDispatcherOperator) SchedulerMessage() *messaging.TargetMessage {
	return m.replicaSet.NewRemoveInferiorMessage(m.replicaSet.GetNodeID())
}

// OnNodeRemove is called when node offline, and the replicaset must already move to absent status and will be scheduled again
func (m *RemoveDispatcherOperator) OnNodeRemove(n node.ID) {
	if n == m.replicaSet.GetNodeID() {
		m.finished.Store(true)
	}
}
func (m *RemoveDispatcherOperator) ID() common.DispatcherID { return m.replicaSet.ID }
func (m *RemoveDispatcherOperator) IsFinished() bool        { return m.finished.Load() }
func (m *RemoveDispatcherOperator) OnTaskRemoved()          { m.finished.Store(true) }
func (m *RemoveDispatcherOperator) PostFinished() {
	log.Info("remove dispatcher operator finished",
		zap.String("replicaSet", m.replicaSet.ID.String()),
		zap.String("changefeed", m.replicaSet.ChangefeedID.String()))
}

type MoveDispatcherOperator struct {
	replicaSet *replica.ReplicaSet
	dest       node.ID

	removed  atomic.Bool
	finished atomic.Bool
	db       *replica.ReplicaSetDB
}

func NewMoveDispatcherOperator(replicaSet *replica.ReplicaSet, dest node.ID, db *replica.ReplicaSetDB) *MoveDispatcherOperator {
	return &MoveDispatcherOperator{
		replicaSet: replicaSet,
		dest:       dest,
		db:         db,
	}
}

func (m *MoveDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	if from == m.dest && status.ComponentStatus == heartbeatpb.ComponentState_Working {
		m.finished.Store(true)
	}
	if from == m.replicaSet.GetNodeID() && status.ComponentStatus != heartbeatpb.ComponentState_Working {
		m.removed.Store(true)
	}
}

func (m *MoveDispatcherOperator) SchedulerMessage() *messaging.TargetMessage {
	if m.removed.Load() {
		m.db.BindReplicaSetToNode(m.replicaSet.GetNodeID(), m.dest, m.replicaSet)
		return m.replicaSet.NewAddInferiorMessage(m.dest)
	}
	return m.replicaSet.NewRemoveInferiorMessage(m.replicaSet.GetNodeID())
}

func (m *MoveDispatcherOperator) OnNodeRemove(n node.ID) {
	// the replicaset is removed from the origin node
	// and the secondary node offline, we mark the operator finished
	// then replica set will be scheduled again
	if m.removed.Load() && n == m.dest {
		m.finished.Store(true)
	}
	if n == m.replicaSet.GetNodeID() {
		m.removed.Store(true)
	}
}
func (m *MoveDispatcherOperator) ID() common.DispatcherID { return m.replicaSet.ID }
func (m *MoveDispatcherOperator) IsFinished() bool        { return m.finished.Load() }
func (m *MoveDispatcherOperator) OnTaskRemoved()          { m.finished.Store(true) }
func (m *MoveDispatcherOperator) PostFinished() {
	log.Info("move dispatcher operator finished",
		zap.String("replicaSet", m.replicaSet.ID.String()),
		zap.String("changefeed", m.replicaSet.ChangefeedID.String()))
}

type SplitDispatcherOperator struct {
	changefeedID string
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

func (m *SplitDispatcherOperator) SchedulerMessage() *messaging.TargetMessage {
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
	m.db.AddAbsentReplicaSet(m.splitSpans...)
}
