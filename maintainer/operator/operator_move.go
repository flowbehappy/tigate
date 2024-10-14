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

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/maintainer/replica"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/pingcap/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// MoveDispatcherOperator is an operator to move a table span to the destination dispatcher
type MoveDispatcherOperator struct {
	replicaSet *replica.SpanReplication
	origin     node.ID
	dest       node.ID

	originRemoved atomic.Bool
	finished      atomic.Bool
	bind          atomic.Bool

	db *replica.ReplicationDB
}

func NewMoveDispatcherOperator(db *replica.ReplicationDB, replicaSet *replica.SpanReplication, origin, dest node.ID) *MoveDispatcherOperator {
	return &MoveDispatcherOperator{
		replicaSet: replicaSet,
		origin:     origin,
		dest:       dest,
		db:         db,
	}
}

func (m *MoveDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	if from == m.origin && status.ComponentStatus != heartbeatpb.ComponentState_Working {
		log.Info("replica set removed from origin node",
			zap.String("replicaSet", m.replicaSet.ID.String()))
		m.originRemoved.Store(true)
	}
	if m.originRemoved.Load() && from == m.dest && status.ComponentStatus == heartbeatpb.ComponentState_Working {
		m.finished.Store(true)
	}
}

func (m *MoveDispatcherOperator) Schedule() *messaging.TargetMessage {
	if m.originRemoved.Load() {
		if !m.bind.Load() {
			m.db.BindSpanToNode(m.origin, m.dest, m.replicaSet)
			m.bind.Store(true)
		}
		return m.replicaSet.NewAddInferiorMessage(m.dest)
	}
	return m.replicaSet.NewRemoveInferiorMessage(m.replicaSet.GetNodeID())
}

func (m *MoveDispatcherOperator) OnNodeRemove(n node.ID) {
	// the replicaset is removed from the origin node
	// and the secondary node offline, we mark the operator finished
	// then replica set will be scheduled again
	if m.originRemoved.Load() && n == m.dest {
		m.finished.Store(true)
	}
	if n == m.origin {
		m.originRemoved.Store(true)
	}
}

func (m *MoveDispatcherOperator) ID() common.DispatcherID {
	return m.replicaSet.ID
}

func (m *MoveDispatcherOperator) IsFinished() bool {
	return m.finished.Load()
}

func (m *MoveDispatcherOperator) OnTaskRemoved() {
	log.Info("replicaset is removed, mark move dispatcher operator finished",
		zap.String("replicaSet", m.replicaSet.ID.String()),
		zap.String("changefeed", m.replicaSet.ChangefeedID.String()))
	m.finished.Store(true)
	m.originRemoved.Store(true)
}

func (m *MoveDispatcherOperator) Start() {
	m.db.MarkSpanScheduling(m.replicaSet)
}

func (m *MoveDispatcherOperator) PostFinished() {
	if m.originRemoved.Load() {
		log.Info("move dispatcher operator finished",
			zap.String("replicaSet", m.replicaSet.ID.String()),
			zap.String("changefeed", m.replicaSet.ChangefeedID.String()))
		m.db.MarkSpanReplicating(m.replicaSet)
	}
}

func (m *MoveDispatcherOperator) String() string {
	return fmt.Sprintf("move dispatcher operator: %s, origin:%s, dest:%s ",
		m.replicaSet.ID, m.origin, m.dest)
}
