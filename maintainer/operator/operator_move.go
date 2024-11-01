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
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

// MoveDispatcherOperator is an operator to move a table span to the destination dispatcher
type MoveDispatcherOperator struct {
	replicaSet *replica.SpanReplication
	db         *replica.ReplicationDB
	origin     node.ID
	dest       node.ID

	originNodeStopped bool
	finished          bool
	bind              bool

	noPostFinishNeed bool

	lck sync.Mutex
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
	m.lck.Lock()
	defer m.lck.Unlock()

	if from == m.origin && status.ComponentStatus != heartbeatpb.ComponentState_Working {
		log.Info("replica set removed from origin node",
			zap.String("replicaSet", m.replicaSet.ID.String()))
		m.originNodeStopped = true
	}
	if m.originNodeStopped && from == m.dest && status.ComponentStatus == heartbeatpb.ComponentState_Working {
		log.Info("replica set added to dest node",
			zap.String("dest", m.dest.String()),
			zap.String("replicaSet", m.replicaSet.ID.String()))
		m.finished = true
	}
}

func (m *MoveDispatcherOperator) Schedule() *messaging.TargetMessage {
	m.lck.Lock()
	defer m.lck.Unlock()

	if m.originNodeStopped {
		if !m.bind {
			m.db.BindSpanToNode(m.origin, m.dest, m.replicaSet)
			m.bind = true
		}
		return m.replicaSet.NewAddDispatcherMessage(m.dest)
	}
	return m.replicaSet.NewRemoveDispatcherMessage(m.origin)
}

func (m *MoveDispatcherOperator) OnNodeRemove(n node.ID) {
	m.lck.Lock()
	defer m.lck.Unlock()

	if n == m.dest {
		// the origin node is finished, we must mark the span as absent to reschedule it again
		if m.originNodeStopped {
			log.Info("dest node is stopped, mark span absent",
				zap.String("replicaSet", m.replicaSet.ID.String()),
				zap.String("dest", m.dest.String()))
			m.db.MarkSpanAbsent(m.replicaSet)
			m.noPostFinishNeed = true
			return
		}

		log.Info("replica set removed from dest node",
			zap.String("dest", m.dest.String()),
			zap.String("origin", m.origin.String()),
			zap.String("replicaSet", m.replicaSet.ID.String()))
		// here we translate the move to an add operation, so we need to swap the origin and dest
		// we need to reset the origin node finished flag
		m.dest = m.origin
		m.db.BindSpanToNode(m.dest, m.origin, m.replicaSet)
		m.bind = true
		m.originNodeStopped = true
	}
	if n == m.origin {
		log.Info("origin node is stopped",
			zap.String("origin", m.origin.String()),
			zap.String("replicaSet", m.replicaSet.ID.String()))
		m.originNodeStopped = true
	}
}

func (m *MoveDispatcherOperator) ID() common.DispatcherID {
	return m.replicaSet.ID
}

func (m *MoveDispatcherOperator) IsFinished() bool {
	m.lck.Lock()
	defer m.lck.Unlock()

	return m.finished || m.noPostFinishNeed
}

func (m *MoveDispatcherOperator) OnTaskRemoved() {
	m.lck.Lock()
	defer m.lck.Unlock()

	log.Info("replicaset is removed, mark move dispatcher operator finished",
		zap.String("replicaSet", m.replicaSet.ID.String()),
		zap.String("changefeed", m.replicaSet.ChangefeedID.String()))
	m.noPostFinishNeed = true
}

func (m *MoveDispatcherOperator) Start() {
	m.lck.Lock()
	defer m.lck.Unlock()

	m.db.MarkSpanScheduling(m.replicaSet)
}

func (m *MoveDispatcherOperator) PostFinish() {
	m.lck.Lock()
	defer m.lck.Unlock()

	if m.noPostFinishNeed {
		return
	}

	log.Info("move dispatcher operator finished",
		zap.String("span", m.replicaSet.ID.String()),
		zap.String("changefeed", m.replicaSet.ChangefeedID.String()))
	m.db.MarkSpanReplicating(m.replicaSet)
}

func (m *MoveDispatcherOperator) String() string {
	m.lck.Lock()
	defer m.lck.Unlock()

	return fmt.Sprintf("move dispatcher operator: %s, origin:%s, dest:%s",
		m.replicaSet.ID, m.origin, m.dest)
}

func (m *MoveDispatcherOperator) Type() string {
	return "move"
}
