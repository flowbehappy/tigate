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

// RemoveDispatcherOperator is an operator to remove a table span from a dispatcher
// and remove it from the replication db
type RemoveDispatcherOperator struct {
	replicaSet *replica.SpanReplication
	finished   atomic.Bool
	db         *replica.ReplicationDB
}

func NewRemoveDispatcherOperator(db *replica.ReplicationDB, replicaSet *replica.SpanReplication) *RemoveDispatcherOperator {
	return &RemoveDispatcherOperator{
		replicaSet: replicaSet,
		db:         db,
	}
}

func (m *RemoveDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	if !m.finished.Load() && from == m.replicaSet.GetNodeID() &&
		status.ComponentStatus != heartbeatpb.ComponentState_Working {
		log.Info("dispatcher report non-working status",
			zap.String("replicaSet", m.replicaSet.ID.String()))
		m.finished.Store(true)
	}
}

func (m *RemoveDispatcherOperator) Schedule() *messaging.TargetMessage {
	return m.replicaSet.NewRemoveInferiorMessage(m.replicaSet.GetNodeID())
}

// OnNodeRemove is called when node offline, and the replicaset must already move to absent status and will be scheduled again
func (m *RemoveDispatcherOperator) OnNodeRemove(n node.ID) {
	if n == m.replicaSet.GetNodeID() {
		m.finished.Store(true)
	}
}

func (m *RemoveDispatcherOperator) ID() common.DispatcherID {
	return m.replicaSet.ID
}

func (m *RemoveDispatcherOperator) IsFinished() bool {
	return m.finished.Load()
}

func (m *RemoveDispatcherOperator) OnTaskRemoved() {
	m.finished.Store(true)
}

func (m *RemoveDispatcherOperator) Start() {
}

func (m *RemoveDispatcherOperator) PostFinish() {
	log.Info("remove dispatcher operator finished",
		zap.String("replicaSet", m.replicaSet.ID.String()),
		zap.String("changefeed", m.replicaSet.ChangefeedID.String()))
}

func (m *RemoveDispatcherOperator) String() string {
	return fmt.Sprintf("remove dispatcher operator: %s, dest %s",
		m.replicaSet.ID, m.replicaSet.GetNodeID())
}

func (m *RemoveDispatcherOperator) Type() string {
	return "remove"
}
