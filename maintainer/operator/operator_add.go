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

type AddDispatcherOperator struct {
	replicaSet *replica.ReplicaSet
	dest       node.ID
	finished   atomic.Bool
	db         *replica.ReplicaSetDB
}

func NewAddDispatcherOperator(
	db *replica.ReplicaSetDB,
	replicaSet *replica.ReplicaSet,
	dest node.ID) *AddDispatcherOperator {
	return &AddDispatcherOperator{
		replicaSet: replicaSet,
		dest:       dest,
		db:         db,
	}
}

func (m *AddDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	if from == m.dest && status.ComponentStatus == heartbeatpb.ComponentState_Working {
		m.finished.Store(true)
	}
}

func (m *AddDispatcherOperator) Schedule() *messaging.TargetMessage {
	return m.replicaSet.NewAddInferiorMessage(m.dest)
}

// OnNodeRemove is called when node offline, and the replicaset must already move to absent status and will be scheduled again
func (m *AddDispatcherOperator) OnNodeRemove(n node.ID) {
	if n == m.dest {
		m.finished.Store(true)
	}
}

func (m *AddDispatcherOperator) ID() common.DispatcherID {
	return m.replicaSet.ID
}

func (m *AddDispatcherOperator) IsFinished() bool {
	return m.finished.Load()
}

func (m *AddDispatcherOperator) OnTaskRemoved() {
	m.finished.Store(true)
}

func (m *AddDispatcherOperator) Start() {
	m.db.BindReplicaSetToNode("", m.dest, m.replicaSet)
}

func (m *AddDispatcherOperator) PostFinished() {
	log.Info("add dispatcher operator finished",
		zap.String("replicaSet", m.replicaSet.ID.String()),
		zap.String("changefeed", m.replicaSet.ChangefeedID.String()))
	m.db.MarkReplicaSetWorking(m.replicaSet)
}

func (m *AddDispatcherOperator) String() string {
	return fmt.Sprintf("add dispatcher operator: %s, dest:%s ",
		m.replicaSet.ID, m.dest)
}
