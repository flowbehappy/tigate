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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// StopChangefeedOperator is an operator to remove a maintainer from a node
type StopChangefeedOperator struct {
	cfID            common.ChangeFeedID
	nodeID          node.ID
	removed         bool
	finished        atomic.Bool
	coordinatorNode *node.Info
}

func NewStopChangefeedOperator(cfID common.ChangeFeedID,
	nodeID node.ID,
	coordinatorNode *node.Info,
	removed bool) *StopChangefeedOperator {
	return &StopChangefeedOperator{
		cfID:            cfID,
		nodeID:          nodeID,
		removed:         removed,
		coordinatorNode: coordinatorNode,
	}
}

func (m *StopChangefeedOperator) Check(_ node.ID, status *heartbeatpb.MaintainerStatus) {
	if !m.finished.Load() && status.State != heartbeatpb.ComponentState_Working {
		log.Info("maintainer report non-working status",
			zap.String("maintainer", m.cfID.String()))
		m.finished.Store(true)
	}
}

func (m *StopChangefeedOperator) Schedule() *messaging.TargetMessage {
	return changefeed.RemoveMaintainerMessage(m.cfID, m.nodeID, true, m.removed)
}

// OnNodeRemove is called when node offline, and the maintainer must already move to absent status and will be scheduled again
func (m *StopChangefeedOperator) OnNodeRemove(n node.ID) {
	if n == m.nodeID {
		log.Info("node is stopped during stop maintainer, schedule stop command to coordinator node",
			zap.String("changefeed", m.cfID.String()),
			zap.String("node", n.String()))
		m.nodeID = m.coordinatorNode.ID
	}
}

func (m *StopChangefeedOperator) ID() common.ChangeFeedID {
	return m.cfID
}

func (m *StopChangefeedOperator) IsFinished() bool {
	return m.finished.Load()
}

func (m *StopChangefeedOperator) OnTaskRemoved() {
	m.finished.Store(true)
}

func (m *StopChangefeedOperator) Start() {
	log.Info("start remove maintainer operator",
		zap.String("changefeed", m.cfID.String()))
}

func (m *StopChangefeedOperator) PostFinish() {
	log.Info("remove maintainer operator finished",
		zap.String("changefeed", m.cfID.String()))
}

func (m *StopChangefeedOperator) String() string {
	return fmt.Sprintf("remove maintainer operator: %s, dest %s",
		m.cfID, m.nodeID)
}

func (m *StopChangefeedOperator) Type() string {
	return "remove"
}
