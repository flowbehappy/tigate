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

// RemoveChangefeedOperator is an operator to remove a maintainer from a node
type RemoveChangefeedOperator struct {
	cfID     common.ChangeFeedID
	nodeID   node.ID
	removed  bool
	finished atomic.Bool
}

func NewRemoveChangefeedOperator(cfID common.ChangeFeedID, nodeID node.ID, removed bool) *RemoveChangefeedOperator {
	return &RemoveChangefeedOperator{
		cfID:    cfID,
		nodeID:  nodeID,
		removed: removed,
	}
}

func (m *RemoveChangefeedOperator) Check(from node.ID, status *heartbeatpb.MaintainerStatus) {
	if !m.finished.Load() && from == m.nodeID &&
		status.State != heartbeatpb.ComponentState_Working {
		log.Info("maintainer report non-working status",
			zap.String("maintainer", m.cfID.String()))
		m.finished.Store(true)
	}
}

func (m *RemoveChangefeedOperator) Schedule() *messaging.TargetMessage {
	return changefeed.RemoveMaintainerMessage(m.cfID, m.nodeID, true, m.removed)
}

// OnNodeRemove is called when node offline, and the maintainer must already move to absent status and will be scheduled again
func (m *RemoveChangefeedOperator) OnNodeRemove(n node.ID) {
	if n == m.nodeID {
		m.finished.Store(true)
	}
}

func (m *RemoveChangefeedOperator) ID() common.ChangeFeedID {
	return m.cfID
}

func (m *RemoveChangefeedOperator) IsFinished() bool {
	return m.finished.Load()
}

func (m *RemoveChangefeedOperator) OnTaskRemoved() {
	m.finished.Store(true)
}

func (m *RemoveChangefeedOperator) Start() {
	log.Info("start remove maintainer operator",
		zap.String("changefeed", m.cfID.String()))
}

func (m *RemoveChangefeedOperator) PostFinish() {
	log.Info("remove maintainer operator finished",
		zap.String("changefeed", m.cfID.String()))
}

func (m *RemoveChangefeedOperator) String() string {
	return fmt.Sprintf("remove maintainer operator: %s, dest %s",
		m.cfID, m.nodeID)
}

func (m *RemoveChangefeedOperator) Type() string {
	return "remove"
}
