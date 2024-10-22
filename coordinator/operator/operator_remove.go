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

	"github.com/flowbehappy/tigate/coordinator/changefeed"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// RemoveChangefeedOperator is an operator to remove a maintainer from a node
type RemoveChangefeedOperator struct {
	cf       *changefeed.Changefeed
	finished atomic.Bool
}

func NewRemoveChangefeedOperator(cf *changefeed.Changefeed) *RemoveChangefeedOperator {
	return &RemoveChangefeedOperator{
		cf: cf,
	}
}

func (m *RemoveChangefeedOperator) Check(from node.ID, status *heartbeatpb.MaintainerStatus) {
	if !m.finished.Load() && from == m.cf.GetNodeID() &&
		status.State != heartbeatpb.ComponentState_Working {
		log.Info("maintainer report non-working status",
			zap.String("maintainer", m.cf.ID.String()))
		m.finished.Store(true)
	}
}

func (m *RemoveChangefeedOperator) Schedule() *messaging.TargetMessage {
	return m.cf.NewRemoveMaintainerMessage(m.cf.GetNodeID(), true)
}

// OnNodeRemove is called when node offline, and the maintainer must already move to absent status and will be scheduled again
func (m *RemoveChangefeedOperator) OnNodeRemove(n node.ID) {
	if n == m.cf.GetNodeID() {
		m.finished.Store(true)
	}
}

func (m *RemoveChangefeedOperator) ID() model.ChangeFeedID {
	return m.cf.ID
}

func (m *RemoveChangefeedOperator) IsFinished() bool {
	return m.finished.Load()
}

func (m *RemoveChangefeedOperator) OnTaskRemoved() {
	m.finished.Store(true)
}

func (m *RemoveChangefeedOperator) Start() {
	log.Info("start remove maintainer operator",
		zap.String("changefeed", m.cf.ID.String()))
}

func (m *RemoveChangefeedOperator) PostFinish() {
	m.cf.SetNodeID("")
	log.Info("remove maintainer operator finished",
		zap.String("changefeed", m.cf.ID.String()))
}

func (m *RemoveChangefeedOperator) String() string {
	return fmt.Sprintf("remove maintainer operator: %s, dest %s",
		m.cf.ID, m.cf.GetNodeID())
}

func (m *RemoveChangefeedOperator) Type() string {
	return "remove"
}
