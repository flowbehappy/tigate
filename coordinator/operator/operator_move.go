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
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

// MoveMaintainerOperator is an operator to move a maintainer to the destination node
type MoveMaintainerOperator struct {
	changefeed *changefeed.Changefeed
	db         *changefeed.ChangefeedDB
	origin     node.ID
	dest       node.ID

	originNodeStopped bool
	finished          bool
	bind              bool

	canceled bool

	lck sync.Mutex
}

func NewMoveMaintainerOperator(db *changefeed.ChangefeedDB, changefeed *changefeed.Changefeed,
	origin, dest node.ID) *MoveMaintainerOperator {
	return &MoveMaintainerOperator{
		changefeed: changefeed,
		origin:     origin,
		dest:       dest,
		db:         db,
	}
}

func (m *MoveMaintainerOperator) Check(from node.ID, status *heartbeatpb.MaintainerStatus) {
	m.lck.Lock()
	defer m.lck.Unlock()

	if from == m.origin && status.State != heartbeatpb.ComponentState_Working {
		log.Info("changefeed changefeedIsRemoved from origin node",
			zap.String("changefeed", m.changefeed.ID.String()))
		m.originNodeStopped = true
	}
	if m.originNodeStopped && from == m.dest && status.State == heartbeatpb.ComponentState_Working {
		log.Info("changefeed added to dest node",
			zap.String("dest", m.dest.String()),
			zap.String("changefeed", m.changefeed.ID.String()))
		m.finished = true
	}
}

func (m *MoveMaintainerOperator) Schedule() *messaging.TargetMessage {
	m.lck.Lock()
	defer m.lck.Unlock()

	if m.finished || m.canceled {
		return nil
	}

	if m.originNodeStopped {
		if !m.bind {
			m.db.BindChangefeedToNode(m.origin, m.dest, m.changefeed)
			m.bind = true
		}
		return m.changefeed.NewAddMaintainerMessage(m.dest)
	}
	return m.changefeed.NewRemoveMaintainerMessage(m.origin, false, false)
}

func (m *MoveMaintainerOperator) OnNodeRemove(n node.ID) {
	m.lck.Lock()
	defer m.lck.Unlock()

	if m.finished || m.canceled {
		return
	}

	if n == m.dest {
		// the origin node is finished, we must mark the maintainer as absent to reschedule it again
		if m.originNodeStopped {
			log.Info("dest node is stopped, mark changefeed absent",
				zap.String("changefeed", m.changefeed.ID.String()),
				zap.String("dest", m.dest.String()))
			m.db.MarkMaintainerAbsent(m.changefeed)
			m.canceled = true
			return
		}

		log.Info("changefeed changefeed is removed from dest node",
			zap.String("dest", m.dest.String()),
			zap.String("origin", m.origin.String()),
			zap.String("changefeed", m.changefeed.ID.String()))
		// here we translate the move to an add operation, so we need to swap the origin and dest
		// we need to reset the origin node finished flag
		m.dest = m.origin
		m.db.BindChangefeedToNode(m.dest, m.origin, m.changefeed)
		m.bind = true
		m.originNodeStopped = true
	}
	if n == m.origin {
		log.Info("origin node is stopped",
			zap.String("origin", m.origin.String()),
			zap.String("changefeed", m.changefeed.ID.String()))
		m.originNodeStopped = true
	}
}

func (m *MoveMaintainerOperator) ID() common.ChangeFeedID {
	return m.changefeed.ID
}

func (m *MoveMaintainerOperator) IsFinished() bool {
	m.lck.Lock()
	defer m.lck.Unlock()

	return m.finished || m.canceled
}

func (m *MoveMaintainerOperator) OnTaskRemoved() {
	m.lck.Lock()
	defer m.lck.Unlock()

	log.Info("changefeed is changefeedIsRemoved, mark move changefeed operator finished",
		zap.String("changefeed", m.changefeed.ID.String()))
	m.canceled = true
}

func (m *MoveMaintainerOperator) Start() {
	m.lck.Lock()
	defer m.lck.Unlock()

	m.db.MarkMaintainerScheduling(m.changefeed)
}

func (m *MoveMaintainerOperator) PostFinish() {
	m.lck.Lock()
	defer m.lck.Unlock()

	if m.canceled {
		return
	}

	log.Info("move changefeed operator finished",
		zap.String("changefeed", m.changefeed.ID.String()))
	m.db.MarkMaintainerReplicating(m.changefeed)
}

func (m *MoveMaintainerOperator) String() string {
	m.lck.Lock()
	defer m.lck.Unlock()

	return fmt.Sprintf("move maintainer operator: %s, origin:%s, dest:%s",
		m.changefeed.ID, m.origin, m.dest)
}

func (m *MoveMaintainerOperator) Type() string {
	return "move"
}
