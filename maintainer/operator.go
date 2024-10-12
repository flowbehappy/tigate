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

package maintainer

import (
	"sync"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"go.uber.org/atomic"
)

// Operator is the interface for the maintainer schedule dispatchers
type Operator interface {
	// ID returns the dispatcher ID
	ID() common.DispatcherID
	// PreStart is called before the operator is started
	PreStart()
	// Check checks when the new status comes, returns true if the operator is finished
	Check(from node.ID, status *heartbeatpb.TableSpanStatus)
	// Cancel is called when the operator is canceled
	Cancel()
	// IsFinished returns true if the operator is finished
	IsFinished() bool
	// PostFinished is called after the operator is finished and before remove from the task tracker
	PostFinished()
	// SchedulerMessage returns the message to be sent to the scheduler
	SchedulerMessage() *messaging.TargetMessage
	// OnNodeRemove is called when node offline
	OnNodeRemove(node.ID)
}

var mc = &Controller{}

type RemoveDispatcherOperator struct {
	id common.DispatcherID
}

type AddDispatcherOperator struct {
	id       common.DispatcherID
	request  *heartbeatpb.ScheduleDispatcherRequest
	dest     node.ID
	finished atomic.Bool
}

func (m *AddDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	if from == m.dest && status.ComponentStatus == heartbeatpb.ComponentState_Working {
		m.finished.Store(true)
	}
}

func (m *AddDispatcherOperator) SchedulerMessage() *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(m.dest, messaging.HeartbeatCollectorTopic, m.request)
}

func (m *AddDispatcherOperator) OnNodeRemove(n node.ID) {
	if n == m.dest {
		m.finished.Store(true)
	}
}

type MoveDispatcherOperator struct {
	id           common.DispatcherID
	dest         node.ID
	origin       node.ID
	checkpointTs uint64

	removed  atomic.Bool
	finished atomic.Bool
}

func (m *MoveDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	if from == m.dest && status.ComponentStatus == heartbeatpb.ComponentState_Working {
		m.finished.Store(true)
	}
	if from == m.origin && status.ComponentStatus != heartbeatpb.ComponentState_Working {
		m.removed = true
	}
}

func (m *MoveDispatcherOperator) SchedulerMessage() *messaging.TargetMessage {
	if m.removed {
		return m.stm.Inferior.NewAddInferiorMessage(m.dest)
	}
	return m.stm.Inferior.NewRemoveInferiorMessage(m.origin)
}

func (m *MoveDispatcherOperator) OnNodeRemove(n node.ID) bool {
	if n == m.dest && m.removed.Load() {
		return true
	}
	if n == m.origin {
		m.removed.Store(true)
	}
	return false
}

type SplitDispatcherOperator struct {
	changefeedID        string
	OriginID            common.DispatcherID
	originNode          node.ID
	schemaID            int64
	splitSpans          []*heartbeatpb.TableSpan
	checkpointTs        uint64
	preDone             sync.Once
	finished            atomic.Bool
	removing            atomic.Bool
	sendIntervalMessage func(event *Event)
}

func (m *SplitDispatcherOperator) OnNodeRemove(n node.ID) {
	if n == m.originNode {
		m.finished.Store(true)
	}
}

func (m *SplitDispatcherOperator) ID() common.DispatcherID {
	return m.OriginID
}

func (m *SplitDispatcherOperator) IsFinished() bool {
	return m.finished.Load()
}

func (m *SplitDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	if from == m.originNode && status.ComponentStatus != heartbeatpb.ComponentState_Working {
		if status.CheckpointTs > m.checkpointTs {
			m.checkpointTs = status.CheckpointTs
		}
		m.finished.Store(true)
	}
}

func (m *SplitDispatcherOperator) onRemoveCalled() {
	m.removing.Store(true)
}

func (m *SplitDispatcherOperator) SchedulerMessage() *messaging.TargetMessage {
	if m.removing.Load() {
		return messaging.NewSingleTargetMessage(m.originNode,
			messaging.HeartbeatCollectorTopic,
			&heartbeatpb.ScheduleDispatcherRequest{
				ChangefeedID: m.changefeedID,
				Config: &heartbeatpb.DispatcherConfig{
					DispatcherID: m.OriginID.ToPB(),
				},
				ScheduleAction: heartbeatpb.ScheduleAction_Remove,
			})
	} else {
		// notify the maintainer to move the dispatcher to removing status
		m.preDone.Do(
			func() {
				m.sendIntervalMessage(
					&Event{
						changefeedID: m.changefeedID,
						dispatcherEvent: &InternalScheduleDispatcherEvent{
							RemovingDispatcher: &RemovingDispatcherEvent{
								DispatcherID: m.OriginID,
							},
						},
					})
			})
	}
	return nil
}

func (m *SplitDispatcherOperator) PostFinished() {
	newDispatchers := make([]NewDispatcher, 0, len(m.splitSpans))
	for _, span := range m.splitSpans {
		newDispatchers = append(newDispatchers, NewDispatcher{
			SchemaID:     m.schemaID,
			Span:         span,
			CheckpointTs: m.checkpointTs,
		})
	}
	m.sendIntervalMessage(
		&Event{
			changefeedID: m.changefeedID,
			dispatcherEvent: &InternalScheduleDispatcherEvent{
				ReplacingDispatcher: &ReplaceDispatcherEvent{
					Removing:      []common.DispatcherID{m.OriginID},
					NewDispatcher: newDispatchers,
				},
			},
		})
}
