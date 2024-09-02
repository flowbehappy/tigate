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
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/tiflow/cdc/model"
)

type ReplicaSet struct {
	ID           common.DispatcherID
	SchemaID     int64
	Span         *common.TableSpan
	ChangefeedID model.ChangeFeedID
	status       ReplicaSetStatus
	stateMachine *scheduler.StateMachine

	checkpointTs uint64
}

func NewReplicaSet(cfID model.ChangeFeedID,
	id scheduler.InferiorID,
	SchemaID int64,
	span *common.TableSpan,
	checkpointTs uint64) scheduler.Inferior {
	r := &ReplicaSet{
		ID:           id.(common.DispatcherID),
		SchemaID:     SchemaID,
		Span:         span,
		ChangefeedID: cfID,
		checkpointTs: checkpointTs,
	}
	return r
}

func (r *ReplicaSet) UpdateStatus(status scheduler.InferiorStatus) {
	newStatus := status.(ReplicaSetStatus)
	if newStatus.CheckpointTs > r.checkpointTs {
		r.checkpointTs = newStatus.CheckpointTs
		r.status.CheckpointTs = newStatus.CheckpointTs
	}
}

func (r *ReplicaSet) NewAddInferiorMessage(server model.CaptureID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(messaging.ServerId(server),
		messaging.HeartbeatCollectorTopic,
		&heartbeatpb.ScheduleDispatcherRequest{
			ChangefeedID: r.ChangefeedID.ID,
			Config: &heartbeatpb.DispatcherConfig{
				DispatcherID: r.ID.ToPB(),
				Span: &heartbeatpb.TableSpan{
					TableID:  r.Span.TableID,
					StartKey: r.Span.StartKey,
					EndKey:   r.Span.EndKey,
				},
				StartTs: r.checkpointTs,
			},
			ScheduleAction: heartbeatpb.ScheduleAction_Create,
		})
}

func (r *ReplicaSet) NewRemoveInferiorMessage(server model.CaptureID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(messaging.ServerId(server),
		messaging.HeartbeatCollectorTopic,
		&heartbeatpb.ScheduleDispatcherRequest{
			ChangefeedID: r.ChangefeedID.ID,
			Config: &heartbeatpb.DispatcherConfig{
				DispatcherID: r.ID.ToPB(),
			},
			ScheduleAction: heartbeatpb.ScheduleAction_Remove,
		})
}

func (r *ReplicaSet) SetStateMachine(state *scheduler.StateMachine) {
	r.stateMachine = state
}

func (r *ReplicaSet) GetStateMachine() *scheduler.StateMachine {
	return r.stateMachine
}

type ReplicaSetStatus struct {
	ID           common.DispatcherID
	State        heartbeatpb.ComponentState
	CheckpointTs uint64
	DDLStatus    *heartbeatpb.State
}

func (c ReplicaSetStatus) GetInferiorID() scheduler.InferiorID {
	return scheduler.InferiorID(c.ID)
}

func (c ReplicaSetStatus) GetInferiorState() heartbeatpb.ComponentState {
	return c.State
}
