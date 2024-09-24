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
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/tiflow/cdc/model"
)

type ReplicaSet struct {
	ID           common.DispatcherID
	SchemaID     int64
	Span         *heartbeatpb.TableSpan
	ChangefeedID model.ChangeFeedID
	status       *heartbeatpb.TableSpanStatus
}

func NewReplicaSet(cfID model.ChangeFeedID,
	id common.DispatcherID,
	SchemaID int64,
	span *heartbeatpb.TableSpan,
	checkpointTs uint64) scheduler.Inferior {
	r := &ReplicaSet{
		ID:           id,
		SchemaID:     SchemaID,
		Span:         span,
		ChangefeedID: cfID,
		status: &heartbeatpb.TableSpanStatus{
			ID:           id.ToPB(),
			CheckpointTs: checkpointTs,
		},
	}
	return r
}

func (r *ReplicaSet) UpdateStatus(status any) {
	if status != nil {
		newStatus := status.(*heartbeatpb.TableSpanStatus)
		if newStatus.CheckpointTs > r.status.CheckpointTs {
			r.status = newStatus
		}
	}
}

func (r *ReplicaSet) NewAddInferiorMessage(server node.ID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(server,
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
				StartTs: r.status.CheckpointTs,
			},
			ScheduleAction: heartbeatpb.ScheduleAction_Create,
		})
}

func (r *ReplicaSet) NewRemoveInferiorMessage(server node.ID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(server,
		messaging.HeartbeatCollectorTopic,
		&heartbeatpb.ScheduleDispatcherRequest{
			ChangefeedID: r.ChangefeedID.ID,
			Config: &heartbeatpb.DispatcherConfig{
				DispatcherID: r.ID.ToPB(),
			},
			ScheduleAction: heartbeatpb.ScheduleAction_Remove,
		})
}
