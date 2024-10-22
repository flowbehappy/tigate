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

package replica

import (
	"encoding/hex"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// SpanReplication is responsible for a table span replication status
// It is used to manage the replication status of a table span,
// the status is updated by the heartbeat collector
type SpanReplication struct {
	ID           common.DispatcherID
	Span         *heartbeatpb.TableSpan
	ChangefeedID model.ChangeFeedID

	schemaID int64
	nodeID   node.ID
	status   *heartbeatpb.TableSpanStatus
}

func NewReplicaSet(cfID model.ChangeFeedID,
	id common.DispatcherID,
	SchemaID int64,
	span *heartbeatpb.TableSpan,
	checkpointTs uint64) *SpanReplication {
	r := &SpanReplication{
		ID:           id,
		schemaID:     SchemaID,
		Span:         span,
		ChangefeedID: cfID,
		status: &heartbeatpb.TableSpanStatus{
			ID:           id.ToPB(),
			CheckpointTs: checkpointTs,
		},
	}
	log.Info("new replica set created",
		zap.String("changefeed id", cfID.ID),
		zap.String("id", id.String()),
		zap.Int64("schema id", SchemaID),
		zap.Int64("table id", span.TableID),
		zap.String("start", hex.EncodeToString(span.StartKey)),
		zap.String("end", hex.EncodeToString(span.EndKey)))
	return r
}

func NewWorkingReplicaSet(
	cfID model.ChangeFeedID,
	id common.DispatcherID,
	SchemaID int64,
	span *heartbeatpb.TableSpan,
	status *heartbeatpb.TableSpanStatus,
	nodeID node.ID,
) *SpanReplication {
	r := &SpanReplication{
		ID:           id,
		schemaID:     SchemaID,
		Span:         span,
		ChangefeedID: cfID,
		nodeID:       nodeID,
		status:       status,
	}
	log.Info("new working replica set created",
		zap.String("changefeed id", cfID.ID),
		zap.String("id", id.String()),
		zap.String("node id", nodeID.String()),
		zap.Uint64("checkpoint ts", status.CheckpointTs),
		zap.String("component status", status.ComponentStatus.String()),
		zap.Int64("schema id", SchemaID),
		zap.Int64("table id", span.TableID),
		zap.String("start", hex.EncodeToString(span.StartKey)),
		zap.String("end", hex.EncodeToString(span.EndKey)))
	return r
}

func (r *SpanReplication) UpdateStatus(newStatus *heartbeatpb.TableSpanStatus) {
	if newStatus != nil {
		if newStatus.CheckpointTs >= r.status.CheckpointTs {
			r.status = newStatus
		}
	}
}

func (r *SpanReplication) GetSchemaID() int64 {
	return r.schemaID
}

func (r *SpanReplication) SetSchemaID(schemaID int64) {
	r.schemaID = schemaID
}

func (r *SpanReplication) SetNodeID(n node.ID) {
	r.nodeID = n
}

func (r *SpanReplication) GetNodeID() node.ID {
	return r.nodeID
}

func (r *SpanReplication) NewAddDispatcherMessage(server node.ID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(server,
		messaging.HeartbeatCollectorTopic,
		&heartbeatpb.ScheduleDispatcherRequest{
			ChangefeedID: r.ChangefeedID.ID,
			Config: &heartbeatpb.DispatcherConfig{
				DispatcherID: r.ID.ToPB(),
				Span:         r.Span,
				StartTs:      r.status.CheckpointTs,
			},
			ScheduleAction: heartbeatpb.ScheduleAction_Create,
		})
}

func (r *SpanReplication) NewRemoveDispatcherMessage(server node.ID) *messaging.TargetMessage {
	return NewRemoveDispatcherMessage(server, r.ChangefeedID.ID, r.ID.ToPB())
}

func NewRemoveDispatcherMessage(server node.ID, cfID string, dispatcherID *heartbeatpb.DispatcherID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(server,
		messaging.HeartbeatCollectorTopic,
		&heartbeatpb.ScheduleDispatcherRequest{
			ChangefeedID: cfID,
			Config: &heartbeatpb.DispatcherConfig{
				DispatcherID: dispatcherID,
			},
			ScheduleAction: heartbeatpb.ScheduleAction_Remove,
		})
}
