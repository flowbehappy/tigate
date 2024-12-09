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
	"bytes"
	"context"
	"encoding/hex"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// SpanReplication is responsible for a table span replication status
// It is used to manage the replication status of a table span,
// the status is updated by the heartbeat collector
type SpanReplication struct {
	ID           common.DispatcherID
	Span         *heartbeatpb.TableSpan
	ChangefeedID common.ChangeFeedID

	schemaID   int64
	nodeID     node.ID
	groupID    GroupID
	status     *atomic.Pointer[heartbeatpb.TableSpanStatus]
	blockState *atomic.Pointer[heartbeatpb.State]

	tsoClient TSOClient
}

func NewReplicaSet(cfID common.ChangeFeedID,
	id common.DispatcherID,
	tsoClient TSOClient,
	SchemaID int64,
	span *heartbeatpb.TableSpan,
	checkpointTs uint64) *SpanReplication {
	r := &SpanReplication{
		ID:           id,
		groupID:      defaultGroupID,
		tsoClient:    tsoClient,
		schemaID:     SchemaID,
		Span:         span,
		ChangefeedID: cfID,
		status:       atomic.NewPointer[heartbeatpb.TableSpanStatus](nil),
		blockState:   atomic.NewPointer[heartbeatpb.State](nil),
	}
	r.initGroupID()
	r.initStatus(&heartbeatpb.TableSpanStatus{
		ID:           id.ToPB(),
		CheckpointTs: checkpointTs,
	})
	log.Info("new replica set created",
		zap.String("changefeed id", cfID.Name()),
		zap.String("id", id.String()),
		zap.Int64("schema id", SchemaID),
		zap.Int64("table id", span.TableID),
		zap.String("group id", printGroupID(r.groupID)),
		zap.String("start", hex.EncodeToString(span.StartKey)),
		zap.String("end", hex.EncodeToString(span.EndKey)))
	return r
}

func NewWorkingReplicaSet(
	cfID common.ChangeFeedID,
	id common.DispatcherID,
	tsoClient TSOClient,
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
		status:       atomic.NewPointer[heartbeatpb.TableSpanStatus](nil),
		blockState:   atomic.NewPointer[heartbeatpb.State](nil),
		tsoClient:    tsoClient,
	}
	r.initGroupID()
	r.initStatus(status)
	log.Info("new working replica set created",
		zap.String("changefeed id", cfID.Name()),
		zap.String("id", id.String()),
		zap.String("node id", nodeID.String()),
		zap.Uint64("checkpoint ts", status.CheckpointTs),
		zap.String("component status", status.ComponentStatus.String()),
		zap.Int64("schema id", SchemaID),
		zap.Int64("table id", span.TableID),
		zap.String("group id", printGroupID(r.groupID)),
		zap.String("start", hex.EncodeToString(span.StartKey)),
		zap.String("end", hex.EncodeToString(span.EndKey)))
	return r
}

func (r *SpanReplication) initStatus(status *heartbeatpb.TableSpanStatus) {
	if status == nil || status.CheckpointTs == 0 {
		log.Panic("add replica with invalid checkpoint ts",
			zap.String("changefeed id", r.ChangefeedID.Name()),
			zap.String("id", r.ID.String()),
			zap.Uint64("checkpoint ts", status.CheckpointTs),
		)
	}
	r.status.Store(status)
}

func (r *SpanReplication) initGroupID() {
	r.groupID = defaultGroupID
	span := tablepb.Span{TableID: r.Span.TableID, StartKey: r.Span.StartKey, EndKey: r.Span.EndKey}
	// check if the table is split
	totalSpan := spanz.TableIDToComparableSpan(span.TableID)
	if !spanz.IsSubSpan(span, totalSpan) {
		log.Warn("invalid span range", zap.String("changefeed id", r.ChangefeedID.Name()),
			zap.String("id", r.ID.String()), zap.Int64("table id", span.TableID),
			zap.String("totalSpan", totalSpan.String()),
			zap.String("start", hex.EncodeToString(span.StartKey)),
			zap.String("end", hex.EncodeToString(span.EndKey)))
	}
	if !bytes.Equal(span.StartKey, totalSpan.StartKey) || !bytes.Equal(span.EndKey, totalSpan.EndKey) {
		r.groupID = getGroupID(groupTable, span.TableID)
	}
}

func (r *SpanReplication) GetStatus() *heartbeatpb.TableSpanStatus {
	return r.status.Load()
}

func (r *SpanReplication) UpdateStatus(newStatus *heartbeatpb.TableSpanStatus) {
	if newStatus != nil {
		oldStatus := r.status.Load()
		if newStatus.CheckpointTs >= oldStatus.CheckpointTs {
			r.status.Store(newStatus)
		}
	}
}

func (r *SpanReplication) IsDropped() bool {
	return false
	// state := r.blockState.Load()
	// if state != nil && state.NeedDroppedTables != nil {
	// 	status := r.status.Load()
	// 	if status == nil || state.BlockTs != status.CheckpointTs+1 {
	// 		return false
	// 	}
	// 	for _, tableID := range state.NeedDroppedTables.TableIDs {
	// 		if tableID == r.Span.TableID {
	// 			return true
	// 		}
	// 	}
	// }
	// return false
}

func (r *SpanReplication) IsWorking() bool {
	status := r.status.Load()
	return status.ComponentStatus == heartbeatpb.ComponentState_Working
}

func (r *SpanReplication) UpdateBlockState(newState heartbeatpb.State) {
	r.blockState.Store(&newState)
}

func (r *SpanReplication) GetSchemaID() int64 {
	return r.schemaID
}

func (r *SpanReplication) GetTsoClient() TSOClient {
	return r.tsoClient
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

func (r *SpanReplication) NewAddDispatcherMessage(server node.ID) (*messaging.TargetMessage, error) {
	ts, err := getTs(r.tsoClient)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return messaging.NewSingleTargetMessage(server,
		messaging.HeartbeatCollectorTopic,
		&heartbeatpb.ScheduleDispatcherRequest{
			ChangefeedID: r.ChangefeedID.ToPB(),
			Config: &heartbeatpb.DispatcherConfig{
				DispatcherID: r.ID.ToPB(),
				SchemaID:     r.schemaID,
				Span:         r.Span,
				StartTs:      r.status.Load().CheckpointTs,
				CurrentPdTs:  ts,
			},
			ScheduleAction: heartbeatpb.ScheduleAction_Create,
		}), nil
}

func (r *SpanReplication) NewRemoveDispatcherMessage(server node.ID) *messaging.TargetMessage {
	return NewRemoveDispatcherMessage(server, r.ChangefeedID, r.ID.ToPB())
}

func NewRemoveDispatcherMessage(server node.ID, cfID common.ChangeFeedID, dispatcherID *heartbeatpb.DispatcherID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(server,
		messaging.HeartbeatCollectorTopic,
		&heartbeatpb.ScheduleDispatcherRequest{
			ChangefeedID: cfID.ToPB(),
			Config: &heartbeatpb.DispatcherConfig{
				DispatcherID: dispatcherID,
			},
			ScheduleAction: heartbeatpb.ScheduleAction_Remove,
		})
}

func getTs(client TSOClient) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var ts uint64
	err := retry.Do(ctx, func() error {
		phy, logic, err := client.GetTS(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		ts = oracle.ComposeTS(phy, logic)
		return nil
	}, retry.WithTotalRetryDuratoin(300*time.Millisecond),
		retry.WithBackoffBaseDelay(100))
	return ts, errors.Trace(err)
}
