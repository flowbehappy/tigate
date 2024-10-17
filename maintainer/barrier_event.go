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
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/maintainer/range_checker"
	"github.com/flowbehappy/tigate/maintainer/replica"
	"github.com/flowbehappy/tigate/pkg/common"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// BarrierEvent is a barrier event that reported by dispatchers, note is a block multiple dispatchers
// all of these dispatchers should report the same event
type BarrierEvent struct {
	cfID                     string
	commitTs                 uint64
	controller               *Controller
	selected                 bool
	writerDispatcher         common.DispatcherID
	writerDispatcherAdvanced bool

	blockedDispatchers *heartbeatpb.InfluencedTables
	dropDispatchers    *heartbeatpb.InfluencedTables
	blockedTasks       []*replica.SpanReplication
	newTables          []*heartbeatpb.Table
	schemaIDChange     []*heartbeatpb.SchemaIDChange
	isSyncPoint        bool
	// if the split table is enable for this changefeeed, if not we can use table id to check coverage
	splitTableEnabled bool

	// rangeChecker is used to check if all the dispatchers reported the block events
	rangeChecker   range_checker.RangeChecker
	lastResendTime time.Time
}

func NewBlockEvent(cfID string, controller *Controller,
	status *heartbeatpb.State, splitTableEnabled bool) *BarrierEvent {
	event := &BarrierEvent{
		controller:         controller,
		selected:           false,
		cfID:               cfID,
		commitTs:           status.BlockTs,
		blockedDispatchers: status.BlockTables,
		newTables:          status.NeedAddedTables,
		dropDispatchers:    status.NeedDroppedTables,
		schemaIDChange:     status.UpdatedSchemas,
		lastResendTime:     time.Time{},
		isSyncPoint:        status.IsSyncPoint,
		splitTableEnabled:  splitTableEnabled,
	}
	if status.BlockTables != nil {
		var tbls []int64
		switch status.BlockTables.InfluenceType {
		case heartbeatpb.InfluenceType_Normal:
			event.setRangeCheckers(status.BlockTables.TableIDs)
			event.blockedTasks = controller.GetTasksByTableIDs(status.BlockTables.TableIDs...)
		case heartbeatpb.InfluenceType_DB:
			reps := controller.GetTasksBySchemaID(status.BlockTables.SchemaID)
			tbls = make([]int64, 0, len(reps))
			for _, rep := range reps {
				tbls = append(tbls, rep.Span.TableID)
			}
		case heartbeatpb.InfluenceType_All:
			reps := controller.GetAllTasks()
			tbls = make([]int64, 0, len(reps))
			for _, rep := range reps {
				tbls = append(tbls, rep.Span.TableID)
			}
		}
		event.setRangeCheckers(status.BlockTables.TableIDs)
	}
	return event
}

func (be *BarrierEvent) setRangeCheckers(tbls []int64) {
	if be.splitTableEnabled {
		be.rangeChecker = range_checker.NewTableSpanRangeChecker(tbls)
	} else {
		be.rangeChecker = range_checker.NewTableIDRangeChecker(tbls)
	}
}

func (be *BarrierEvent) scheduleBlockEvent() {
	// dispatcher notify us to drop some tables, by dispatcher ID or schema ID
	if be.dropDispatchers != nil {
		switch be.dropDispatchers.InfluenceType {
		case heartbeatpb.InfluenceType_DB:
			be.controller.RemoveTasksBySchemaID(be.dropDispatchers.SchemaID)
			log.Info(" remove table",
				zap.String("changefeed", be.cfID),
				zap.Uint64("commitTs", be.commitTs),
				zap.Int64("schema", be.dropDispatchers.SchemaID))
		case heartbeatpb.InfluenceType_Normal:
			be.controller.RemoveTasksByTableIDs(be.dropDispatchers.TableIDs...)
			log.Info(" remove table",
				zap.String("changefeed", be.cfID),
				zap.Uint64("commitTs", be.commitTs),
				zap.Int64s("table", be.dropDispatchers.TableIDs))
		case heartbeatpb.InfluenceType_All:
			be.controller.RemoveAllTasks()
			log.Info("remove all tables by barrier",
				zap.Uint64("commitTs", be.commitTs),
				zap.String("changefeed", be.cfID))
		}
	}
	for _, add := range be.newTables {
		log.Info(" add new table",
			zap.Uint64("commitTs", be.commitTs),
			zap.String("changefeed", be.cfID),
			zap.Int64("schema", add.SchemaID),
			zap.Int64("table", add.TableID))
		be.controller.AddNewTable(commonEvent.Table{
			SchemaID: add.SchemaID,
			TableID:  add.TableID,
		}, be.commitTs)
	}

	for _, change := range be.schemaIDChange {
		log.Info("update schema id",
			zap.String("changefeed", be.cfID),
			zap.Uint64("commitTs", be.commitTs),
			zap.Int64("newSchema", change.OldSchemaID),
			zap.Int64("oldSchema", change.NewSchemaID),
			zap.Int64("table", change.TableID))
		be.controller.UpdateSchemaID(change.TableID, change.NewSchemaID)
	}
}

func (be *BarrierEvent) markDispatcherEventDone(dispatcherID common.DispatcherID) {
	replicaSpan := be.controller.GetTask(dispatcherID)
	if replicaSpan == nil {
		log.Warn("dispatcher not found, ignore",
			zap.String("changefeed", be.cfID),
			zap.String("dispatcher", dispatcherID.String()))
		return
	}
	be.rangeChecker.AddSubRange(replicaSpan.Span.TableID, replicaSpan.Span.StartKey, replicaSpan.Span.EndKey)
}

func (be *BarrierEvent) allDispatcherReported() bool {
	if be.blockedDispatchers == nil {
		return true
	}
	return be.rangeChecker.IsFullyCovered()
}

func (be *BarrierEvent) sendPassAction() []*messaging.TargetMessage {
	if be.blockedDispatchers == nil {
		return []*messaging.TargetMessage{}
	}
	msgMap := make(map[node.ID]*messaging.TargetMessage)
	switch be.blockedDispatchers.InfluenceType {
	case heartbeatpb.InfluenceType_DB:
		for _, stm := range be.controller.GetTasksBySchemaID(be.blockedDispatchers.SchemaID) {
			nodeID := stm.GetNodeID()
			if nodeID == "" {
				continue
			}
			_, ok := msgMap[nodeID]
			if !ok {
				msgMap[nodeID] = be.newPassActionMessage(nodeID)
			}
		}
	case heartbeatpb.InfluenceType_All:
		for _, n := range be.controller.GetAllNodes() {
			msgMap[n] = be.newPassActionMessage(n)
		}
	case heartbeatpb.InfluenceType_Normal:
		// send pass action
		for _, stm := range be.blockedTasks {
			if stm == nil {
				continue
			}
			nodeID := stm.GetNodeID()
			dispatcherID := stm.ID
			if dispatcherID == be.writerDispatcher {
				continue
			}
			msg, ok := msgMap[nodeID]
			if !ok {
				msg = be.newPassActionMessage(nodeID)
				msgMap[nodeID] = msg
			}
			influencedDispatchers := msg.Message[0].(*heartbeatpb.HeartBeatResponse).DispatcherStatuses[0].InfluencedDispatchers
			influencedDispatchers.DispatcherIDs = append(influencedDispatchers.DispatcherIDs, dispatcherID.ToPB())
		}
	}
	var msgs = make([]*messaging.TargetMessage, 0, len(msgMap))
	for _, msg := range msgMap {
		msgs = append(msgs, msg)
	}
	return msgs
}

// dispatcherReachedBlockTs check if all the dispatchers reported the block events,
// if so, select one dispatcher to write, currently choose the last one
func (be *BarrierEvent) dispatcherReachedBlockTs(dispatcherID common.DispatcherID) *heartbeatpb.DispatcherAction {
	if be.selected {
		return nil
	}
	be.markDispatcherEventDone(dispatcherID)
	// all dispatcher reported heartbeat, select the last one to write
	if be.allDispatcherReported() {
		be.writerDispatcher = dispatcherID
		be.selected = true
		// reset ranger checkers
		be.rangeChecker.Reset()
		log.Info("all dispatcher reported heartbeat, select one to write",
			zap.String("changefeed", be.cfID),
			zap.String("dispatcher", dispatcherID.String()),
			zap.Uint64("commitTs", be.commitTs),
			zap.String("barrierType", be.blockedDispatchers.InfluenceType.String()))
		return be.action(heartbeatpb.Action_Write)
	}
	return nil
}

func (be *BarrierEvent) resend() []*messaging.TargetMessage {
	if time.Since(be.lastResendTime) < time.Second {
		return nil
	}
	// still waiting for all dispatcher to reach the block commit ts
	if !be.selected {
		return nil
	}
	be.lastResendTime = time.Now()
	// we select a dispatcher as the writer, still waiting for that dispatcher advance its checkpoint ts
	if !be.writerDispatcherAdvanced {
		//resend write action
		stm := be.controller.GetTask(be.writerDispatcher)
		if stm == nil || stm.GetNodeID() == "" {
			return nil
		}
		return []*messaging.TargetMessage{be.newWriterActionMessage(stm.GetNodeID())}
	}
	// the writer dispatcher is advanced, resend pass action
	return be.sendPassAction()
}

func (be *BarrierEvent) newWriterActionMessage(capture node.ID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(capture, messaging.HeartbeatCollectorTopic,
		&heartbeatpb.HeartBeatResponse{
			ChangefeedID: be.cfID,
			DispatcherStatuses: []*heartbeatpb.DispatcherStatus{
				{
					Action: be.action(heartbeatpb.Action_Write),
					InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						DispatcherIDs: []*heartbeatpb.DispatcherID{
							be.writerDispatcher.ToPB(),
						},
					},
				},
			}})
}

func (be *BarrierEvent) newPassActionMessage(capture node.ID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(capture, messaging.HeartbeatCollectorTopic,
		&heartbeatpb.HeartBeatResponse{
			ChangefeedID: be.cfID,
			DispatcherStatuses: []*heartbeatpb.DispatcherStatus{
				{
					Action: be.action(heartbeatpb.Action_Pass),
					InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
						InfluenceType:       be.blockedDispatchers.InfluenceType,
						SchemaID:            be.blockedDispatchers.SchemaID,
						ExcludeDispatcherId: be.writerDispatcher.ToPB(),
					},
				},
			}})
}

func (be *BarrierEvent) action(action heartbeatpb.Action) *heartbeatpb.DispatcherAction {
	return &heartbeatpb.DispatcherAction{
		Action:      action,
		CommitTs:    be.commitTs,
		IsSyncPoint: be.isSyncPoint,
	}
}
