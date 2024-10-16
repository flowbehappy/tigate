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
	"bytes"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/maintainer/replica"
	"github.com/flowbehappy/tigate/pkg/common"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/google/btree"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/spanz"
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

	reportedRange  map[int64]*RangeChecker
	lastResendTime time.Time
}

func NewBlockEvent(cfID string, controller *Controller,
	status *heartbeatpb.State) *BarrierEvent {
	event := &BarrierEvent{
		controller:         controller,
		selected:           false,
		cfID:               cfID,
		commitTs:           status.BlockTs,
		blockedDispatchers: status.BlockTables,
		newTables:          status.NeedAddedTables,
		dropDispatchers:    status.NeedDroppedTables,
		schemaIDChange:     status.UpdatedSchemas,
		reportedRange:      make(map[int64]*RangeChecker),
		lastResendTime:     time.Time{},
		isSyncPoint:        status.IsSyncPoint,
	}
	if status.BlockTables != nil {
		switch status.BlockTables.InfluenceType {
		case heartbeatpb.InfluenceType_Normal:
			event.addRangeCheckers(status.BlockTables.TableIDs...)
			event.blockedTasks = controller.GetTasksByTableIDs(status.BlockTables.TableIDs...)
		// todo: we should query the schema id and all tasks from the schema storage
		case heartbeatpb.InfluenceType_DB:
			for _, rep := range controller.GetTasksBySchemaID(status.BlockTables.SchemaID) {
				event.addRangeCheckers(rep.Span.TableID)
			}
		case heartbeatpb.InfluenceType_All:
			for _, rep := range controller.GetAllTasks() {
				event.addRangeCheckers(rep.Span.TableID)
			}
		}
	}
	return event
}

func (be *BarrierEvent) addRangeCheckers(tbls ...int64) {
	for _, tbl := range tbls {
		if _, ok := be.reportedRange[tbl]; !ok {
			span := spanz.TableIDToComparableSpan(tbl)
			be.reportedRange[tbl] = NewRangeChecker(span.StartKey, span.EndKey)
		}
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
	rangeChecker, ok := be.reportedRange[replicaSpan.Span.TableID]
	if !ok {
		log.Warn("dispatcher not in the block list, ignore",
			zap.String("changefeed", be.cfID),
			zap.String("dispatcher", dispatcherID.String()))
		return
	}
	rangeChecker.AddSubRange(replicaSpan.Span.StartKey, replicaSpan.Span.EndKey)
}

func (be *BarrierEvent) allDispatcherDone() bool {
	return be.allDispatcherReported()
}

func (be *BarrierEvent) allDispatcherReported() bool {
	if be.blockedDispatchers == nil {
		return true
	}
	for _, checker := range be.reportedRange {
		if !checker.IsFullyCovered() {
			return false
		}
	}
	return true
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
		for _, checker := range be.reportedRange {
			checker.Reset()
		}
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

type rangeCoverChecker struct {
	startKey      []byte
	endKey        []byte
	rangeCoverMap *btree.BTreeG[*rangeEntry]
}

func newSpanCoverChecker(startKey, endKey []byte) *rangeCoverChecker {
	return &rangeCoverChecker{
		startKey:      startKey,
		endKey:        endKey,
		rangeCoverMap: btree.NewG[*rangeEntry](16, rangeEntryLess),
	}
}

type rangeEntry struct {
	startKey []byte
	endKey   []byte
}

func rangeEntryLess(a, b *rangeEntry) bool {
	return bytes.Compare(a.startKey, b.startKey) < 0
}

func (r *rangeCoverChecker) addRange(startKey, endKey []byte) {
	item := &rangeEntry{
		startKey: startKey,
		endKey:   endKey,
	}
	oldItem, ok := r.rangeCoverMap.Get(item)
	if ok {
		// the old range is covered by the new range, replace the old range
		if bytes.Compare(oldItem.endKey, endKey) < 0 {
			oldItem.endKey = endKey
		}
	} else {
		r.rangeCoverMap.ReplaceOrInsert(item)
	}
}

func (r *rangeCoverChecker) hasHole() bool {
	var lastEndKey []byte
	r.rangeCoverMap.Ascend(func(i *rangeEntry) bool {
		if lastEndKey != nil && bytes.Compare(lastEndKey, i.startKey) < 0 {
			return false
		}
		lastEndKey = i.endKey
		return true
	})
	return lastEndKey == nil || bytes.Compare(lastEndKey, r.endKey) < 0
}
