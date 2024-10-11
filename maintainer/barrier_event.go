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
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// BarrierEvent is a barrier event that reported by dispatchers, note is a block multiple dispatchers
// all of these dispatchers should report the same event
type BarrierEvent struct {
	cfID                     string
	commitTs                 uint64
	scheduler                *Controller
	selected                 bool
	writerDispatcher         common.DispatcherID
	writerDispatcherAdvanced bool

	blockedDispatchers *heartbeatpb.InfluencedTables
	dropDispatchers    *heartbeatpb.InfluencedTables

	blockedTasks   []*scheduler.StateMachine[common.DispatcherID]
	dropTasks      []*scheduler.StateMachine[common.DispatcherID]
	newTables      []*heartbeatpb.Table
	schemaIDChange []*heartbeatpb.SchemaIDChange

	reportedDispatchers map[common.DispatcherID]bool
	lastResendTime      time.Time
}

func NewBlockEvent(cfID string, scheduler *Controller,
	status *heartbeatpb.State) *BarrierEvent {
	event := &BarrierEvent{
		scheduler:           scheduler,
		selected:            false,
		cfID:                cfID,
		commitTs:            status.BlockTs,
		blockedDispatchers:  status.BlockTables,
		newTables:           status.NeedAddedTables,
		dropDispatchers:     status.NeedDroppedTables,
		schemaIDChange:      status.UpdatedSchemas,
		reportedDispatchers: make(map[common.DispatcherID]bool),
		lastResendTime:      time.Time{},
	}
	if event.blockedDispatchers != nil && event.blockedDispatchers.InfluenceType == heartbeatpb.InfluenceType_Normal {
		event.blockedTasks = event.scheduler.GetTasksByTableIDs(event.blockedDispatchers.TableIDs...)
	}
	if event.dropDispatchers != nil && event.dropDispatchers.InfluenceType == heartbeatpb.InfluenceType_Normal {
		event.dropTasks = event.scheduler.GetTasksByTableIDs(event.dropDispatchers.TableIDs...)
	}
	return event
}

func (be *BarrierEvent) scheduleBlockEvent() {
	// dispatcher notify us to drop some tables, by dispatcher ID or schema ID
	if be.dropDispatchers != nil {
		switch be.dropDispatchers.InfluenceType {
		case heartbeatpb.InfluenceType_DB:
			for _, stm := range be.scheduler.GetTasksBySchemaID(be.dropDispatchers.SchemaID) {
				log.Info(" remove table",
					zap.String("changefeed", be.cfID),
					zap.String("table", stm.ID.String()))
				be.scheduler.RemoveTask(stm)
			}
		case heartbeatpb.InfluenceType_Normal:
			for _, stm := range be.dropTasks {
				log.Info(" remove table",
					zap.String("changefeed", be.cfID),
					zap.String("table", stm.ID.String()))
				be.scheduler.RemoveTask(stm)
			}
		case heartbeatpb.InfluenceType_All:
			log.Info("remove all tables by barrier", zap.String("changefeed", be.cfID))
			be.scheduler.RemoveAllTasks()
		}
	}
	for _, add := range be.newTables {
		log.Info(" add new table",
			zap.String("changefeed", be.cfID),
			zap.Int64("schema", add.SchemaID),
			zap.Int64("table", add.TableID))
		be.scheduler.AddNewTable(common.Table{
			SchemaID: add.SchemaID,
			TableID:  add.TableID,
		}, be.commitTs)
	}

	for _, change := range be.schemaIDChange {
		log.Info("update schema id",
			zap.String("changefeed", be.cfID),
			zap.Int64("newSchema", change.OldSchemaID),
			zap.Int64("oldSchema", change.NewSchemaID),
			zap.Int64("table", change.TableID))
		be.scheduler.UpdateSchemaID(change.TableID, change.NewSchemaID)
	}
}

func (be *BarrierEvent) allDispatcherReported() bool {
	if be.blockedDispatchers == nil {
		return true
	}
	// currently we use the task size to check if all the dispatchers reported
	// todo: we should check ddl dispatcher checkpoint ts here, because the size may affect by ddls?
	switch be.blockedDispatchers.InfluenceType {
	case heartbeatpb.InfluenceType_DB:
		return len(be.reportedDispatchers) >=
			len(be.scheduler.GetTasksBySchemaID(be.blockedDispatchers.SchemaID))
	case heartbeatpb.InfluenceType_All:
		return len(be.reportedDispatchers) >= be.scheduler.TaskSize()
	case heartbeatpb.InfluenceType_Normal:
		return len(be.reportedDispatchers) >= len(be.blockedTasks)
	}
	return false
}

func (be *BarrierEvent) sendPassAction() []*messaging.TargetMessage {
	if be.blockedDispatchers == nil {
		return []*messaging.TargetMessage{}
	}
	msgMap := make(map[node.ID]*messaging.TargetMessage)
	switch be.blockedDispatchers.InfluenceType {
	case heartbeatpb.InfluenceType_DB:
		for _, stm := range be.scheduler.GetTasksBySchemaID(be.blockedDispatchers.SchemaID) {
			if stm.Primary == "" {
				continue
			}
			_, ok := msgMap[stm.Primary]
			if !ok {
				msgMap[stm.Primary] = be.newPassActionMessage(stm.Primary)
			}
		}
	case heartbeatpb.InfluenceType_All:
		for _, n := range be.scheduler.GetAllNodes() {
			msgMap[n] = be.newPassActionMessage(n)
		}
	case heartbeatpb.InfluenceType_Normal:
		// send pass action
		for _, stm := range be.blockedTasks {
			if stm == nil || stm.Primary == "" {
				continue
			}
			replica := stm.Inferior.(*ReplicaSet)
			dispatcherID := replica.ID
			if dispatcherID == be.writerDispatcher {
				continue
			}
			msg, ok := msgMap[stm.Primary]
			if !ok {
				msg = be.newPassActionMessage(stm.Primary)
				msgMap[stm.Primary] = msg
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
	be.reportedDispatchers[dispatcherID] = true
	// all dispatcher reported heartbeat, select the last one to write
	if be.allDispatcherReported() {
		be.writerDispatcher = dispatcherID
		be.selected = true
		// release memory
		be.reportedDispatchers = nil

		log.Info("all dispatcher reported heartbeat, select one to write",
			zap.String("changefeed", be.cfID),
			zap.String("dispatcher", dispatcherID.String()),
			zap.Uint64("commitTs", be.commitTs),
			zap.String("barrierType", be.blockedDispatchers.InfluenceType.String()))
		return &heartbeatpb.DispatcherAction{
			Action:   heartbeatpb.Action_Write,
			CommitTs: be.commitTs,
		}
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
		stm := be.scheduler.GetTask(be.writerDispatcher)
		if stm == nil || stm.Primary == "" {
			return nil
		}
		return []*messaging.TargetMessage{be.newWriterActionMessage(stm.Primary)}
	}
	// the writer dispatcher is advanced, resend pass action
	return be.sendPassAction()
}

func (be *BarrierEvent) newWriterActionMessage(capture node.ID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(capture, messaging.HeartbeatCollectorTopic,
		&heartbeatpb.HeartBeatResponse{DispatcherStatuses: []*heartbeatpb.DispatcherStatus{
			{
				Action: &heartbeatpb.DispatcherAction{
					Action:   heartbeatpb.Action_Write,
					CommitTs: be.commitTs,
				},
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
		&heartbeatpb.HeartBeatResponse{DispatcherStatuses: []*heartbeatpb.DispatcherStatus{
			{
				Action: &heartbeatpb.DispatcherAction{
					Action:   heartbeatpb.Action_Pass,
					CommitTs: be.commitTs,
				},
				InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
					InfluenceType:       be.blockedDispatchers.InfluenceType,
					SchemaID:            be.blockedDispatchers.SchemaID,
					ExcludeDispatcherId: be.writerDispatcher.ToPB(),
				},
			},
		}})
}
