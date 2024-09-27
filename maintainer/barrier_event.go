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
	scheduler                *Scheduler
	selected                 bool
	writerDispatcher         common.DispatcherID
	writerDispatcherAdvanced bool

	blockedDispatchers *heartbeatpb.InfluencedTables
	dropDispatchers    *heartbeatpb.InfluencedTables

	blockedTasks []*scheduler.StateMachine[common.DispatcherID]
	dropTasks    []*scheduler.StateMachine[common.DispatcherID]
	newTables    []*heartbeatpb.Table

	advancedDispatchers map[common.DispatcherID]bool
	lastResendTime      time.Time
}

func NewBlockEvent(cfID string, scheduler *Scheduler,
	status *heartbeatpb.State) *BarrierEvent {
	event := &BarrierEvent{
		scheduler:           scheduler,
		selected:            false,
		cfID:                cfID,
		commitTs:            status.BlockTs,
		blockedDispatchers:  status.BlockTables,
		newTables:           status.NeedAddedTables,
		dropDispatchers:     status.NeedDroppedTables,
		advancedDispatchers: make(map[common.DispatcherID]bool),
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

func (b *BarrierEvent) scheduleBlockEvent() {
	// dispatcher notify us to drop some tables, by dispatcher ID or schema ID
	if b.dropDispatchers != nil {
		switch b.dropDispatchers.InfluenceType {
		case heartbeatpb.InfluenceType_DB:
			for _, stm := range b.scheduler.GetTasksBySchemaID(b.dropDispatchers.SchemaID) {
				log.Info(" remove table",
					zap.String("changefeed", b.cfID),
					zap.String("table", stm.ID.String()))
				b.scheduler.RemoveTask(stm)
			}
		case heartbeatpb.InfluenceType_Normal:
			for _, stm := range b.dropTasks {
				log.Info(" remove table",
					zap.String("changefeed", b.cfID),
					zap.String("table", stm.ID.String()))
				b.scheduler.RemoveTask(stm)
			}
		case heartbeatpb.InfluenceType_All:
			log.Info("remove all tables by barrier", zap.String("changefeed", b.cfID))
			b.scheduler.RemoveAllTasks()
		}
	}
	for _, add := range b.newTables {
		log.Info(" add new table",
			zap.String("changefeed", b.cfID),
			zap.Int64("schema", add.SchemaID),
			zap.Int64("table", add.TableID))
		b.scheduler.AddNewTable(common.Table{
			SchemaID: add.SchemaID,
			TableID:  add.TableID,
		}, b.commitTs)
	}
}

func (b *BarrierEvent) allDispatcherReported() bool {
	if b.blockedDispatchers == nil {
		return true
	}
	// currently we use the task size to check if all the dispatchers reported
	// todo: we should check ddl dispatcher checkpoint ts here, because the size may affect by ddls?
	switch b.blockedDispatchers.InfluenceType {
	case heartbeatpb.InfluenceType_DB:
		return len(b.advancedDispatchers) >=
			len(b.scheduler.GetTasksBySchemaID(b.blockedDispatchers.SchemaID))
	case heartbeatpb.InfluenceType_All:
		return len(b.advancedDispatchers) >= b.scheduler.TaskSize()
	case heartbeatpb.InfluenceType_Normal:
		return len(b.advancedDispatchers) >= len(b.blockedTasks)
	}
	return false
}

func (b *BarrierEvent) sendPassAction() []*messaging.TargetMessage {
	if b.blockedDispatchers == nil {
		return []*messaging.TargetMessage{}
	}
	msgMap := make(map[node.ID]*messaging.TargetMessage)
	switch b.blockedDispatchers.InfluenceType {
	case heartbeatpb.InfluenceType_DB:
		for _, stm := range b.scheduler.GetTasksBySchemaID(b.blockedDispatchers.SchemaID) {
			if stm.Primary == "" {
				continue
			}
			_, ok := msgMap[stm.Primary]
			if !ok {
				msgMap[stm.Primary] = b.newPassActionMessage(stm.Primary)
			}
		}
	case heartbeatpb.InfluenceType_All:
		for _, n := range b.scheduler.GetAllNodes() {
			msgMap[n] = b.newPassActionMessage(n)
		}
	case heartbeatpb.InfluenceType_Normal:
		// send pass action
		for _, stm := range b.blockedTasks {
			if stm == nil || stm.Primary == "" {
				continue
			}
			replica := stm.Inferior.(*ReplicaSet)
			dispatcherID := replica.ID
			if dispatcherID == b.writerDispatcher {
				continue
			}
			msg, ok := msgMap[stm.Primary]
			if !ok {
				msg = b.newPassActionMessage(stm.Primary)
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

// dispatcherReachedBlockTs check if all the dispatchers reached the block ts
func (b *BarrierEvent) dispatcherReachedBlockTs(dispatcherID common.DispatcherID) *heartbeatpb.DispatcherAction {
	if b.selected {
		return nil
	}
	b.advancedDispatchers[dispatcherID] = true
	// all dispatcher reported heartbeat, select the last one to write
	if b.allDispatcherReported() {
		b.writerDispatcher = dispatcherID
		b.selected = true
		// release memory
		b.advancedDispatchers = nil

		log.Info("all dispatcher reported heartbeat, select one to write",
			zap.String("changefeed", b.cfID),
			zap.String("dispatcher", dispatcherID.String()),
			zap.Uint64("commitTs", b.commitTs),
			zap.String("barrierType", b.blockedDispatchers.InfluenceType.String()))
		return &heartbeatpb.DispatcherAction{
			Action:   heartbeatpb.Action_Write,
			CommitTs: b.commitTs,
		}
	}
	return nil
}

func (b *BarrierEvent) resend() []*messaging.TargetMessage {
	if time.Since(b.lastResendTime) < time.Second {
		return nil
	}
	// still waiting for all dispatcher to reach the block commit ts
	if !b.selected {
		return nil
	}
	b.lastResendTime = time.Now()
	// we select a dispatcher as the writer, still waiting for that dispatcher advance it's checkpoint ts
	if !b.writerDispatcherAdvanced {
		//resend write action
		stm := b.scheduler.GetTask(b.writerDispatcher)
		if stm == nil || stm.Primary == "" {
			return nil
		}
		return []*messaging.TargetMessage{b.newWriterActionMessage(stm.Primary)}
	}
	// the writer dispatcher is advanced, resend pass action
	return b.sendPassAction()
}

func (b *BarrierEvent) newWriterActionMessage(capture node.ID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(capture, messaging.HeartbeatCollectorTopic,
		&heartbeatpb.HeartBeatResponse{DispatcherStatuses: []*heartbeatpb.DispatcherStatus{
			{
				Action: &heartbeatpb.DispatcherAction{
					Action:   heartbeatpb.Action_Write,
					CommitTs: b.commitTs,
				},
				InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
					InfluenceType: heartbeatpb.InfluenceType_Normal,
					DispatcherIDs: []*heartbeatpb.DispatcherID{
						b.writerDispatcher.ToPB(),
					},
				},
			},
		}})
}

func (b *BarrierEvent) newPassActionMessage(capture node.ID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(capture, messaging.HeartbeatCollectorTopic,
		&heartbeatpb.HeartBeatResponse{DispatcherStatuses: []*heartbeatpb.DispatcherStatus{
			{
				Action: &heartbeatpb.DispatcherAction{
					Action:   heartbeatpb.Action_Pass,
					CommitTs: b.commitTs,
				},
				InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
					InfluenceType:       b.blockedDispatchers.InfluenceType,
					SchemaID:            b.blockedDispatchers.SchemaID,
					ExcludeDispatcherId: b.writerDispatcher.ToPB(),
				},
			},
		}})
}
