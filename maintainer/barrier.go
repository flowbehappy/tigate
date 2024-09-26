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

	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/scheduler"
)

// Barrier manage the block events for the changefeed
// the block event processing logic:
// 1. dispatcher report an event to maintainer, like ddl, sync point
// 2. maintainer wait for all dispatcher to reach the same commit ts (all reported the same event)
// 3. maintainer choose one dispatcher to write(tack an action) the event to downstream, (resend needed)
// 4. maintainer wait for the selected dispatcher advance its checkpoint ts,(means it already finished the write action)
// 5. maintainer send pass action to all other dispatchers. (resend needed)
// 6. maintainer wait for all dispatchers advance checkpoints, and cleanup memory
type Barrier struct {
	blockedTs map[uint64]*BarrierEvent
	scheduler *Scheduler
	// if maintainer is down, the barrier will be re-built, so we can use the dispatcher as the key
	blockedDispatcher map[common.DispatcherID]*BarrierEvent
}

func NewBarrier(scheduler *Scheduler) *Barrier {
	return &Barrier{
		blockedTs:         make(map[uint64]*BarrierEvent),
		blockedDispatcher: make(map[common.DispatcherID]*BarrierEvent),
		scheduler:         scheduler,
	}
}

func (b *Barrier) HandleStatus(from node.ID,
	request *heartbeatpb.HeartBeatRequest) ([]*messaging.TargetMessage, error) {
	var msgs []*messaging.TargetMessage
	var dispatcherStatus []*heartbeatpb.DispatcherStatus
	for _, status := range request.Statuses {
		msgsList, resp, err := b.handleOneStatus(request.ChangefeedID, status)
		if err != nil {
			return nil, err
		}
		if resp != nil {
			dispatcherStatus = append(dispatcherStatus, resp)
		}
		if msgsList != nil {
			msgs = append(msgs, msgsList...)
		}
	}
	// send ack message to dispatcher
	if len(dispatcherStatus) > 0 {
		msgs = append(msgs, messaging.NewSingleTargetMessage(from,
			messaging.HeartbeatCollectorTopic,
			&heartbeatpb.HeartBeatResponse{
				ChangefeedID:       request.ChangefeedID,
				DispatcherStatuses: dispatcherStatus,
			}))
	}
	return msgs, nil
}

func (b *Barrier) Resend() []*messaging.TargetMessage {
	var msgs []*messaging.TargetMessage
	for _, event := range b.blockedDispatcher {
		if time.Since(event.lastResendTime) < time.Second*2 {
			continue
		}
		// still waiting for all dispatcher to reach the block commit ts
		if !event.selected {
			continue
		}
		if !event.writerDispatcherAdvanced {
			//resend write action
			stm := b.scheduler.GetTask(event.writerDispatcher)
			if stm == nil || stm.Primary == "" {
				continue
			}
			msg := messaging.NewSingleTargetMessage(stm.Primary, messaging.HeartbeatCollectorTopic,
				&heartbeatpb.HeartBeatResponse{DispatcherStatuses: []*heartbeatpb.DispatcherStatus{
					{
						Action: &heartbeatpb.DispatcherAction{
							Action:   heartbeatpb.Action_Write,
							CommitTs: event.commitTs,
						},
						InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
							InfluenceType: event.blockedDispatchers.InfluenceType,
							SchemaID:      event.blockedDispatchers.SchemaID,
							DispatcherIDs: []*heartbeatpb.DispatcherID{
								event.writerDispatcher.ToPB(),
							},
						},
					},
				}})
			msgs = append(msgs, msg)
		} else {
			//resend pass action
			msgs = append(msgs, event.sendPassAction()...)
		}
		event.lastResendTime = time.Now()
	}
	return msgs
}

func (b *Barrier) handleOneStatus(changefeedID string, status *heartbeatpb.TableSpanStatus) ([]*messaging.TargetMessage, *heartbeatpb.DispatcherStatus, error) {
	dispatcherID := common.NewDispatcherIDFromPB(status.ID)
	if status.State == nil {
		return b.handleNoStateHeartbeat(dispatcherID, status.CheckpointTs)
	}
	return b.handleStateHeartbeat(changefeedID, dispatcherID, status)
}

func (b *Barrier) handleNoStateHeartbeat(dispatcherID common.DispatcherID, checkpointTs uint64) ([]*messaging.TargetMessage, *heartbeatpb.DispatcherStatus, error) {
	event, ok := b.blockedDispatcher[dispatcherID]
	// no block event found
	if !ok {
		return nil, nil, nil
	}
	// no block event send ,but reached the block point
	if checkpointTs == event.commitTs {
		event.advancedDispatchers[dispatcherID] = true
		// all dispatcher reported heartbeat, select one to write
		if !event.selected && event.allDispatcherReported() {
			dispatcherStatus := &heartbeatpb.DispatcherStatus{
				InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
					InfluenceType: heartbeatpb.InfluenceType_Normal,
					DispatcherIDs: []*heartbeatpb.DispatcherID{dispatcherID.ToPB()},
				},
				Action: &heartbeatpb.DispatcherAction{
					Action:   heartbeatpb.Action_Write,
					CommitTs: event.commitTs,
				}}
			event.writerDispatcher = dispatcherID
			event.selected = true
			return nil, dispatcherStatus, nil
		}
	}

	var msgs []*messaging.TargetMessage
	// there is a block event and the dispatcher advanced its checkpoint ts
	// which means we have sent pass or write action to it
	if checkpointTs > event.commitTs {
		// the writer already synced ddl to downstream
		if event.writerDispatcher == dispatcherID {
			// send pass action to all
			msgs = append(msgs, event.sendPassAction()...)
			// schedule new and removed tasks
			event.scheduleBlockEvent()
			event.writerDispatcherAdvanced = true
		}

		// checkpoint ts is advanced, clear the map, so do not need to resend message anymore
		delete(b.blockedDispatcher, dispatcherID)
		// all blocked dispatchers are advanced checkpoint ts
		if len(b.blockedDispatcher) == 0 {
			delete(b.blockedTs, event.commitTs)
		}
	}
	return msgs, nil, nil
}

func (b *Barrier) handleStateHeartbeat(changefeedID string,
	dispatcherID common.DispatcherID,
	status *heartbeatpb.TableSpanStatus) ([]*messaging.TargetMessage, *heartbeatpb.DispatcherStatus, error) {
	var (
		msgs            []*messaging.TargetMessage
		distacherStatus *heartbeatpb.DispatcherStatus
	)
	blockState := status.State
	if blockState.IsBlocked {
		event, ok := b.blockedTs[blockState.BlockTs]
		ack := &heartbeatpb.DispatcherStatus{
			InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
				InfluenceType: heartbeatpb.InfluenceType_Normal,
				DispatcherIDs: []*heartbeatpb.DispatcherID{dispatcherID.ToPB()},
			},
			Ack: &heartbeatpb.ACK{CommitTs: blockState.BlockTs}}
		if !ok {
			event = NewBlockEvent(changefeedID, b.scheduler, blockState)
			b.blockedTs[blockState.BlockTs] = event
		}
		_, ok = b.blockedDispatcher[dispatcherID]
		if !ok {
			b.blockedDispatcher[dispatcherID] = event
		}

		event.advancedDispatchers[dispatcherID] = true
		// all dispatcher reported heartbeat, select one to write
		if !event.selected && event.allDispatcherReported() {
			ack.Action = &heartbeatpb.DispatcherAction{
				Action:   heartbeatpb.Action_Write,
				CommitTs: event.commitTs,
			}
			event.writerDispatcher = dispatcherID
			event.selected = true
		}
		distacherStatus = ack
	} else {
		// it's not a blocked event, it must be sent by table event trigger dispatcher
		// the ddl already synced to downstream , e.g.: create table, drop table
		distacherStatus = &heartbeatpb.DispatcherStatus{
			InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
				InfluenceType: heartbeatpb.InfluenceType_Normal,
				DispatcherIDs: []*heartbeatpb.DispatcherID{
					dispatcherID.ToPB(),
				},
			},
			Ack: &heartbeatpb.ACK{CommitTs: blockState.BlockTs},
		}
		// if ack failed, dispatcher will send a heartbeat again
		NewBlockEvent(changefeedID, b.scheduler, blockState).scheduleBlockEvent()
	}
	return msgs, distacherStatus, nil
}

type BarrierEvent struct {
	cfID                     string
	commitTs                 uint64
	scheduler                *Scheduler
	selected                 bool
	writerDispatcher         common.DispatcherID
	writerDispatcherAdvanced bool
	newTables                []*heartbeatpb.Table
	blockedDispatchers       *heartbeatpb.InfluencedTables
	dropDispatchers          *heartbeatpb.InfluencedTables

	blockedTasks []*scheduler.StateMachine[common.DispatcherID]
	dropTasks    []*scheduler.StateMachine[common.DispatcherID]

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
		lastResendTime:      time.Now(),
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
			b.scheduler.RemoveAllTasks()
		}
	}
	for _, add := range b.newTables {
		log.Info(" add new table",
			zap.String("changefeed", b.cfID),
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
			msg, ok := msgMap[stm.Primary]
			if !ok {
				msg = messaging.NewSingleTargetMessage(stm.Primary, messaging.HeartbeatCollectorTopic,
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
				msgMap[stm.Primary] = msg
			}
		}
	case heartbeatpb.InfluenceType_All:
		for _, n := range b.scheduler.GetAllNodes() {
			msg := messaging.NewSingleTargetMessage(n, messaging.HeartbeatCollectorTopic,
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
			msgMap[n] = msg
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
				msg = messaging.NewSingleTargetMessage(stm.Primary, messaging.HeartbeatCollectorTopic,
					&heartbeatpb.HeartBeatResponse{DispatcherStatuses: []*heartbeatpb.DispatcherStatus{
						{
							Action: &heartbeatpb.DispatcherAction{
								Action:   heartbeatpb.Action_Pass,
								CommitTs: b.commitTs,
							},
							InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
								InfluenceType: b.blockedDispatchers.InfluenceType,
							},
						},
					}})
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
