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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/spanz"
	"go.uber.org/zap"
)

type Barrier struct {
	blockedTs map[uint64]*BlockEvent
	scheduler *Scheduler
	// if maintainer is down, the barrier will be re-built, so we can use the dispatcher as the key
	blockedDispatcher map[common.DispatcherID]*BlockEvent
}

func NewBarrier(scheduler *Scheduler) *Barrier {
	return &Barrier{
		blockedTs:         make(map[uint64]*BlockEvent),
		blockedDispatcher: make(map[common.DispatcherID]*BlockEvent),
		scheduler:         scheduler,
	}
}

func (b *Barrier) HandleStatus(from messaging.ServerId,
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
	// todo add resend logic
	return nil
}

func (b *Barrier) handleOneStatus(changefeedID string, status *heartbeatpb.TableSpanStatus) ([]*messaging.TargetMessage, *heartbeatpb.DispatcherStatus, error) {
	dispatcherID := common.NewDispatcherIDFromPB(status.ID)
	var (
		msgs            []*messaging.TargetMessage
		distacherStatus *heartbeatpb.DispatcherStatus
		err             error
	)
	if status.State == nil {
		msgs, err = b.handleNoStateHeartbeat(dispatcherID, status.CheckpointTs)
		return msgs, distacherStatus, err
	} else {
		msgs, distacherStatus, err = b.handleStateHeartbeat(changefeedID, dispatcherID, status)
	}
	return msgs, distacherStatus, nil
}

func (b *Barrier) handleNoStateHeartbeat(dispatcherID common.DispatcherID, checkpointTs uint64) ([]*messaging.TargetMessage, error) {
	event, ok := b.blockedDispatcher[dispatcherID]
	// no block event found
	if !ok {
		return nil, nil
	}
	var (
		err  error
		msgs []*messaging.TargetMessage
	)
	// there is a block event and the dispatcher advanced its checkpoint ts
	// which means we have sent pass or write action to it
	if checkpointTs > event.commitTs {
		// the writer already synced ddl to downstream
		if event.selectedDispatcher == dispatcherID {
			// schedule new and removed tasks
			msgs, err = event.scheduleBlockEvent()
			if err != nil {
				return nil, errors.Trace(err)
			}
			msgs = append(msgs, event.sendPassAction()...)
		}

		// checkpoint ts is advanced, clear the map, so do not need to resend message anymore
		delete(b.blockedDispatcher, dispatcherID)
		// all blocked dispatchers are advanced checkpoint ts
		if len(b.blockedDispatcher) == 0 {
			delete(b.blockedTs, event.commitTs)
		}
	}
	return msgs, nil
}

func (b *Barrier) handleStateHeartbeat(changefeedID string,
	dispatcherID common.DispatcherID,
	status *heartbeatpb.TableSpanStatus) ([]*messaging.TargetMessage, *heartbeatpb.DispatcherStatus, error) {
	var (
		msgs            []*messaging.TargetMessage
		distacherStatus *heartbeatpb.DispatcherStatus
		err             error
	)
	blockState := status.State
	if blockState.IsBlocked {
		event, ok := b.blockedTs[blockState.BlockTs]
		ack := &heartbeatpb.DispatcherStatus{
			InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
				InfluenceType: heartbeatpb.InfluenceType_Normal,
			},
			Ack: &heartbeatpb.ACK{CommitTs: status.CheckpointTs}}
		if !ok {
			event = NewBlockEvent(changefeedID, b.scheduler, blockState)
			b.blockedTs[blockState.BlockTs] = event
		}
		_, ok = b.blockedDispatcher[dispatcherID]
		if !ok {
			b.blockedDispatcher[dispatcherID] = event
		}

		event.blockedDispatcherMap[dispatcherID] = true
		// all dispatcher reported heartbeat, select one to write
		if !event.selected && event.allDispatcherReported() {
			ack.Action = &heartbeatpb.DispatcherAction{
				Action:   heartbeatpb.Action_Write,
				CommitTs: status.CheckpointTs,
			}
			event.selectedDispatcher = common.NewDispatcherIDFromPB(dispatcherID.ToPB())
			event.selected = true
		}
		distacherStatus = ack
	} else {
		// not blocked event, it must be sent by table event trigger dispatcher
		// the ddl already synced to downstream , e.g.: create table, drop table
		distacherStatus = &heartbeatpb.DispatcherStatus{
			InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
				InfluenceType: heartbeatpb.InfluenceType_Normal,
			},
			Ack: &heartbeatpb.ACK{CommitTs: status.CheckpointTs},
		}
		msgs, err = NewBlockEvent(changefeedID, b.scheduler, blockState).scheduleBlockEvent()
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	}
	return msgs, distacherStatus, nil
}

type BlockEvent struct {
	cfID               string
	commitTs           uint64
	scheduler          *Scheduler
	selected           bool
	selectedDispatcher common.DispatcherID
	// todo: support big set of dispatcher, like sync point, create database
	blockedDispatcherMap map[common.DispatcherID]bool
	newTables            []*heartbeatpb.Table
	dropDispatcherIDs    *heartbeatpb.InfluencedDispatchers
}

func NewBlockEvent(cfID string, scheduler *Scheduler,
	status *heartbeatpb.State) *BlockEvent {
	event := &BlockEvent{
		scheduler:            scheduler,
		selected:             false,
		cfID:                 cfID,
		commitTs:             status.BlockTs,
		newTables:            status.NeedAddedTables,
		dropDispatcherIDs:    status.NeedDroppedDispatchers,
		blockedDispatcherMap: make(map[common.DispatcherID]bool),
	}
	if status.BlockDispatchers != nil {
		for _, block := range status.BlockDispatchers.DispatcherIDs {
			event.blockedDispatcherMap[common.NewDispatcherIDFromPB(block)] = false
		}
	}
	return event
}

func (b *BlockEvent) scheduleBlockEvent() ([]*messaging.TargetMessage, error) {
	var msgs []*messaging.TargetMessage
	for _, removed := range b.dropDispatcherIDs.DispatcherIDs {
		msg, err := b.scheduler.RemoveTask(common.NewDispatcherIDFromPB(removed))
		if err != nil {
			return nil, errors.Trace(err)
		}
		if msg != nil {
			msgs = append(msgs, msg)
		}
	}
	for _, add := range b.newTables {
		span := spanz.TableIDToComparableSpan(add.TableID)
		tableSpan := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{
			TableID:  uint64(add.TableID),
			StartKey: span.StartKey,
			EndKey:   span.EndKey,
		}}
		dispatcherID := common.NewDispatcherID()
		replicaSet := NewReplicaSet(model.DefaultChangeFeedID(b.cfID),
			dispatcherID, tableSpan, b.commitTs).(*ReplicaSet)
		stm, err := scheduler.NewStateMachine(dispatcherID, nil, replicaSet)
		if err != nil {
			log.Panic("failed to create state machine", zap.Error(err))
		}
		b.scheduler.AddNewTask(stm)
	}
	// todo: trigger a schedule event support filter, rename table or databases
	msgList, err := b.scheduler.Schedule()
	if err != nil {
		return nil, errors.Trace(err)
	}
	msgs = append(msgs, msgList...)
	return msgs, nil
}

func (b *BlockEvent) allDispatcherReported() bool {
	for _, value := range b.blockedDispatcherMap {
		if !value {
			return false
		}
	}
	return true
}

func (b *BlockEvent) sendPassAction() []*messaging.TargetMessage {
	var msgs = make([]*messaging.TargetMessage, 0, len(b.blockedDispatcherMap))
	// send pass action
	for dispatcherID, _ := range b.blockedDispatcherMap {
		// skip write dispatcher, we already send the write action
		if b.selectedDispatcher == dispatcherID {
			continue
		}
		stm := b.scheduler.GetTask(dispatcherID)
		if stm != nil {
			//todo: group DispatcherStatus by servers
			msgs = append(msgs, messaging.NewSingleTargetMessage(messaging.ServerId(stm.Primary),
				messaging.HeartbeatCollectorTopic,
				&heartbeatpb.HeartBeatResponse{DispatcherStatuses: []*heartbeatpb.DispatcherStatus{
					{
						InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
							InfluenceType: heartbeatpb.InfluenceType_Normal,
						},
						Action: &heartbeatpb.DispatcherAction{
							Action:   heartbeatpb.Action_Pass,
							CommitTs: b.commitTs,
						},
					},
				}}))
		}
	}
	return msgs
}
