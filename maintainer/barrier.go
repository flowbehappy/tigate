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
)

// Barrier manage the block events for the changefeed
// the block event processing logic:
// 1. dispatcher report an event to maintainer, like ddl, sync point
// 2. maintainer wait for all dispatchers to reach the same commit ts (all dispatchers will report the same event)
// 3. maintainer choose one dispatcher to write(tack an action) the event to downstream, (resend logic is needed)
// 4. maintainer wait for the selected dispatcher advance its checkpoint ts,(means it already finished the write action), (resend logic is needed)
// 5. maintainer send pass action to all other dispatchers. (resend logic is needed)
// 6. maintainer wait for all dispatchers advance checkpoints, and cleanup memory
type Barrier struct {
	blockedTs map[uint64]*BarrierEvent
	scheduler *Scheduler
	// if maintainer is down, the barrier will be re-built, so we can use the dispatcher as the key
	blockedDispatcher map[common.DispatcherID]*BarrierEvent
}

// NewBarrier create a new barrier for the changefeed
func NewBarrier(scheduler *Scheduler) *Barrier {
	return &Barrier{
		blockedTs:         make(map[uint64]*BarrierEvent),
		blockedDispatcher: make(map[common.DispatcherID]*BarrierEvent),
		scheduler:         scheduler,
	}
}

// HandleStatus handle the heartbeat status from dispatcher manager
func (b *Barrier) HandleStatus(from node.ID,
	request *heartbeatpb.HeartBeatRequest) *messaging.TargetMessage {
	var dispatcherStatus []*heartbeatpb.DispatcherStatus
	for _, status := range request.Statuses {
		resp := b.handleOneStatus(request.ChangefeedID, status)
		if resp != nil {
			dispatcherStatus = append(dispatcherStatus, resp)
		}
	}
	// send ack or write action message to dispatcher
	if len(dispatcherStatus) > 0 {
		return messaging.NewSingleTargetMessage(from,
			messaging.HeartbeatCollectorTopic,
			&heartbeatpb.HeartBeatResponse{
				ChangefeedID:       request.ChangefeedID,
				DispatcherStatuses: dispatcherStatus,
			})
	}
	return nil
}

// Resend resends the message to the dispatcher manger, the pass action is handle here
func (b *Barrier) Resend() []*messaging.TargetMessage {
	var msgs []*messaging.TargetMessage
	for _, event := range b.blockedTs {
		//todo: we can limit the number of messages to send in one round here
		msgs = append(msgs, event.resend()...)
	}
	return msgs
}

func (b *Barrier) handleOneStatus(changefeedID string, status *heartbeatpb.TableSpanStatus) *heartbeatpb.DispatcherStatus {
	dispatcherID := common.NewDispatcherIDFromPB(status.ID)
	if status.State == nil {
		return b.handleNoStateHeartbeat(dispatcherID, status.CheckpointTs)
	}
	return b.handleStateHeartbeat(changefeedID, dispatcherID, status)
}

func (b *Barrier) handleNoStateHeartbeat(dispatcherID common.DispatcherID, checkpointTs uint64) *heartbeatpb.DispatcherStatus {
	event, ok := b.blockedDispatcher[dispatcherID]
	// no block event found
	if !ok {
		return nil
	}
	// no block event send ,but reached the block point
	if checkpointTs == event.commitTs {
		action := event.dispatcherReachedBlockTs(dispatcherID)
		// all dispatcher reported heartbeat, select one to write
		if action != nil {
			dispatcherStatus := &heartbeatpb.DispatcherStatus{
				InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
					InfluenceType: heartbeatpb.InfluenceType_Normal,
					DispatcherIDs: []*heartbeatpb.DispatcherID{event.writerDispatcher.ToPB()},
				},
				Action: action,
			}
			return dispatcherStatus
		}
	}

	// there is a block event and the dispatcher advanced its checkpoint ts
	// which means we have sent pass or write action to it
	if checkpointTs > event.commitTs {
		// the writer already synced ddl to downstream
		if event.writerDispatcher == dispatcherID {
			// schedule new and removed tasks
			// the pass action will be sent periodically in resend logic if not acked
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
	return nil
}

func (b *Barrier) handleStateHeartbeat(changefeedID string,
	dispatcherID common.DispatcherID,
	status *heartbeatpb.TableSpanStatus) *heartbeatpb.DispatcherStatus {
	blockState := status.State
	dispatcherStatus := &heartbeatpb.DispatcherStatus{
		InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			DispatcherIDs: []*heartbeatpb.DispatcherID{
				dispatcherID.ToPB(),
			},
		},
		Ack: &heartbeatpb.ACK{CommitTs: blockState.BlockTs},
	}
	if blockState.IsBlocked {
		// insert an event, or get the old one event check if the event is already tracked
		event := b.getOrInsertNewEvent(changefeedID, dispatcherID, blockState)
		// the dispatcher already reached the block event block ts, check whether we need to send write action
		if status.CheckpointTs == blockState.BlockTs {
			dispatcherStatus.Action = event.dispatcherReachedBlockTs(dispatcherID)
		}
	} else {
		// it's not a blocked event, it must be sent by table event trigger dispatcher
		// the ddl already synced to downstream , e.g.: create table, drop table
		// if ack failed, dispatcher will send a heartbeat again
		NewBlockEvent(changefeedID, b.scheduler, blockState).scheduleBlockEvent()
	}
	return dispatcherStatus
}

func (b *Barrier) getOrInsertNewEvent(changefeedID string,
	dispatcherID common.DispatcherID,
	blockState *heartbeatpb.State) *BarrierEvent {
	event, ok := b.blockedTs[blockState.BlockTs]
	if !ok {
		event = NewBlockEvent(changefeedID, b.scheduler, blockState)
		b.blockedTs[blockState.BlockTs] = event
	}
	_, ok = b.blockedDispatcher[dispatcherID]
	if !ok {
		b.blockedDispatcher[dispatcherID] = event
	}
	return event
}
