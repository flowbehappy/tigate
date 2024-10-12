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
// 2. maintainer wait for all dispatchers reporting block event (all dispatchers must report the same event)
// 3. maintainer choose one dispatcher to write(tack an action) the event to downstream, (resend logic is needed)
// 4. maintainer wait for the selected dispatcher reporting event(write) done message (resend logic is needed)
// 5. maintainer send pass action to all other dispatchers. (resend logic is needed)
// 6. maintainer wait for all dispatchers reporting event(pass) done message
// 7. maintainer clear the event
type Barrier struct {
	blockedTs  map[eventKey]*BarrierEvent
	controller *Controller
}

// eventKey is the key of the block event,
// the ddl and sync point are identified by the blockTs and isSyncPoint since they can share the same blockTs
type eventKey struct {
	blockTs     uint64
	isSyncPoint bool
}

// NewBarrier create a new barrier for the changefeed
func NewBarrier(controller *Controller) *Barrier {
	return &Barrier{
		blockedTs:  make(map[eventKey]*BarrierEvent),
		controller: controller,
	}
}

// HandleStatus handle the block status from dispatcher manager
func (b *Barrier) HandleStatus(from node.ID,
	request *heartbeatpb.BlockStatusRequest) *messaging.TargetMessage {
	var dispatcherStatus []*heartbeatpb.DispatcherStatus
	for _, status := range request.BlockStatuses {
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

func (b *Barrier) handleOneStatus(changefeedID string, status *heartbeatpb.TableSpanBlockStatus) *heartbeatpb.DispatcherStatus {
	dispatcherID := common.NewDispatcherIDFromPB(status.ID)
	if status.State.EventDone {
		b.handleEventDone(dispatcherID, status)
		return nil
	}
	return b.handleBlockState(changefeedID, dispatcherID, status)
}

func (b *Barrier) handleEventDone(dispatcherID common.DispatcherID, status *heartbeatpb.TableSpanBlockStatus) {
	key := getEventKey(status.State)
	event, ok := b.blockedTs[key]
	// no block event found
	if !ok {
		return
	}

	// there is a block event and the dispatcher write or pass action already
	// which means we have sent pass or write action to it
	// the writer already synced ddl to downstream
	if event.writerDispatcher == dispatcherID {
		// schedule new and removed tasks
		// the pass action will be sent periodically in resend logic if not acked
		event.scheduleBlockEvent()
		event.writerDispatcherAdvanced = true
	}

	// checkpoint ts is advanced, clear the map, so do not need to resend message anymore
	event.markDispatcherEventDone(dispatcherID)
	// all blocked dispatchers are reported event done, we can clean up the event
	if event.allDispatcherDone() {
		delete(b.blockedTs, key)
	}
}

func (b *Barrier) handleBlockState(changefeedID string,
	dispatcherID common.DispatcherID,
	status *heartbeatpb.TableSpanBlockStatus) *heartbeatpb.DispatcherStatus {
	blockState := status.State
	dispatcherStatus := &heartbeatpb.DispatcherStatus{
		InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			DispatcherIDs: []*heartbeatpb.DispatcherID{
				dispatcherID.ToPB(),
			},
		},
		Ack: &heartbeatpb.ACK{CommitTs: blockState.BlockTs, IsSyncPoint: blockState.IsSyncPoint},
	}
	if blockState.IsBlocked {
		key := getEventKey(blockState)
		// insert an event, or get the old one event check if the event is already tracked
		event := b.getOrInsertNewEvent(changefeedID, key, blockState)
		// check if all dispatchers already reported the block event, and check whether we need to send write action
		dispatcherStatus.Action = event.dispatcherReachedBlockTs(dispatcherID)
	} else {
		// it's not a blocked event, it must be sent by table event trigger dispatcher
		// and the ddl already synced to downstream , e.g.: create table, drop table
		// if ack failed, dispatcher will send a heartbeat again, so we do not need to care about resend message here
		NewBlockEvent(changefeedID, b.controller, blockState).scheduleBlockEvent()
	}
	return dispatcherStatus
}

// getOrInsertNewEvent get the block event from the map, if not found, create a new one
func (b *Barrier) getOrInsertNewEvent(changefeedID string, key eventKey,
	blockState *heartbeatpb.State) *BarrierEvent {
	event, ok := b.blockedTs[key]
	if !ok {
		event = NewBlockEvent(changefeedID, b.controller, blockState)
		b.blockedTs[key] = event
	}
	return event
}

// getEventKey returns the key of the block event
func getEventKey(blockState *heartbeatpb.State) eventKey {
	return eventKey{
		blockTs:     blockState.BlockTs,
		isSyncPoint: blockState.IsSyncPoint,
	}
}
