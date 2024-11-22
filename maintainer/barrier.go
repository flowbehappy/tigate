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
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/range_checker"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

// Barrier manage the block events for the changefeed
// note: the dispatcher will guarantee the order of the block event.
// the block event processing logic:
// 1. dispatcher report an event to maintainer, like ddl, sync point
// 2. maintainer wait for all dispatchers reporting block event (all dispatchers must report the same event)
// 3. maintainer choose one dispatcher to write(tack an action) the event to downstream, (resend logic is needed)
// 4. maintainer wait for the selected dispatcher reporting event(write) done message (resend logic is needed)
// 5. maintainer send pass action to all other dispatchers. (resend logic is needed)
// 6. maintainer wait for all dispatchers reporting event(pass) done message
// 7. maintainer clear the event
type Barrier struct {
	blockedTs         map[eventKey]*BarrierEvent
	controller        *Controller
	splitTableEnabled bool
}

// eventKey is the key of the block event,
// the ddl and sync point are identified by the blockTs and isSyncPoint since they can share the same blockTs
type eventKey struct {
	blockTs     uint64
	isSyncPoint bool
}

// NewBarrier create a new barrier for the changefeed
func NewBarrier(controller *Controller, splitTableEnabled bool) *Barrier {
	return &Barrier{
		blockedTs:         make(map[eventKey]*BarrierEvent),
		controller:        controller,
		splitTableEnabled: splitTableEnabled,
	}
}

// HandleStatus handle the block status from dispatcher manager
func (b *Barrier) HandleStatus(from node.ID,
	request *heartbeatpb.BlockStatusRequest) *messaging.TargetMessage {
	log.Debug("handle block status", zap.String("from", from.String()),
		zap.String("changefeed", request.ChangefeedID.GetName()),
		zap.Any("detail", request))
	eventMap := make(map[*BarrierEvent][]*heartbeatpb.DispatcherID)
	var dispatcherStatus []*heartbeatpb.DispatcherStatus
	for _, status := range request.BlockStatuses {
		event := b.handleOneStatus(request.ChangefeedID, status)
		if event == nil {
			// should not happen
			log.Error("handle block status failed, event is nil",
				zap.String("from", from.String()),
				zap.String("changefeed", request.ChangefeedID.GetName()),
				zap.String("detail", status.String()))
			continue
		}
		eventMap[event] = append(eventMap[event], status.ID)
	}
	for event, dispatchers := range eventMap {
		dispatcherStatus = append(dispatcherStatus, &heartbeatpb.DispatcherStatus{
			InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
				InfluenceType: heartbeatpb.InfluenceType_Normal,
				DispatcherIDs: dispatchers,
			},
			Ack: ackEvent(event.commitTs, event.isSyncPoint),
		})
		// check if all dispatchers reported the block event
		if writeAction := b.checkEvent(event, dispatchers); writeAction != nil {
			dispatcherStatus = append(dispatcherStatus, writeAction)
		}
	}
	if len(dispatcherStatus) <= 0 {
		log.Warn("no dispatcher status to send",
			zap.String("from", from.String()),
			zap.String("changefeed", request.ChangefeedID.String()))
		return nil
	}
	// send ack or write action message to dispatcher
	return messaging.NewSingleTargetMessage(from,
		messaging.HeartbeatCollectorTopic,
		&heartbeatpb.HeartBeatResponse{
			ChangefeedID:       request.ChangefeedID,
			DispatcherStatuses: dispatcherStatus,
		})
}

// HandleBootstrapResponse rebuild the block event from the bootstrap response
func (b *Barrier) HandleBootstrapResponse(bootstrapRespMap map[node.ID]*heartbeatpb.MaintainerBootstrapResponse) {
	for _, resp := range bootstrapRespMap {
		for _, span := range resp.Spans {
			// we only care about the WAITING, WRITING and DONE stage
			if span.BlockState == nil || span.BlockState.Stage == heartbeatpb.BlockStage_NONE {
				continue
			}

			blockState := span.BlockState
			key := getEventKey(blockState.BlockTs, blockState.IsSyncPoint)
			event, ok := b.blockedTs[key]
			if !ok {
				event = NewBlockEvent(common.NewChangefeedIDFromPB(resp.ChangefeedID), b.controller, blockState, b.splitTableEnabled)
				b.blockedTs[key] = event
			}
			switch blockState.Stage {
			case heartbeatpb.BlockStage_WAITING:
				// it's the dispatcher's responsibility to resend the block event
			case heartbeatpb.BlockStage_WRITING:
				// it's in writing stage, must be the writer dispatcher
				// it's the maintainer's responsibility to resend the write action
				event.selected = true
				event.writerDispatcher = common.NewDispatcherIDFromPB(span.ID)
			case heartbeatpb.BlockStage_DONE:
				// it's the maintainer's responsibility to resend the pass action
				event.selected = true
				event.writerDispatcherAdvanced = true
			}
			event.markDispatcherEventDone(common.NewDispatcherIDFromPB(span.ID))
		}
	}
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

func (b *Barrier) handleOneStatus(changefeedID *heartbeatpb.ChangefeedID, status *heartbeatpb.TableSpanBlockStatus) *BarrierEvent {
	cfID := common.NewChangefeedIDFromPB(changefeedID)
	dispatcherID := common.NewDispatcherIDFromPB(status.ID)

	// when a span send a block event, its checkpint must reached status.State.BlockTs - 1,
	// so here we forward the span's checkpoint ts to status.State.BlockTs - 1
	span := b.controller.GetTask(dispatcherID)
	if span != nil {
		span.UpdateStatus(&heartbeatpb.TableSpanStatus{
			ID:              status.ID,
			CheckpointTs:    status.State.BlockTs - 1,
			ComponentStatus: heartbeatpb.ComponentState_Working,
		})
	}
	if status.State.Stage == heartbeatpb.BlockStage_DONE {
		return b.handleEventDone(cfID, dispatcherID, status)
	}
	return b.handleBlockState(cfID, dispatcherID, status)
}

func (b *Barrier) handleEventDone(changefeedID common.ChangeFeedID, dispatcherID common.DispatcherID, status *heartbeatpb.TableSpanBlockStatus) *BarrierEvent {
	key := getEventKey(status.State.BlockTs, status.State.IsSyncPoint)
	event, ok := b.blockedTs[key]
	if !ok {
		// no block event found
		be := NewBlockEvent(changefeedID, b.controller, status.State, b.splitTableEnabled)
		// the event is a fake event, the dispatcher will not send the block event
		be.rangeChecker = range_checker.NewBoolRangeChecker(false)
		return be
	}

	// there is a block event and the dispatcher write or pass action already
	// which means we have sent pass or write action to it
	// the writer already synced ddl to downstream
	if event.writerDispatcher == dispatcherID {
		// the pass action will be sent periodically in resend logic if not acked
		event.writerDispatcherAdvanced = true
	}

	// checkpoint ts is advanced, clear the map, so do not need to resend message anymore
	event.markDispatcherEventDone(dispatcherID)
	return event
}

func (b *Barrier) handleBlockState(changefeedID common.ChangeFeedID,
	dispatcherID common.DispatcherID,
	status *heartbeatpb.TableSpanBlockStatus) *BarrierEvent {
	blockState := status.State
	if blockState.IsBlocked {
		key := getEventKey(blockState.BlockTs, blockState.IsSyncPoint)
		// insert an event, or get the old one event check if the event is already tracked
		event := b.getOrInsertNewEvent(changefeedID, key, blockState)
		if dispatcherID == b.controller.ddlDispatcherID {
			log.Info("the block event is sent by ddl dispatcher",
				zap.String("changefeed", changefeedID.Name()),
				zap.String("dispatcher", dispatcherID.String()),
				zap.Uint64("commitTs", blockState.BlockTs))
			event.tableTriggerDispatcherRelated = true
		}
		if event.selected {
			// the event already in the selected state, ignore the block event just sent ack
			log.Warn("the block event already selected, ignore the block event",
				zap.String("changefeed", changefeedID.Name()),
				zap.String("dispatcher", dispatcherID.String()),
				zap.Uint64("commitTs", blockState.BlockTs),
			)
			return event
		}
		//  the block event, and check whether we need to send write action
		event.markDispatcherEventDone(dispatcherID)
		return event
	}
	// it's not a blocked event, it must be sent by table event trigger dispatcher
	// and the ddl already synced to downstream , e.g.: create table, drop table
	// if ack failed, dispatcher will send a heartbeat again, so we do not need to care about resend message here
	event := NewBlockEvent(changefeedID, b.controller, blockState, b.splitTableEnabled)
	// mark the event as selected, so we do not need to wait for all dispatchers to report the event
	// and make the rangeChecker always return true
	event.rangeChecker = range_checker.NewBoolRangeChecker(true)
	event.selected = true
	return event
}

// getOrInsertNewEvent get the block event from the map, if not found, create a new one
func (b *Barrier) getOrInsertNewEvent(changefeedID common.ChangeFeedID, key eventKey,
	blockState *heartbeatpb.State) *BarrierEvent {
	event, ok := b.blockedTs[key]
	if !ok {
		event = NewBlockEvent(changefeedID, b.controller, blockState, b.splitTableEnabled)
		b.blockedTs[key] = event
	}
	return event
}

func (b *Barrier) checkEvent(be *BarrierEvent,
	dispatchers []*heartbeatpb.DispatcherID) *heartbeatpb.DispatcherStatus {
	if !be.allDispatcherReported() {
		return nil
	}
	if be.selected {
		log.Info("the all dispatchers reported event done, remove event and schedule it",
			zap.String("changefeed", be.cfID.Name()),
			zap.Uint64("committs", be.commitTs))
		// already selected a dispatcher to write, now all dispatchers reported the block event
		delete(b.blockedTs, getEventKey(be.commitTs, be.isSyncPoint))
		be.scheduleBlockEvent()
		return nil
	}
	return be.onAllDispatcherReportedBlockEvent(dispatchers)
}

// ackEvent creates an ack event
func ackEvent(commitTs uint64, isSyncPoint bool) *heartbeatpb.ACK {
	return &heartbeatpb.ACK{
		CommitTs:    commitTs,
		IsSyncPoint: isSyncPoint,
	}
}

// getEventKey returns the key of the block event
func getEventKey(blockTs uint64, isSyncPoint bool) eventKey {
	return eventKey{
		blockTs:     blockTs,
		isSyncPoint: isSyncPoint,
	}
}
