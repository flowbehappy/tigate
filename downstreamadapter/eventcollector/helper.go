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

package eventcollector

import (
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/utils/dynstream"
	"go.uber.org/zap"
)

func NewEventDynamicStream(collector *EventCollector) dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherEvent, *DispatcherStat, *EventsHandler] {
	option := dynstream.NewOption()
	option.BatchCount = 128
	// Enable memory control for dispatcher events dynamic stream.
	log.Info("New EventDynamicStream, memory control is enabled")
	option.EnableMemoryControl = true
	eventsHandler := &EventsHandler{
		eventCollector: collector,
	}
	eventDynamicStream := dynstream.NewDynamicStream(eventsHandler, option)
	eventDynamicStream.Start()
	return eventDynamicStream
}

// EventsHandler is used to dispatch the received events.
// If the event is a DML event, it will be added to the sink for writing to downstream.
// If the event is a resolved TS event, it will be update the resolvedTs of the dispatcher.
// If the event is a DDL event,
//  1. If it is a single table DDL, it will be added to the sink for writing to downstream(async).
//  2. If it is a multi-table DDL, We will generate a TableSpanBlockStatus message with ddl info to send to maintainer.
//     for the multi-table DDL, we will also generate a ResendTask to resend the TableSpanBlockStatus message with ddl info
//     to maintainer each 200ms to avoid message is missing.
//
// If the event is a Sync Point event, we deal it as a multi-table DDL event.
//
// We can handle multi events in batch if there only dml events and resovledTs events.
// For DDL event and Sync Point Event, we should handle them singlely.
// Thus, if a event is DDL event or Sync Point Event, we will only get one event at once.
// Otherwise, we can get a batch events.
// We always return block = true for Handle() except we only receive the resolvedTs events.
// So we only will reach next Handle() when previous events are all push downstream successfully.
type EventsHandler struct {
	eventCollector *EventCollector
}

func (h *EventsHandler) Path(event dispatcher.DispatcherEvent) common.DispatcherID {
	return event.GetDispatcherID()
}

// Invariant: at any times, we can receive events from at most two event service, and one of them must be local event service.
func (h *EventsHandler) Handle(stat *DispatcherStat, events ...dispatcher.DispatcherEvent) bool {
	// do some check for safety
	if len(events) == 0 {
		return false
	}
	switch events[0].GetType() {
	case commonEvent.TypeDDLEvent,
		commonEvent.TypeSyncPointEvent,
		commonEvent.TypeHandshakeEvent,
		commonEvent.TypeReadyEvent,
		commonEvent.TypeNotReusableEvent:
		if len(events) > 1 {
			log.Panic("receive multiple non-batchable events",
				zap.String("changefeedID", stat.target.GetChangefeedID().ID().String()),
				zap.Stringer("dispatcher", stat.target.GetId()),
				zap.Any("events", events))
		}
	default:
		for i := 1; i < len(events); i++ {
			if events[i].GetType() != events[0].GetType() {
				log.Panic("receive multiple events with different types",
					zap.String("changefeedID", stat.target.GetChangefeedID().ID().String()),
					zap.Stringer("dispatcher", stat.target.GetId()),
					zap.Any("events", events))
			}
		}
	}

	// just check the first event type, because all event types should be same
	switch events[0].GetType() {
	case commonEvent.TypeDMLEvent,
		commonEvent.TypeDDLEvent,
		commonEvent.TypeResolvedEvent,
		commonEvent.TypeSyncPointEvent:
		if stat.waitHandshake.Load() {
			log.Warn("Receive event before handshake event, ignore it",
				zap.String("changefeedID", stat.target.GetChangefeedID().ID().String()),
				zap.Stringer("dispatcher", stat.target.GetId()),
				zap.Any("event", events))
			return false
		}
		for _, dispatcherEvent := range events {
			if dispatcherEvent.GetType() == commonEvent.TypeDMLEvent ||
				dispatcherEvent.GetType() == commonEvent.TypeDDLEvent {
				expectedSeq := stat.lastEventSeq.Add(1)
				if dispatcherEvent.GetSeq() != expectedSeq {
					log.Warn("Received an out-of-order event, reset the dispatcher",
						zap.String("changefeedID", stat.target.GetChangefeedID().ID().String()),
						zap.Stringer("dispatcher", stat.target.GetId()),
						zap.Uint64("receivedSeq", dispatcherEvent.GetSeq()),
						zap.Uint64("expectedSeq", expectedSeq),
						zap.Uint64("commitTs", dispatcherEvent.GetCommitTs()),
						zap.Any("event", dispatcherEvent))
					h.eventCollector.ResetDispatcherStat(stat)
					return false
				}
			}
		}
		return stat.target.HandleEvents(events, func() { h.eventCollector.WakeDispatcher(stat.dispatcherID) })
	case commonEvent.TypeHandshakeEvent:
		dispatcherEvent := events[0]
		if !stat.isCurrentEventService(dispatcherEvent.From) {
			if !stat.isCurrentEventService(h.eventCollector.serverId) {
				log.Panic("receive handshake event from remote event service, but current event service is not local event service",
					zap.String("changefeedID", stat.target.GetChangefeedID().ID().String()),
					zap.Stringer("dispatcher", stat.target.GetId()),
					zap.Stringer("from", dispatcherEvent.From))
			}
			return false
		}
		if !stat.waitHandshake.Load() {
			log.Panic("receive handshake event when dispatcher is ready",
				zap.String("changefeedID", stat.target.GetChangefeedID().ID().String()),
				zap.Stringer("dispatcher", stat.target.GetId()))
		}
		handshake, ok := dispatcherEvent.Event.(*commonEvent.HandshakeEvent)
		if !ok {
			log.Panic("cast handshake event failed",
				zap.Stringer("dispatcher", stat.target.GetId()),
				zap.Uint64("commitTs", dispatcherEvent.GetCommitTs()))
		}
		if handshake.GetCommitTs() == stat.target.GetCheckpointTs() {
			currentSeq := stat.lastEventSeq.Load()
			if currentSeq != 0 {
				log.Panic("Receive handshake event, but current seq is not zero",
					zap.Any("event", handshake),
					zap.Stringer("dispatcher", stat.target.GetId()),
					zap.Uint64("currentSeq", currentSeq))
				return false
			}
			// In some case, the eventService may send handshake event multiple times,
			// we should use the first handshake event we received to initialize the dispatcher.
			stat.lastEventSeq.Store(handshake.GetSeq())
			stat.target.SetInitialTableInfo(handshake.TableInfo)
			stat.waitHandshake.Store(false)
			log.Info("Receive handshake event, dispatcher is ready to handle events",
				zap.Any("dispatcher", stat.target.GetId()),
				zap.Uint64("commitTs", handshake.GetCommitTs()),
			)
		} else {
			log.Warn("Handshake event commitTs not equal to dispatcher startTs, ignore it",
				zap.Any("event", dispatcherEvent),
				zap.Stringer("dispatcher", stat.target.GetId()),
				zap.Uint64("checkpointTs", stat.target.GetCheckpointTs()),
				zap.Uint64("commitTs", dispatcherEvent.GetCommitTs()))
		}
		return false
	case commonEvent.TypeReadyEvent:
		// If we have received the ready signal from the event service, we can ignore it.
		// TODO: add timeout to resend needed request if we receive ready signal from the same server for a long time.
		if stat.isCurrentEventService(events[0].From) ||
			stat.isCurrentEventService(h.eventCollector.serverId) {
			return false
		}
		// receive ready signal from local event service
		if events[0].From == h.eventCollector.serverId {
			stat.unregisterFromRemoteEventServiceIfHave(h.eventCollector)
			stat.clearRemoteCandidateInfo()
			stat.notifyReadyForReceiveData(events[0].From, h.eventCollector)
			return false
		}
		// receive ready signal from remote event service
		stat.checkNoReadySignalReceived(events[0].From)
		stat.clearRemoteCandidateInfo()
		stat.notifyReadyForReceiveData(events[0].From, h.eventCollector)
		return false
	case commonEvent.TypeNotReusableEvent:
		if !stat.isCurrentEventService(h.eventCollector.serverId) {
			stat.tryNextRemoteCandidate(h.eventCollector)
		}
		return false
	default:
		log.Panic("unknown event type", zap.Int("type", int(events[0].GetType())))
	}
	return false
}

const (
	DataGroupResolvedTsOrDML = 1
	DataGroupDDL             = 2
	DataGroupSyncPoint       = 3
	DataGroupHandshake       = 4
	DataGroupReady           = 5
	DataGroupNotReusable     = 6
)

func (h *EventsHandler) GetType(event dispatcher.DispatcherEvent) dynstream.EventType {
	switch event.GetType() {
	case commonEvent.TypeResolvedEvent:
		return dynstream.EventType{DataGroup: DataGroupResolvedTsOrDML, Property: dynstream.PeriodicSignal}
	case commonEvent.TypeDMLEvent:
		return dynstream.EventType{DataGroup: DataGroupResolvedTsOrDML, Property: dynstream.BatchableData}
	case commonEvent.TypeDDLEvent:
		return dynstream.EventType{DataGroup: DataGroupDDL, Property: dynstream.NonBatchable}
	case commonEvent.TypeSyncPointEvent:
		return dynstream.EventType{DataGroup: DataGroupSyncPoint, Property: dynstream.NonBatchable}
	case commonEvent.TypeHandshakeEvent:
		return dynstream.EventType{DataGroup: DataGroupHandshake, Property: dynstream.NonBatchable}
	case commonEvent.TypeReadyEvent:
		return dynstream.EventType{DataGroup: DataGroupReady, Property: dynstream.NonBatchable}
	case commonEvent.TypeNotReusableEvent:
		return dynstream.EventType{DataGroup: DataGroupNotReusable, Property: dynstream.NonBatchable}
	default:
		log.Panic("unknown event type", zap.Int("type", int(event.GetType())))
	}
	return dynstream.DefaultEventType
}

func (h *EventsHandler) GetSize(event dispatcher.DispatcherEvent) int { return int(event.GetSize()) }

func (h *EventsHandler) IsPaused(event dispatcher.DispatcherEvent) bool { return event.IsPaused() }

func (h *EventsHandler) GetArea(path common.DispatcherID, dest *DispatcherStat) common.GID {
	return dest.target.GetChangefeedID().ID()
}

func (h *EventsHandler) GetTimestamp(event dispatcher.DispatcherEvent) dynstream.Timestamp {
	return dynstream.Timestamp(event.GetCommitTs())
}

func (h *EventsHandler) OnDrop(event dispatcher.DispatcherEvent) {
	log.Info("event dropped", zap.Any("dispatcher", event.GetDispatcherID()), zap.Any("commitTs", event.GetCommitTs()), zap.Any("sequence", event.GetSeq()))
}
