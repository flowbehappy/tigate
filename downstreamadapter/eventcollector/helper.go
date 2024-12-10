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
	option.UseBuffer = true
	// Enable memory control for dispatcher events dynamic stream.
	log.Info("New EventDynamicStream, memory control is enabled")
	option.EnableMemoryControl = true
	eventsHandler := &EventsHandler{
		eventCollector: collector,
	}
	stream := dynstream.NewParallelDynamicStream(func(id common.DispatcherID) uint64 { return (common.GID)(id).FastHash() }, eventsHandler, option)
	stream.Start()
	return stream
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
	// switch events[0].GetType() {
	// case commonEvent.TypeDDLEvent,
	// 	commonEvent.TypeSyncPointEvent,
	// 	commonEvent.TypeHandshakeEvent,
	// 	commonEvent.TypeReadyEvent,
	// 	commonEvent.TypeNotReusableEvent:
	// 	if len(events) > 1 {
	// 		log.Panic("receive multiple non-batchable events",
	// 			zap.String("changefeedID", stat.target.GetChangefeedID().ID().String()),
	// 			zap.Stringer("dispatcher", stat.target.GetId()),
	// 			zap.Any("events", events))
	// 	}
	// case commonEvent.TypeResolvedEvent,
	// 	commonEvent.TypeDMLEvent:
	// 	// TypeResolvedEvent and TypeDMLEvent can be in the same batch
	// 	for i := 0; i < len(events); i++ {
	// 		if events[i].GetType() != commonEvent.TypeResolvedEvent && events[i].GetType() != commonEvent.TypeDMLEvent {
	// 			log.Panic("receive multiple events with upexpected types",
	// 				zap.String("changefeedID", stat.target.GetChangefeedID().ID().String()),
	// 				zap.Stringer("dispatcher", stat.target.GetId()),
	// 				zap.Any("events", events))
	// 		}
	// 	}
	// default:
	// 	for i := 1; i < len(events); i++ {
	// 		if events[i].GetType() != events[0].GetType() {
	// 			log.Panic("receive multiple events with different types",
	// 				zap.String("changefeedID", stat.target.GetChangefeedID().ID().String()),
	// 				zap.Stringer("dispatcher", stat.target.GetId()),
	// 				zap.Any("events", events))
	// 		}
	// 	}
	// }

	// just check the first event type, because all event types should be same
	switch events[0].GetType() {
	// note: TypeDMLEvent and TypeResolvedEvent can be in the same batch, so we should handle them together.
	case commonEvent.TypeDMLEvent,
		commonEvent.TypeResolvedEvent:
		validEventStart := 0
		for _, event := range events {
			if stat.shouldIgnoreDataEvent(event, h.eventCollector) {
				continue
			}
		}
		return stat.target.HandleEvents(events[validEventStart:], func() { h.eventCollector.WakeDispatcher(stat.dispatcherID) })
	case commonEvent.TypeDDLEvent,
		commonEvent.TypeSyncPointEvent:
		if stat.shouldIgnoreDataEvent(events[0], h.eventCollector) {
			return false
		}
		return stat.target.HandleEvents(events, func() { h.eventCollector.WakeDispatcher(stat.dispatcherID) })
	case commonEvent.TypeHandshakeEvent:
		stat.handleHandshakeEvent(events[0], h.eventCollector)
		return false
	case commonEvent.TypeReadyEvent:
		stat.handleReadyEvent(events[0], h.eventCollector)
		return false
	case commonEvent.TypeNotReusableEvent:
		stat.handleNotReusableEvent(events[0], h.eventCollector)
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
	if event.GetType() != commonEvent.TypeResolvedEvent {
		// It is normal to drop resolved event
		log.Info("event dropped",
			zap.Any("dispatcher", event.GetDispatcherID()),
			zap.Any("type", event.GetType()),
			zap.Any("commitTs", event.GetCommitTs()),
			zap.Any("sequence", event.GetSeq()))
	}
}
