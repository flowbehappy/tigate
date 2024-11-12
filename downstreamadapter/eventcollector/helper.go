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

func (h *EventsHandler) Handle(stat *DispatcherStat, events ...dispatcher.DispatcherEvent) bool {
	// If the dispatcher is not ready, try to find handshake event to make the dispatcher ready.
	if !stat.isReady.Load() {
		ready, restEvents := stat.checkHandshakeEvents(events)
		if !ready {
			return false
		}
		events = restEvents
	}

	// Pre-check, make sure the event is not out-of-order
	for _, dispatcherEvent := range events {
		event := dispatcherEvent.Event
		if event.GetType() == commonEvent.TypeDMLEvent ||
			event.GetType() == commonEvent.TypeDDLEvent ||
			event.GetType() == commonEvent.TypeHandshakeEvent {
			expectedSeq := stat.lastEventSeq.Add(1)
			if event.GetSeq() != expectedSeq {
				log.Warn("Received an out-of-order event, reset the dispatcher",
					zap.String("changefeedID", stat.target.GetChangefeedID().ID().String()), zap.Stringer("dispatcher", stat.target.GetId()),
					zap.Uint64("receivedSeq", event.GetSeq()), zap.Uint64("expectedSeq", expectedSeq),
					zap.Uint64("commitTs", event.GetCommitTs()), zap.Any("event", event))
				h.eventCollector.ResetDispatcherStat(stat)
				return false
			}
		}
	}

	// TODO: will stat become invalid after HandleEvents?
	return stat.target.HandleEvents(events, func() { h.eventCollector.WakeDispatcher(stat.dispatcherID) })
}

const (
	DataGroupResolvedTsOrDML = 1
	DataGroupDDL             = 2
	DataGroupSyncPoint       = 3
	DataGroupHandshake       = 4
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
