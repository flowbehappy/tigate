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

func newEventDynamicStream() dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherEvent, *dispatcher.Dispatcher, *EventsHandler] {
	option := dynstream.NewOption()
	option.BatchCount = 128
	// Enable memory control for dispatcher events dynamic stream.
	log.Info("New EventDynamicStream, memory control is enabled")
	option.EnableMemoryControl = true
	eventDynamicStream := dynstream.NewDynamicStream(&EventsHandler{}, option)
	eventDynamicStream.Start()
	return eventDynamicStream
}

type EventsHandler struct {
}

func (h *EventsHandler) Path(event dispatcher.DispatcherEvent) common.DispatcherID {
	return event.GetDispatcherID()
}

func (h *EventsHandler) Handle(dispatcher *dispatcher.Dispatcher, events ...dispatcher.DispatcherEvent) bool {
	return dispatcher.HandleEvents(events)
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

func (h *EventsHandler) GetSize(event dispatcher.DispatcherEvent) int   { return int(event.GetSize()) }
func (h *EventsHandler) IsPaused(event dispatcher.DispatcherEvent) bool { return event.IsPaused() }
func (h *EventsHandler) GetArea(path common.DispatcherID, dest *dispatcher.Dispatcher) common.GID {
	return dest.changefeedID.ID()
}
func (h *EventsHandler) GetTimestamp(event dispatcher.DispatcherEvent) dynstream.Timestamp {
	return dynstream.Timestamp(event.GetCommitTs())
}
func (h *EventsHandler) OnDrop(event dispatcher.DispatcherEvent) {
	log.Info("event dropped", zap.Any("dispatcher", event.GetDispatcherID()), zap.Any("commitTs", event.GetCommitTs()), zap.Any("sequence", event.GetSeq()))
}
