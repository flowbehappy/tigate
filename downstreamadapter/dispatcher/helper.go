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

package dispatcher

import (
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/pingcap/ticdc/utils/threadpool"
	"go.uber.org/zap"
)

type ResendTaskMap struct {
	mutex sync.Mutex
	m     map[BlockEventIdentifier]*ResendTask
}

func newResendTaskMap() *ResendTaskMap {
	return &ResendTaskMap{
		m: make(map[BlockEventIdentifier]*ResendTask),
	}
}

func (r *ResendTaskMap) Get(identifier BlockEventIdentifier) *ResendTask {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.m[identifier]
}

func (r *ResendTaskMap) Set(identifier BlockEventIdentifier, task *ResendTask) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.m[identifier] = task
}

func (r *ResendTaskMap) Delete(identifier BlockEventIdentifier) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	delete(r.m, identifier)
}

// Considering the sync point event and ddl event may have the same commitTs,
// we need to distinguish them.
type BlockEventIdentifier struct {
	CommitTs    uint64
	IsSyncPoint bool
}

type BlockStatus struct {
	mutex             sync.Mutex
	blockPendingEvent commonEvent.BlockEvent
	blockStage        heartbeatpb.BlockStage
}

func (b *BlockStatus) clear() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.blockPendingEvent = nil
	b.blockStage = heartbeatpb.BlockStage_NONE
}

func (b *BlockStatus) setBlockEvent(event commonEvent.BlockEvent, blockStage heartbeatpb.BlockStage) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.blockPendingEvent = event
	b.blockStage = blockStage
}

func (b *BlockStatus) updateBlockStage(blockStage heartbeatpb.BlockStage) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.blockStage = blockStage
}

func (b *BlockStatus) getEventAndStage() (commonEvent.BlockEvent, heartbeatpb.BlockStage) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.blockPendingEvent, b.blockStage
}

type SchemaIDToDispatchers struct {
	mutex sync.RWMutex
	m     map[int64]map[common.DispatcherID]interface{}
}

func NewSchemaIDToDispatchers() *SchemaIDToDispatchers {
	return &SchemaIDToDispatchers{
		m: make(map[int64]map[common.DispatcherID]interface{}),
	}
}

func (s *SchemaIDToDispatchers) Set(schemaID int64, dispatcherID common.DispatcherID) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.m[schemaID]; !ok {
		s.m[schemaID] = make(map[common.DispatcherID]interface{})
	}
	s.m[schemaID][dispatcherID] = struct{}{}
}

func (s *SchemaIDToDispatchers) Delete(schemaID int64, dispatcherID common.DispatcherID) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.m[schemaID]; ok {
		delete(s.m[schemaID], dispatcherID)
	}
}

func (s *SchemaIDToDispatchers) Update(oldSchemaID int64, newSchemaID int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.m[oldSchemaID]; ok {
		s.m[newSchemaID] = s.m[oldSchemaID]
		delete(s.m, oldSchemaID)
	} else {
		log.Error("schemaID not found", zap.Any("schemaID", oldSchemaID))
	}
}

func (s *SchemaIDToDispatchers) GetDispatcherIDs(schemaID int64) []common.DispatcherID {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if ids, ok := s.m[schemaID]; ok {
		dispatcherIDs := make([]common.DispatcherID, 0, len(ids))
		for id := range ids {
			dispatcherIDs = append(dispatcherIDs, id)
		}
		return dispatcherIDs
	}
	return nil
}

type ComponentStateWithMutex struct {
	mutex           sync.Mutex
	componentStatus heartbeatpb.ComponentState
}

func newComponentStateWithMutex(status heartbeatpb.ComponentState) *ComponentStateWithMutex {
	return &ComponentStateWithMutex{
		componentStatus: status,
	}
}

func (s *ComponentStateWithMutex) Set(status heartbeatpb.ComponentState) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.componentStatus = status
}

func (s *ComponentStateWithMutex) Get() heartbeatpb.ComponentState {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.componentStatus
}

type TsWithMutex struct {
	mutex sync.Mutex
	ts    uint64
}

func newTsWithMutex(ts uint64) *TsWithMutex {
	return &TsWithMutex{
		ts: ts,
	}
}

func (r *TsWithMutex) Set(ts uint64) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.ts = ts
}

func (r *TsWithMutex) Get() uint64 {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.ts
}

/*
HeartBeatInfo is used to collect the message for HeartBeatRequest for each dispatcher.
Mainly about the progress of each dispatcher:
1. The checkpointTs of the dispatcher, shows that all the events whose ts <= checkpointTs are flushed to downstream successfully.
*/
type HeartBeatInfo struct {
	heartbeatpb.Watermark
	EventSizePerSecond float32
	Id                 common.DispatcherID
	TableSpan          *heartbeatpb.TableSpan
	ComponentStatus    heartbeatpb.ComponentState
	IsRemoving         bool
}

// Resend Task is reponsible for resending the TableSpanBlockStatus message with ddl info to maintainer each 50ms.
// The task will be cancelled when the the dispatcher received the ack message from the maintainer
type ResendTask struct {
	message    *heartbeatpb.TableSpanBlockStatus
	dispatcher *Dispatcher
	taskHandle *threadpool.TaskHandle
}

func newResendTask(message *heartbeatpb.TableSpanBlockStatus, dispatcher *Dispatcher) *ResendTask {
	taskScheduler := GetDispatcherTaskScheduler()
	t := &ResendTask{
		message:    message,
		dispatcher: dispatcher,
	}
	t.taskHandle = taskScheduler.Submit(t, time.Now().Add(50*time.Millisecond))
	return t
}

func (t *ResendTask) Execute() time.Time {
	t.dispatcher.blockStatusesChan <- t.message
	return time.Now().Add(200 * time.Millisecond)
}

func (t *ResendTask) Cancel() {
	t.taskHandle.Cancel()
}

var DispatcherTaskScheduler threadpool.ThreadPool
var dispatcherTaskSchedulerOnce sync.Once

func GetDispatcherTaskScheduler() threadpool.ThreadPool {
	if DispatcherTaskScheduler == nil {
		dispatcherTaskSchedulerOnce.Do(func() {
			DispatcherTaskScheduler = threadpool.NewThreadPoolDefault()
		})
	}
	return DispatcherTaskScheduler
}

func SetDispatcherTaskScheduler(taskScheduler threadpool.ThreadPool) {
	DispatcherTaskScheduler = taskScheduler
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
}

func (h *EventsHandler) Path(event DispatcherEvent) common.DispatcherID {
	return event.GetDispatcherID()
}

func (h *EventsHandler) Handle(dispatcher *Dispatcher, events ...DispatcherEvent) bool {
	return dispatcher.HandleEvents(events)
}

const (
	DataGroupResolvedTsOrDML = 1
	DataGroupDDL             = 2
	DataGroupSyncPoint       = 3
	DataGroupHandshake       = 4
)

func (h *EventsHandler) GetType(event DispatcherEvent) dynstream.EventType {
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

func (h *EventsHandler) GetSize(event DispatcherEvent) int   { return int(event.GetSize()) }
func (h *EventsHandler) IsPaused(event DispatcherEvent) bool { return event.IsPaused() }
func (h *EventsHandler) GetArea(path common.DispatcherID, dest *Dispatcher) common.GID {
	return dest.changefeedID.ID()
}
func (h *EventsHandler) GetTimestamp(event DispatcherEvent) dynstream.Timestamp {
	return dynstream.Timestamp(event.GetCommitTs())
}
func (h *EventsHandler) OnDrop(event DispatcherEvent) {
}

type DispatcherEvent struct {
	commonEvent.Event
}

func NewDispatcherEvent(event commonEvent.Event) DispatcherEvent {
	return DispatcherEvent{
		Event: event,
	}
}

// eventDynamicStream is used to process the events received by eventCollector.
var eventDynamicStream dynstream.DynamicStream[common.GID, common.DispatcherID, DispatcherEvent, *Dispatcher, *EventsHandler]
var eventDynamicStreamOnce sync.Once

func GetEventDynamicStream() dynstream.DynamicStream[common.GID, common.DispatcherID, DispatcherEvent, *Dispatcher, *EventsHandler] {
	if eventDynamicStream == nil {
		eventDynamicStreamOnce.Do(func() {
			option := dynstream.NewOption()
			option.BatchCount = 128
			// Enable memory control for dispatcher events dynamic stream.
			log.Info("New EventDynamicStream, memory control is enabled")
			option.EnableMemoryControl = true
			eventDynamicStream = dynstream.NewDynamicStream(&EventsHandler{}, option)
			eventDynamicStream.Start()
		})
	}
	return eventDynamicStream
}

type DispatcherStatusWithID struct {
	id     common.DispatcherID
	status *heartbeatpb.DispatcherStatus
}

func NewDispatcherStatusWithID(dispatcherStatus *heartbeatpb.DispatcherStatus, dispatcherID common.DispatcherID) DispatcherStatusWithID {
	return DispatcherStatusWithID{
		status: dispatcherStatus,
		id:     dispatcherID,
	}
}

func (d *DispatcherStatusWithID) GetDispatcherStatus() *heartbeatpb.DispatcherStatus {
	return d.status
}

func (d *DispatcherStatusWithID) GetDispatcherID() common.DispatcherID {
	return d.id
}

// DispatcherStatusHandler is used to handle the DispatcherStatus event.
// Each dispatcher status may contain a ACK info or a dispatcher action or both.
// If we get a ack info, we need to check whether the ack is for the current pending ddl event.
// If so, we can cancel the resend task.
// If we get a dispatcher action, we need to check whether the action is for the current pending ddl event.
// If so, we can deal the ddl event based on the action.
// 1. If the action is a write, we need to add the ddl event to the sink for writing to downstream(async).
// 2. If the action is a pass, we just need to pass the event in tableProgress(for correct calculation) and
// wake the dispatcherEventsHandler to handle the event.
type DispatcherStatusHandler struct {
}

func (h *DispatcherStatusHandler) Path(event DispatcherStatusWithID) common.DispatcherID {
	return event.GetDispatcherID()
}

func (h *DispatcherStatusHandler) Handle(dispatcher *Dispatcher, events ...DispatcherStatusWithID) (await bool) {
	for _, event := range events {
		dispatcher.HandleDispatcherStatus(event.GetDispatcherStatus())
	}
	return false
}

func (h *DispatcherStatusHandler) GetSize(event DispatcherStatusWithID) int   { return 0 }
func (h *DispatcherStatusHandler) IsPaused(event DispatcherStatusWithID) bool { return false }
func (h *DispatcherStatusHandler) GetArea(path common.DispatcherID, dest *Dispatcher) common.GID {
	return dest.changefeedID.ID()
}
func (h *DispatcherStatusHandler) GetTimestamp(event DispatcherStatusWithID) dynstream.Timestamp {
	if event.GetDispatcherStatus().Action != nil {
		return dynstream.Timestamp(event.GetDispatcherStatus().Action.CommitTs)
	} else if event.GetDispatcherStatus().Ack != nil {
		return dynstream.Timestamp(event.GetDispatcherStatus().Ack.CommitTs)
	}
	return 0
}
func (h *DispatcherStatusHandler) GetType(event DispatcherStatusWithID) dynstream.EventType {
	return dynstream.DefaultEventType
}
func (h *DispatcherStatusHandler) OnDrop(event DispatcherStatusWithID) {}

var dispatcherStatusDynamicStream dynstream.DynamicStream[common.GID, common.DispatcherID, DispatcherStatusWithID, *Dispatcher, *DispatcherStatusHandler]
var dispatcherStatusDynamicStreamOnce sync.Once

func GetDispatcherStatusDynamicStream() dynstream.DynamicStream[common.GID, common.DispatcherID, DispatcherStatusWithID, *Dispatcher, *DispatcherStatusHandler] {
	if dispatcherStatusDynamicStream == nil {
		dispatcherStatusDynamicStreamOnce.Do(func() {
			dispatcherStatusDynamicStream = dynstream.NewDynamicStream(&DispatcherStatusHandler{})
			dispatcherStatusDynamicStream.Start()
		})
	}
	return dispatcherStatusDynamicStream
}

func SetDispatcherStatusDynamicStream(dynamicStream dynstream.DynamicStream[common.GID, common.DispatcherID, DispatcherStatusWithID, *Dispatcher, *DispatcherStatusHandler]) {
	dispatcherStatusDynamicStream = dynamicStream
}
