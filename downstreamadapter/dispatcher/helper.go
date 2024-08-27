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
	"log"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/utils/dynstream"
	"github.com/flowbehappy/tigate/utils/threadpool"
)

type SyncPointInfo struct {
	EnableSyncPoint   bool
	SyncPointInterval time.Duration
	NextSyncPointTs   uint64
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
	Id              common.DispatcherID
	TableSpan       *common.TableSpan
	ComponentStatus heartbeatpb.ComponentState
	IsRemoving      bool
}

// DispatcherStatusHandler is used to handle the DispatcherStatus event.
// Each dispatcher status may contain a ACK info or a dispatcher action or both.
// If we get a ack info, we need to check whether the ack is for the current pending ddl event. If so, we can cancel the resend task.
// If we get a dispatcher action, we need to check whether the action is for the current pending ddl event. If so, we can deal the ddl event based on the action.
// 1. If the action is a write, we need to add the ddl event to the sink for writing to downstream(async).
// 2. If the action is a pass, we just need to pass the event in tableProgress(for correct calculation) and wake the dispatcherEventsHandler
type DispatcherStatusHandler struct {
}

func (h *DispatcherStatusHandler) Path(event DispatcherStatusWithID) common.DispatcherID {
	return event.GetDispatcherID()
}

<<<<<<< HEAD
func (h *DispatcherStatusHandler) Handle(event DispatcherStatusWithDispatcherID, dispatcher *Dispatcher) (await bool) {
	sink := dispatcher.GetSink()
	tableSpan := dispatcher.GetTableSpan()
	pendingEvent := dispatcher.GetDDLPendingEvent()
	dispatcherStatus := event.GetDispatcherStatus()
	if pendingEvent == nil {
		if dispatcherStatus.GetAction() != nil {
			// 只可能出现在 event 已经推进了，但是还重复收到了 action 消息的时候，则重发包含 checkpointTs 的心跳
			dispatcher.GetTableSpanStatusesChan() <- &heartbeatpb.TableSpanStatus{
				Span:            tableSpan.TableSpan,
				ComponentStatus: heartbeatpb.ComponentState_Working,
				CheckpointTs:    dispatcher.GetCheckpointTs(),
			}
		}
		return false
	}

	dispatcherAction := dispatcherStatus.GetAction()
	if dispatcherAction != nil {
		if dispatcherAction.CommitTs == pendingEvent.CommitTs {
			if dispatcherAction.Action == heartbeatpb.Action_Write {
				sink.AddDDLAndSyncPointEvent(pendingEvent, dispatcher.tableProgress)
			} else {
				sink.PassDDLAndSyncPointEvent(pendingEvent, dispatcher.tableProgress)
				dispatcherEventDynamicStream := appcontext.GetService[dynstream.DynamicStream[common.DispatcherID, common.Event, *Dispatcher]](appcontext.DispatcherEventsDynamicStream)
				dispatcherEventDynamicStream.Wake() <- event.GetDispatcherID()
			}
			dispatcher.GetTableSpanStatusesChan() <- &heartbeatpb.TableSpanStatus{
				Span:            tableSpan.TableSpan,
				ComponentStatus: heartbeatpb.ComponentState_Working,
				CheckpointTs:    dispatcher.GetCheckpointTs(),
			}
		}
	}

	if dispatcherStatus.GetAck() != nil {
		dispatcher.CancelResendTask()
	}
=======
func (h *DispatcherStatusHandler) Handle(event DispatcherStatusWithID, dispatcher *Dispatcher) (await bool) {
	dispatcher.HandleDispatcherStatus(event)
>>>>>>> upstream/master
	return false
}

// CheckTableProgressEmptyTask is reponsible for checking whether the tableProgress is empty.
// If the tableProgress is empty,
// 1. If the event is a single table DDL, it will be added to the sink for writing to downstream(async).
// 2. If the event is a multi-table DDL, it will generate a TableSpanStatus message with ddl info to send to maintainer.
// When the tableProgress is empty, the task will finished after this execution.
// If the tableProgress is not empty, the task will be rescheduled after 10ms.
type CheckProgressEmptyTask struct {
	dispatcher *Dispatcher
	taskHandle *threadpool.TaskHandle
}

func newCheckProgressEmptyTask(dispatcher *Dispatcher) *CheckProgressEmptyTask {
	taskScheduler := GetDispatcherTaskScheduler()
	t := &CheckProgressEmptyTask{
		dispatcher: dispatcher,
	}
	t.taskHandle = taskScheduler.Submit(t, time.Now().Add(10*time.Millisecond))
	return t
}

func (t *CheckProgressEmptyTask) Execute() time.Time {
	if t.dispatcher.tableProgress.Empty() {
		t.dispatcher.DealWithDDLWhenProgressEmpty()
		return time.Time{}
	}
	return time.Now().Add(10 * time.Millisecond)
}

// Resend Task is reponsible for resending the TableSpanStatus message with ddl info to maintainer each 50ms.
// The task will be cancelled when the the dispatcher received the ack message from the maintainer
type ResendTask struct {
	message    *heartbeatpb.TableSpanStatus
	dispatcher *Dispatcher
	taskHandle *threadpool.TaskHandle
}

func newResendTask(message *heartbeatpb.TableSpanStatus, dispatcher *Dispatcher) *ResendTask {
	taskScheduler := GetDispatcherTaskScheduler()
	t := &ResendTask{
		message:    message,
		dispatcher: dispatcher,
	}
	t.taskHandle = taskScheduler.Submit(t, time.Now().Add(50*time.Millisecond))
	return t
}

func (t *ResendTask) Execute() time.Time {
	t.dispatcher.GetStatusesChan() <- t.message
	return time.Now().Add(50 * time.Millisecond)
}

func (t *ResendTask) Cancel() {
	t.taskHandle.Cancel()
}

// DispatcherEventsHandler is used to dispatcher the events received.
// If the event is a DML event, it will be added to the sink for writing to downstream.
// If the event is a resolved TS event, it will be update the resolvedTs of the dispatcher.
// If the event is a DDL event,
//  1. If it is a single table DDL,
//     a. If the tableProgress is empty（previous events are flushed successfully），it will be added to the sink for writing to downstream(async).
//     b. If the tableProgress is not empty, we will generate a CheckTableProgressEmptyTask to periodly check whether the tableProgress is empty,
//     and then add the DDL event to the sink for writing to downstream(async).
//  2. If it is a multi-table DDL,
//     a. If the tableProgress is empty（previous events are flushed successfully），We will generate a TableSpanStatus message with ddl info to send to maintainer.
//     b. If the tableProgress is not empty, we will generate a CheckTableProgressEmptyTask to periodly check whether the tableProgress is empty,
//     and then we will generate a TableSpanStatus message with ddl info to send to maintainer.
//     for the multi-table DDL, we will also generate a ResendTask to resend the TableSpanStatus message with ddl info to maintainer each 50ms to avoid message is missing.
//
// Considering for ddl event, we always do an async write, so we need to be blocked before the ddl event flushed to downstream successfully.
// Thus, we add a callback function to let the handler be waked when the ddl event flushed to downstream successfully.

type DispatcherEventsHandler struct {
}

func (h *DispatcherEventsHandler) Path(event common.Event) common.DispatcherID {
	return event.GetDispatcherID()
}

// TODO: 这个后面需要按照更大的粒度进行攒批
<<<<<<< HEAD
func (h *DispatcherEventsHandler) Handle(event common.Event, dispatcher *Dispatcher) bool {
	sink := dispatcher.GetSink()
	switch event.GetType() {
	case common.TypeResolvedEvent:
		dispatcher.resolvedTs.Set(event.(*common.ResolvedEvent).ResolvedTs)
		return false
	case common.TypeTxnEvent:
		txnEvent := event.(*common.TxnEvent)
		if txnEvent.IsDMLEvent() {
			sink.AddDMLEvent(txnEvent, dispatcher.tableProgress)
			return false
		}
		if txnEvent.IsDDLEvent() {
			txnEvent.PostTxnFlushed = append(txnEvent.PostTxnFlushed, func() {
				dispatcherEventDynamicStream := appcontext.GetService[dynstream.DynamicStream[common.DispatcherID, common.Event, *Dispatcher]](appcontext.DispatcherEventsDynamicStream)
				dispatcherEventDynamicStream.Wake() <- txnEvent.GetDispatcherID()
			})
			dispatcher.AddDDLEventToSinkWhenAvailable(txnEvent)
			return true
		}
	default:
		log.Panic("unknown event type")
	}
	return false
=======
func (h *DispatcherEventsHandler) Handle(event *common.TxnEvent, dispatcher *Dispatcher) bool {
	return dispatcher.HandleEvent(event)
}

type DispatcherStatusWithID struct {
	status *heartbeatpb.DispatcherStatus
	id     common.DispatcherID
}

func NewDispatcherStatusWithID(dispatcherStatus *heartbeatpb.DispatcherStatus, dispatcherID common.DispatcherID) *DispatcherStatusWithID {
	return &DispatcherStatusWithID{
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

var DispatcherTaskScheduler threadpool.ThreadPool
var dispatcherTaskSchedulerOnce sync.Once

func GetDispatcherTaskScheduler() threadpool.ThreadPool {
	if DispatcherTaskScheduler == nil {
		dispatcherTaskSchedulerOnce.Do(func() {
			DispatcherTaskScheduler = threadpool.NewThreadPoolDefault()
		})
	}
	return DispatcherTaskScheduler
>>>>>>> upstream/master
}

func SetDispatcherTaskScheduler(taskScheduler threadpool.ThreadPool) {
	DispatcherTaskScheduler = taskScheduler
}

var dispatcherEventsDynamicStream dynstream.DynamicStream[common.DispatcherID, *common.TxnEvent, *Dispatcher]
var dispatcherEventsDynamicStreamOnce sync.Once

func GetDispatcherEventsDynamicStream() dynstream.DynamicStream[common.DispatcherID, *common.TxnEvent, *Dispatcher] {
	if dispatcherEventsDynamicStream == nil {
		dispatcherEventsDynamicStreamOnce.Do(func() {
			dispatcherEventsDynamicStream = dynstream.NewDynamicStreamDefault(&DispatcherEventsHandler{})
			dispatcherEventsDynamicStream.Start()
		})
	}
	return dispatcherEventsDynamicStream
}

func SetDispatcherEventsDynamicStream(dynamicStream dynstream.DynamicStream[common.DispatcherID, *common.TxnEvent, *Dispatcher]) {
	dispatcherEventsDynamicStream = dynamicStream
}

var dispatcherStatusDynamicStream dynstream.DynamicStream[common.DispatcherID, DispatcherStatusWithID, *Dispatcher]
var dispatcherStatusDynamicStreamOnce sync.Once

func GetDispatcherStatusDynamicStream() dynstream.DynamicStream[common.DispatcherID, DispatcherStatusWithID, *Dispatcher] {
	if dispatcherStatusDynamicStream == nil {
		dispatcherStatusDynamicStreamOnce.Do(func() {
			dispatcherStatusDynamicStream = dynstream.NewDynamicStreamDefault(&DispatcherStatusHandler{})
			dispatcherStatusDynamicStream.Start()
		})
	}
	return dispatcherStatusDynamicStream
}

func SetDispatcherStatusDynamicStream(dynamicStream dynstream.DynamicStream[common.DispatcherID, DispatcherStatusWithID, *Dispatcher]) {
	dispatcherStatusDynamicStream = dynamicStream
}
