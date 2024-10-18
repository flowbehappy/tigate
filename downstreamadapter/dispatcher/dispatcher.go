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
	"sync/atomic"
	"time"

	tisink "github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/downstreamadapter/sink/types"
	"github.com/flowbehappy/tigate/downstreamadapter/syncpoint"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
)

/*
Dispatcher is responsible for getting events from LogService and sending them to Sink in appropriate order.
Each dispatcher only deal with the events of one tableSpan in one changefeed.
Here is a special dispatcher will deal with the events of the DDLSpan in one changefeed, we call it TableTriggerEventDispatcher
Each EventDispatcherManager will have multiple dispatchers.

All dispatchers in the changefeed of the same node will share the same Sink.
All dispatchers will communicate with the Maintainer about self progress and whether can push down the blocked ddl event.

Because Sink does not flush events to the downstream in strict order.
the dispatcher can't send event to Sink continuously all the time,
1. The ddl event/sync point event can be send to Sink only when the previous event has beed flushed to downstream successfully.
2. Only when the ddl event/sync point event is flushed to downstream successfully, the dispatcher can send the following event to Sink.
3. For the cross table ddl event/sync point event, dispatcher needs to negotiate with the maintainer to decide whether and when send it to Sink.

The workflow related to the dispatcher is as follows:

	+--------------+       +----------------+       +------------+       +--------+       +--------+       +------------+
	| EventService |  -->  | EventCollector |  -->  | Dispatcher |  -->  |  Sink  |  -->  | Worker |  -->  | Downstream |
	+--------------+       +----------------+       +------------+       +--------+       +--------+       +------------+
	                                                        |
										  HeartBeatResponse | HeartBeatRequest
															|
	                                              +--------------------+
	                                              | HeartBeatCollector |
												  +--------------------+
												            |
															|
												      +------------+
	                                                  | Maintainer |
												      +------------+
*/

type Dispatcher struct {
	id        common.DispatcherID
	tableSpan *heartbeatpb.TableSpan
	sink      tisink.Sink
	// startTs is the start timestamp of the dispatcher
	startTs atomic.Uint64
	// lastEventSeq is the sequence number of the last received DML/DDL event.
	// It is used to ensure the order of events.
	lastEventSeq atomic.Uint64

	// blockStatusesChan use to report block status of ddl/sync point event to Maintainer
	blockStatusesChan chan *heartbeatpb.TableSpanBlockStatus

	// dispatcherStatusChan is used to report the status of the dispatcher to the dispatcherManager
	dispatcherActionChan chan common.DispatcherAction

	SyncPointInfo *syncpoint.SyncPointInfo

	componentStatus *ComponentStateWithMutex

	filter filter.Filter

	resolvedTs *TsWithMutex // 用来记 中目前收到的 event 中收到的最大的 commitTs - 1,不代表 dispatcher 的 checkpointTs

	blockPendingEvent commonEvent.BlockEvent
	isRemoving        atomic.Bool

	tableProgress *types.TableProgress

	resendTask *ResendTask

	schemaIDToDispatchers *SchemaIDToDispatchers
	schemaID              int64

	// only exist when the dispatcher is a table trigger event dispatcher
	tableNameStore *TableNameStore

	// isReady is used to indicate whether the dispatcher is ready.
	// If false, the dispatcher will drop the event it received.
	isReady atomic.Bool
}

func NewDispatcher(
	id common.DispatcherID,
	tableSpan *heartbeatpb.TableSpan,
	sink tisink.Sink,
	startTs uint64,
	dispatcherActionChan chan common.DispatcherAction,
	blockStatusesChan chan *heartbeatpb.TableSpanBlockStatus,
	filter filter.Filter,
	schemaID int64,
	schemaIDToDispatchers *SchemaIDToDispatchers,
	syncPointInfo *syncpoint.SyncPointInfo) *Dispatcher {
	dispatcher := &Dispatcher{
		id:                    id,
		tableSpan:             tableSpan,
		sink:                  sink,
		blockStatusesChan:     blockStatusesChan,
		dispatcherActionChan:  dispatcherActionChan,
		SyncPointInfo:         syncPointInfo,
		componentStatus:       newComponentStateWithMutex(heartbeatpb.ComponentState_Working),
		resolvedTs:            newTsWithMutex(startTs),
		filter:                filter,
		isRemoving:            atomic.Bool{},
		blockPendingEvent:     nil,
		tableProgress:         types.NewTableProgress(),
		schemaID:              schemaID,
		schemaIDToDispatchers: schemaIDToDispatchers,
	}
	dispatcher.startTs.Store(startTs)

	// only when is not mysql sink, table trigger event dispatcher need tableNameStore to store the table name
	// in order to calculate all the topics when sending checkpointTs to downstream
	if tableSpan.Equal(heartbeatpb.DDLSpan) && dispatcher.sink.SinkType() != tisink.MysqlSinkType {
		dispatcher.tableNameStore = NewTableNameStore()
	}

	dispatcher.AddToDynamicStream()

	return dispatcher
}

// Each dispatcher status may contain a ACK info or a dispatcher action or both.
// If we get a ack info, we need to check whether the ack is for the current pending ddl event. If so, we can cancel the resend task.
// If we get a dispatcher action, we need to check whether the action is for the current pending ddl event. If so, we can deal the ddl event based on the action.
// 1. If the action is a write, we need to add the ddl event to the sink for writing to downstream(async).
// 2. If the action is a pass, we just need to pass the event in tableProgress(for correct calculation) and wake the dispatcherEventsHandler
func (d *Dispatcher) HandleDispatcherStatus(dispatcherStatus *heartbeatpb.DispatcherStatus) {
	if d.blockPendingEvent == nil {
		// receive outdated status
		// If status is about ack, ignore it.
		// If status is about action, we need to return message show we have finished the event.
		if dispatcherStatus.GetAction() != nil {
			d.blockStatusesChan <- &heartbeatpb.TableSpanBlockStatus{
				ID: d.id.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     dispatcherStatus.GetAction().CommitTs,
					IsSyncPoint: dispatcherStatus.GetAction().IsSyncPoint,
					EventDone:   true,
				},
			}
		}
		return
	}

	action := dispatcherStatus.GetAction()
	if action != nil {
		if action.CommitTs == d.blockPendingEvent.GetCommitTs() {
			if action.Action == heartbeatpb.Action_Write {
				d.sink.AddBlockEvent(d.blockPendingEvent, d.tableProgress)
			} else {
				d.sink.PassBlockEvent(d.blockPendingEvent, d.tableProgress)
				dispatcherEventDynamicStream := GetDispatcherEventsDynamicStream()
				dispatcherEventDynamicStream.Wake() <- d.id
			}
		}
		d.blockStatusesChan <- &heartbeatpb.TableSpanBlockStatus{
			ID: d.id.ToPB(),
			State: &heartbeatpb.State{
				IsBlocked:   true,
				BlockTs:     dispatcherStatus.GetAction().CommitTs,
				IsSyncPoint: dispatcherStatus.GetAction().IsSyncPoint,
				EventDone:   true,
			},
		}
	}

	ack := dispatcherStatus.GetAck()
	if ack != nil && ack.CommitTs == d.blockPendingEvent.GetCommitTs() {
		d.CancelResendTask()
	}
}

// HandleEvents can batch handle events about resolvedTs Event and DML Event.
// While for DDLEvent and SyncPointEvent, they should be handled singly,
// because they are block events.
// We ensure we only will receive one event when it's ddl event or sync point event by IsBatchable() function.
// When we handle events, we don't have any previous events still in sink.
func (d *Dispatcher) HandleEvents(dispatcherEvents []DispatcherEvent) (block bool) {

	// If the dispatcher is not ready, try to find handshake event to make the dispatcher ready.
	if !d.isReady.Load() {
		if !d.checkHandshakeEvents(dispatcherEvents) {
			return false
		}
	}

	// Only return false when all events are resolvedTs Event.
	block = false
	// Dispatcher is ready, handle the events
	for _, dispatcherEvent := range dispatcherEvents {
		// Pre-check, make sure the event is not stale or out-of-order
		event := dispatcherEvent.Event
		if event.GetCommitTs() < d.resolvedTs.Get() {
			log.Panic("Received a stale event, it should never happen", zap.Any("event", event), zap.Any("dispatcher", d.id))
		}
		if event.GetType() == commonEvent.TypeDMLEvent ||
			event.GetType() == commonEvent.TypeDDLEvent {
			if event.GetSeq() != d.lastEventSeq.Add(1) {
				log.Warn("Received a out-of-order event, reset the dispatcher", zap.Any("event", event), zap.Any("dispatcher", d.id))
				d.reset()
				return false
			}
		}

		switch event.GetType() {
		case commonEvent.TypeResolvedEvent:
			d.resolvedTs.Set(event.(commonEvent.ResolvedEvent).ResolvedTs)
		case commonEvent.TypeDMLEvent:
			block = true
			event := event.(*commonEvent.DMLEvent)
			// Update the last event sequence number.
			event.AddPostFlushFunc(func() {
				// Considering dml event in sink may be write to downstream not in order,
				// thus, we use tableProgress.Empty() to ensure these events are flushed to downstream completely
				// and wake dynamic stream to handle the next events.
				if d.tableProgress.Empty() {
					dispatcherEventDynamicStream := GetDispatcherEventsDynamicStream()
					dispatcherEventDynamicStream.Wake() <- event.GetDispatcherID()
				}
			})
			d.sink.AddDMLEvent(event, d.tableProgress)
		case commonEvent.TypeDDLEvent:
			if len(dispatcherEvents) != 1 {
				log.Panic("ddl event should only be singly handled", zap.Any("dispatcherID", d.id))
			}
			block = true
			event := event.(*commonEvent.DDLEvent)
			log.Info("dispatcher receive ddl event",
				zap.Stringer("dispatcher", d.id),
				zap.String("query", event.Query),
				zap.Int64("table", event.TableID),
				zap.Uint64("commitTs", event.GetCommitTs()),
				zap.Uint64("seq", event.GetSeq()))
			if d.tableNameStore != nil {
				d.tableNameStore.AddEvent(event)
			}

			event.AddPostFlushFunc(func() {
				dispatcherEventDynamicStream := GetDispatcherEventsDynamicStream()
				dispatcherEventDynamicStream.Wake() <- event.GetDispatcherID()
			})
			d.dealWithBlockEvent(event)
		case commonEvent.TypeSyncPointEvent:
			if len(dispatcherEvents) != 1 {
				log.Panic("sync point event should only be singly handled", zap.Any("dispatcherID", d.id))
			}
			block = true
			event := event.(*commonEvent.SyncPointEvent)
			event.AddPostFlushFunc(func() {
				dispatcherEventDynamicStream := GetDispatcherEventsDynamicStream()
				dispatcherEventDynamicStream.Wake() <- event.GetDispatcherID()
			})
			d.dealWithBlockEvent(event)
		default:
			log.Panic("Unexpected event type", zap.Any("event Type", event.GetType()), zap.Stringer("dispatcher", d.id), zap.Uint64("commitTs", event.GetCommitTs()))
		}
	}
	return block
}

func (d *Dispatcher) checkHandshakeEvents(dispatcherEvents []DispatcherEvent) bool {
	for _, dispatcherEvent := range dispatcherEvents {
		event := dispatcherEvent.Event
		if event.GetType() == commonEvent.TypeHandshakeEvent {
			if event.GetCommitTs() == d.startTs.Load() {
				if event.GetSeq() != d.lastEventSeq.Add(1) {
					log.Panic("Receive handshake event, but seq is not the next one",
						zap.Any("event", event),
						zap.Stringer("dispatcher", d.id),
						zap.Uint64("lastEventSeq", d.lastEventSeq.Load()))
				}
				d.isReady.Store(true)
				log.Info("Receive handshake event, dispatcher is ready to handle events",
					zap.Any("dispatcher", d.id))
				return true
			} else {
				log.Warn("Handshake event commit ts not equal to dispatcher resolved ts",
					zap.Any("event", event),
					zap.Stringer("dispatcher", d.id),
					zap.Uint64("resolvedTs", d.resolvedTs.Get()),
					zap.Uint64("commitTs", event.GetCommitTs()))
			}
		}
		// Drop other events if dispatcher is not ready
	}
	return false
}

func shouldBlock(event commonEvent.BlockEvent) bool {
	switch event.GetType() {
	case commonEvent.TypeDDLEvent:
		ddlEvent := event.(*commonEvent.DDLEvent)
		return filter.ShouldBlock(model.ActionType(ddlEvent.Type))
	case commonEvent.TypeSyncPointEvent:
		return true
	default:
		log.Error("invalid event type", zap.Any("event Type", event.GetType()))
	}
	return false
}

func (d *Dispatcher) reset() {
	if !d.isReady.Load() {
		return
	}
	d.isReady.Store(false)
	d.lastEventSeq.Store(0)
	// Reset startTs to the checkpointTs
	d.startTs.Store(d.GetCheckpointTs())
	d.dispatcherActionChan <- common.DispatcherAction{
		DispatcherID: d.id,
		Action:       common.ActionReset,
	}
}

// 1.If the event is a single table DDL, it will be added to the sink for writing to downstream(async). If the ddl leads to add new tables or drop tables, it should send heartbeat to maintainer
// 2. If the event is a multi-table DDL / sync point Event, it will generate a TableSpanBlockStatus message with ddl info to send to maintainer.
func (d *Dispatcher) dealWithBlockEvent(event commonEvent.BlockEvent) {
	d.blockPendingEvent = event
	if !shouldBlock(d.blockPendingEvent) {
		d.sink.AddBlockEvent(d.blockPendingEvent, d.tableProgress)
		if d.blockPendingEvent.GetNeedAddedTables() != nil || d.blockPendingEvent.GetNeedDroppedTables() != nil {
			message := &heartbeatpb.TableSpanBlockStatus{
				ID: d.id.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:         false,
					BlockTs:           d.blockPendingEvent.GetCommitTs(),
					NeedDroppedTables: d.blockPendingEvent.GetNeedDroppedTables().ToPB(),
					NeedAddedTables:   commonEvent.ToTablesPB(d.blockPendingEvent.GetNeedAddedTables()),
					IsSyncPoint:       false, // sync point event must should block
					EventDone:         false,
				},
			}
			d.SetResendTask(newResendTask(message, d))
			d.blockStatusesChan <- message
		}
	} else {
		message := &heartbeatpb.TableSpanBlockStatus{
			ID: d.id.ToPB(),
			State: &heartbeatpb.State{
				IsBlocked:         true,
				BlockTs:           d.blockPendingEvent.GetCommitTs(),
				BlockTables:       d.blockPendingEvent.GetBlockedTables().ToPB(),
				NeedDroppedTables: d.blockPendingEvent.GetNeedDroppedTables().ToPB(),
				NeedAddedTables:   commonEvent.ToTablesPB(d.blockPendingEvent.GetNeedAddedTables()),
				UpdatedSchemas:    commonEvent.ToSchemaIDChangePB(d.blockPendingEvent.GetUpdatedSchemas()), // only exists for rename table and rename tables
				IsSyncPoint:       d.blockPendingEvent.GetType() == commonEvent.TypeSyncPointEvent,
				EventDone:         false,
			},
		}
		d.SetResendTask(newResendTask(message, d))
		d.blockStatusesChan <- message
	}

	// dealing with events which update schema ids
	// Only rename table and rename tables may update schema ids(rename db1.table1 to db2.table2)
	// Here we directly update schema id of dispatcher when we begin to handle the ddl event,
	// but not waiting maintainer response for ready to write/pass the ddl event.
	// Because the schemaID of each dispatcher is only use to dealing with the db-level ddl event(like drop db) or drop table.
	// Both the rename table/rename tables, drop table and db-level ddl event will be send to the table trigger event dispatcher in order.
	// So there won't be a related db-level ddl event is in dealing when we get update schema id events.
	// Thus, whether to update schema id before or after current ddl event is not important.
	// To make it easier, we choose to directly update schema id here.
	if d.blockPendingEvent.GetUpdatedSchemas() != nil && d.tableSpan != heartbeatpb.DDLSpan {
		for _, schemaIDChange := range d.blockPendingEvent.GetUpdatedSchemas() {
			if schemaIDChange.TableID == d.tableSpan.TableID {
				if schemaIDChange.OldSchemaID != d.schemaID {
					log.Error("Wrong Schema ID", zap.Any("dispatcherID", d.id), zap.Any("except schemaID", schemaIDChange.OldSchemaID), zap.Any("actual schemaID", d.schemaID), zap.Any("tableSpan", d.tableSpan.String()))
					return
				} else {
					d.schemaID = schemaIDChange.NewSchemaID
					d.schemaIDToDispatchers.Update(schemaIDChange.OldSchemaID, schemaIDChange.NewSchemaID)
					return
				}
			}
		}
	}
}

func (d *Dispatcher) GetTableSpan() *heartbeatpb.TableSpan {
	return d.tableSpan
}

func (d *Dispatcher) GetStartTs() uint64 {
	return d.startTs.Load()
}

func (d *Dispatcher) GetResolvedTs() uint64 {
	return d.resolvedTs.Get()
}

func (d *Dispatcher) GetCheckpointTs() uint64 {
	checkpointTs, isEmpty := d.tableProgress.GetCheckpointTs()
	if checkpointTs == 0 {
		// 说明从没有数据写到过 sink，则选择用 resolveTs 作为 checkpointTs
		return d.GetResolvedTs()
	}

	if isEmpty {
		return max(checkpointTs, d.GetResolvedTs())
	}
	return checkpointTs
}

func (d *Dispatcher) GetId() common.DispatcherID {
	return d.id
}

func (d *Dispatcher) CancelResendTask() {
	if d.resendTask != nil {
		d.resendTask.Cancel()
		d.resendTask = nil
	} else {
		log.Warn("try to cancel a nil resend task")
	}
}

func (d *Dispatcher) SetResendTask(task *ResendTask) {
	d.resendTask = task
}

func (d *Dispatcher) GetSchemaID() int64 {
	return d.schemaID
}

func (d *Dispatcher) EnableSyncPoint() bool {
	return d.SyncPointInfo.EnableSyncPoint
}

func (d *Dispatcher) GetSyncPointTs() uint64 {
	if d.SyncPointInfo.EnableSyncPoint {
		return d.SyncPointInfo.InitSyncPointTs
	} else {
		return 0
	}
}

func (d *Dispatcher) GetSyncPointInterval() time.Duration {
	if d.SyncPointInfo.EnableSyncPoint {
		return d.SyncPointInfo.SyncPointConfig.SyncPointInterval
	} else {
		return time.Duration(0)
	}
}

func (d *Dispatcher) Remove() {
	log.Info("table event dispatcher component status changed to stopping", zap.String("table", d.tableSpan.String()))
	d.isRemoving.Store(true)
	dispatcherEventDynamicStream := GetDispatcherEventsDynamicStream()
	err := dispatcherEventDynamicStream.RemovePath(d.id)
	if err != nil {
		log.Error("remove dispatcher from dynamic stream failed", zap.Error(err))
	}

	dispatcherStatusDynamicStream := GetDispatcherStatusDynamicStream()
	err = dispatcherStatusDynamicStream.RemovePath(d.id)
	if err != nil {
		log.Error("remove dispatcher from dynamic stream failed", zap.Error(err))
	}
}

func (d *Dispatcher) AddToDynamicStream() {
	dispatcherEventDynamicStream := GetDispatcherEventsDynamicStream()
	err := dispatcherEventDynamicStream.AddPath(d.id, d)
	if err != nil {
		log.Error("add dispatcher to dynamic stream failed", zap.Error(err))
	}

	dispatcherStatusDynamicStream := GetDispatcherStatusDynamicStream()
	err = dispatcherStatusDynamicStream.AddPath(d.id, d)
	if err != nil {
		log.Error("add dispatcher to dynamic stream failed", zap.Error(err))
	}
}

func (d *Dispatcher) TryClose() (w heartbeatpb.Watermark, ok bool) {
	// removing 后每次收集心跳的时候，call TryClose, 来判断是否能关掉 dispatcher 了（sink.isEmpty)
	// 如果不能关掉，返回 0， false; 可以关掉的话，就返回 checkpointTs, true -- 这个要对齐过（startTs 和 checkpointTs 的关系）
	if d.tableProgress.Empty() {
		w.CheckpointTs = d.GetCheckpointTs()
		w.ResolvedTs = d.GetResolvedTs()

		//d.MemoryUsage.Clear()
		d.componentStatus.Set(heartbeatpb.ComponentState_Stopped)
		return w, true
	}
	return w, false
}

func (d *Dispatcher) GetComponentStatus() heartbeatpb.ComponentState {
	return d.componentStatus.Get()
}

func (d *Dispatcher) GetRemovingStatus() bool {
	return d.isRemoving.Load()
}

func (d *Dispatcher) CollectDispatcherHeartBeatInfo(h *HeartBeatInfo) {
	// use checkpointTs to release memory usage
	//d.GetMemoryUsage().Release(checkpointTs)

	h.Watermark.CheckpointTs = d.GetCheckpointTs()
	h.Watermark.ResolvedTs = d.GetResolvedTs()
	h.Id = d.GetId()
	h.ComponentStatus = d.GetComponentStatus()
	h.TableSpan = d.GetTableSpan()
	h.IsRemoving = d.GetRemovingStatus()
}

func (d *Dispatcher) HandleCheckpointTs(checkpointTs uint64) {
	if d.tableNameStore == nil {
		log.Error("Should not HandleCheckpointTs for table trigger event dispatcher without tableNameStore")
		return
	}

	tableNames := d.tableNameStore.GetAllTableNames(checkpointTs)
	d.sink.AddCheckpointTs(checkpointTs, tableNames)
}
