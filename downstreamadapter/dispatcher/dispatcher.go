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

	// TableSpanStatus use to report checkpointTs / componentStatus to Maintainer
	statusesChan chan *heartbeatpb.TableSpanStatus

	// TableSpanBlockStatus use to report block status of ddl/sync point event to Maintainer
	blockStatusesChan chan *heartbeatpb.TableSpanBlockStatus

	SyncPointInfo *syncpoint.SyncPointInfo

	componentStatus *ComponentStateWithMutex

	filter filter.Filter

	resolvedTs *TsWithMutex // 用来记 中目前收到的 event 中收到的最大的 commitTs - 1,不代表 dispatcher 的 checkpointTs

	blockPendingEvent commonEvent.BlockEvent
	isRemoving        atomic.Bool

	tableProgress *types.TableProgress

	resendTask                  *ResendTask
	checkTableProgressEmptyTask *CheckProgressEmptyTask

	schemaIDToDispatchers *SchemaIDToDispatchers
	schemaID              int64

	// only exist when the dispatcher is a table trigger event dispatcher
	tableNameStore *TableNameStore
}

func NewDispatcher(
	id common.DispatcherID,
	tableSpan *heartbeatpb.TableSpan,
	sink tisink.Sink,
	startTs uint64,
	statusesChan chan *heartbeatpb.TableSpanStatus,
	blockStatusesChan chan *heartbeatpb.TableSpanBlockStatus,
	filter filter.Filter,
	schemaID int64,
	schemaIDToDispatchers *SchemaIDToDispatchers,
	syncPointInfo *syncpoint.SyncPointInfo) *Dispatcher {
	dispatcher := &Dispatcher{
		id:                    id,
		tableSpan:             tableSpan,
		sink:                  sink,
		statusesChan:          statusesChan,
		blockStatusesChan:     blockStatusesChan,
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

	// only when is not mysql sink, table trigger event dispatcher need tableNameStore to store the table name
	// in order to calculate all the topics when sending checkpointTs to downstream
	if tableSpan.Equal(heartbeatpb.DDLSpan) && dispatcher.sink.SinkType() != tisink.MysqlSinkType {
		dispatcher.tableNameStore = NewTableNameStore()
	}

	dispatcherStatusDynamicStream := GetDispatcherStatusDynamicStream()
	err := dispatcherStatusDynamicStream.AddPath(dispatcher.id, dispatcher)
	if err != nil {
		log.Error("add dispatcher to dynamic stream failed", zap.Error(err))
	}

	dispatcherEventsDynamicStream := GetDispatcherEventsDynamicStream()
	err = dispatcherEventsDynamicStream.AddPath(dispatcher.id, dispatcher)
	if err != nil {
		log.Error("add dispatcher to dynamic stream failed", zap.Error(err))
	}

	return dispatcher
}

//  1. 如果是单表内的 ddl，达到下推的条件为： sink 中没有还没执行完的当前表的 event
//  2. 如果是多表内的 ddl 或者是表间的 ddl，则需要满足的条件为：
//     2.1 sink 中没有还没执行完的当前表的 event
//     2.2 maintainer 通知自己可以 write 或者 pass event
//
// TODO:特殊处理有 add index 的逻辑
// Block Event including ddl Event and Sync Point Event
func (d *Dispatcher) addBlockEventToSinkWhenAvailable(event commonEvent.BlockEvent) {
	// 根据 filter 过滤 query 中不需要 send to downstream 的数据
	// 但应当不出现整个 query 都不需要 send to downstream 的 ddl，这种 ddl 不应该发给 dispatcher
	// TODO: ddl 影响到的 tableSpan 也在 filter 中过滤一遍
	if event.GetType() == commonEvent.TypeDDLEvent {
		ddlEvent := event.(*commonEvent.DDLEvent)
		// TODO:看一下这种写法有没有问题，加个测试后面
		err := d.filter.FilterDDLEvent(ddlEvent)
		if err != nil {
			log.Error("filter ddl query failed", zap.Error(err))
			// 这里怎么处理更合适呢？有错然后反上去让 changefeed 报错
			return
		}
	}

	d.blockPendingEvent = event

	if d.tableProgress.Empty() {
		d.DealWithBlockEventWhenProgressEmpty()
	} else {
		d.checkTableProgressEmptyTask = newCheckProgressEmptyTask(d)
	}
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

func (d *Dispatcher) HandleEvent(event commonEvent.Event) (block bool) {
	switch event.GetType() {
	case commonEvent.TypeResolvedEvent:
		d.resolvedTs.Set(event.(commonEvent.ResolvedEvent).ResolvedTs)
		return false
	case commonEvent.TypeDMLEvent:
		d.sink.AddDMLEvent(event.(*commonEvent.DMLEvent), d.tableProgress)
		return false
	case commonEvent.TypeDDLEvent:
		event := event.(*commonEvent.DDLEvent)
		if d.tableNameStore != nil {
			d.tableNameStore.AddEvent(event)
		}
		event.AddPostFlushFunc(func() {
			dispatcherEventDynamicStream := GetDispatcherEventsDynamicStream()
			dispatcherEventDynamicStream.Wake() <- event.GetDispatcherID()
		})
		d.addBlockEventToSinkWhenAvailable(event)
		return true
	case commonEvent.TypeSyncPointEvent:
		event := event.(*commonEvent.SyncPointEvent)
		event.AddPostFlushFunc(func() {
			dispatcherEventDynamicStream := GetDispatcherEventsDynamicStream()
			dispatcherEventDynamicStream.Wake() <- event.GetDispatcherID()
		})
		d.addBlockEventToSinkWhenAvailable(event)
		return true
	default:
		log.Error("invalid event type", zap.Any("event Type", event.GetType()))
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

// 1.If the event is a single table DDL, it will be added to the sink for writing to downstream(async). If the ddl leads to add new tables or drop tables, it should send heartbeat to maintainer
// 2. If the event is a multi-table DDL / sync point Event, it will generate a TableSpanBlockStatus message with ddl info to send to maintainer.
func (d *Dispatcher) DealWithBlockEventWhenProgressEmpty() {
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
	// TODO: 修改这个 dispatcher 的 status 为 removing
	log.Info("table event dispatcher component status changed to stopping", zap.String("table", d.tableSpan.String()))
	d.isRemoving.Store(true)

	dispatcherEventDynamicStream := GetDispatcherEventsDynamicStream()
	errs := dispatcherEventDynamicStream.RemovePaths(d.id)

	for _, err := range errs {
		if err != nil {
			log.Error("remove dispatcher from dynamic stream failed", zap.Error(err))
		}
	}

	dispatcherStatusDynamicStream := GetDispatcherStatusDynamicStream()
	errs = dispatcherStatusDynamicStream.RemovePaths(d.id)
	for _, err := range errs {
		if err != nil {
			log.Error("remove dispatcher from dynamic stream failed", zap.Error(err))
		}
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
