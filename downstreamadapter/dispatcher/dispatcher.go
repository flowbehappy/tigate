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

	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/downstreamadapter/sink/types"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/flowbehappy/tigate/utils/dynstream"
	"github.com/pingcap/log"
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
	tableSpan *common.TableSpan
	sink      sink.Sink

	tableSpanStatusesChan chan *heartbeatpb.TableSpanStatus

	//SyncPointInfo *SyncPointInfo

	//MemoryUsage *MemoryUsage

	componentStatus *ComponentStateWithMutex

	filter filter.Filter

	resolvedTs *TsWithMutex // 用来记 中目前收到的 event 中收到的最大的 commitTs - 1,不代表 dispatcher 的 checkpointTs

	ddlPendingEvent *common.TxnEvent
	isRemoving      atomic.Bool

	tableProgress *types.TableProgress

	resendTask                  *ResendTask
	checkTableProgressEmptyTask *CheckTableProgressEmptyTask
}

func NewDispatcher(tableSpan *common.TableSpan, sink sink.Sink, startTs uint64, tableSpanStatusesChan chan *heartbeatpb.TableSpanStatus, filter filter.Filter) *Dispatcher {
	dispatcher := &Dispatcher{
		id:                    common.NewDispatcherID(),
		tableSpan:             tableSpan,
		sink:                  sink,
		tableSpanStatusesChan: tableSpanStatusesChan,
		//SyncPointInfo:   syncPointInfo,
		//MemoryUsage:     NewMemoryUsage(),
		componentStatus: newComponentStateWithMutex(heartbeatpb.ComponentState_Working),
		resolvedTs:      newTsWithMutex(startTs),
		filter:          filter,
		isRemoving:      atomic.Bool{},
		ddlPendingEvent: nil,
		tableProgress:   types.NewTableProgress(),
	}

	dispatcherEventsDynamicStream := appcontext.GetService[dynstream.DynamicStream[common.DispatcherID, *common.TxnEvent, *Dispatcher]](appcontext.DispatcherEventsDynamicStream)

	err := dispatcherEventsDynamicStream.AddPath(dynstream.PathAndDest[common.DispatcherID, *Dispatcher]{Path: dispatcher.id, Dest: dispatcher})
	if err != nil {
		log.Error("add dispatcher to dynamic stream failed", zap.Error(err))
	}

	dispatcherStatusDynamicStream := appcontext.GetService[dynstream.DynamicStream[common.DispatcherID, DispatcherStatusWithDispatcherID, *Dispatcher]](appcontext.DispatcherStatusDynamicStream)
	err = dispatcherStatusDynamicStream.AddPath(dynstream.PathAndDest[common.DispatcherID, *Dispatcher]{Path: dispatcher.id, Dest: dispatcher})
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
func (d *Dispatcher) AddDDLEventToSinkWhenAvailable(event *common.TxnEvent) bool {
	// 根据 filter 过滤 query 中不需要 send to downstream 的数据
	// 但应当不出现整个 query 都不需要 send to downstream 的 ddl，这种 ddl 不应该发给 dispatcher
	// TODO: ddl 影响到的 tableSpan 也在 filter 中过滤一遍
	filter := d.GetFilter()
	err := filter.FilterDDLEvent(event.GetDDLEvent())
	if err != nil {
		log.Error("filter ddl query failed", zap.Error(err))
		// 这里怎么处理更合适呢？有错然后反上去让 changefeed 报错
		return false
	}

	sink := d.GetSink()
	tableSpan := d.GetTableSpan()
	if event.IsSingleTableDDL() {
		if d.tableProgress.Empty() {
			sink.AddDDLAndSyncPointEvent(event, d.tableProgress)
			return true // 不能直接在内部调用写入，需要用异步操作
		} else {
			d.SetDDLPendingEvent(event)
			d.checkTableProgressEmptyTask = newCheckTableProgressEmptyTask(d)
		}
	} else {
		// cross ddl 也需要前序 dml 写完了才能开始判断是否可以下推
		d.SetDDLPendingEvent(event)
		if d.tableProgress.Empty() {
			message := &heartbeatpb.TableSpanStatus{
				Span:            tableSpan.TableSpan,
				ComponentStatus: heartbeatpb.ComponentState_Working,
				State: &heartbeatpb.State{
					IsBlocked:            true,
					BlockTs:              event.CommitTs,
					BlockTableSpan:       event.GetBlockedTableSpan(), // 这个包含自己的 span 是不是也无所谓，不然就要剔除掉
					NeedDroppedTableSpan: event.GetNeedDroppedTableSpan(),
					NeedAddedTableSpan:   event.GetNeedAddedTableSpan(),
				},
			}
			d.GetTableSpanStatusesChan() <- message
			d.SetResendTask(newResendTask(message, d))
		} else {
			d.checkTableProgressEmptyTask = newCheckTableProgressEmptyTask(d)
		}
	}
	return true
}

func (d *Dispatcher) GetSink() sink.Sink {
	return d.sink
}

func (d *Dispatcher) GetTableSpan() *common.TableSpan {
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

func (d *Dispatcher) GetTableSpanStatusesChan() chan *heartbeatpb.TableSpanStatus {
	return d.tableSpanStatusesChan
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

//func (d *Dispatcher) GetSyncPointInfo() *SyncPointInfo {
// 	return d.syncPointInfo
// }

// func (d *Dispatcher) GetMemoryUsage() *MemoryUsage {
// 	return d.MemoryUsage
// }

func (d *Dispatcher) Remove() {
	// TODO: 修改这个 dispatcher 的 status 为 removing
	log.Info("table event dispatcher component status changed to stopping", zap.String("table", d.tableSpan.String()))
	d.isRemoving.Store(true)

	dispatcherEventDynamicStream := appcontext.GetService[dynstream.DynamicStream[common.DispatcherID, *common.TxnEvent, *Dispatcher]](appcontext.DispatcherEventsDynamicStream)
	errs := dispatcherEventDynamicStream.RemovePath(d.id)

	for _, err := range errs {
		if err != nil {
			log.Error("remove dispatcher from dynamic stream failed", zap.Error(err))
		}
	}

	dispatcherStatusDynamicStream := appcontext.GetService[dynstream.DynamicStream[common.DispatcherID, *heartbeatpb.TableSpanStatus, *Dispatcher]](appcontext.DispatcherStatusDynamicStream)
	errs = dispatcherStatusDynamicStream.RemovePath(d.id)
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

func (d *Dispatcher) GetFilter() filter.Filter {
	return d.filter
}

func (d *Dispatcher) GetDDLPendingEvent() *common.TxnEvent {
	return d.ddlPendingEvent
}

func (d *Dispatcher) SetDDLPendingEvent(event *common.TxnEvent) {
	if d.ddlPendingEvent != nil {
		log.Error("there is already a pending ddl event, can not set a new one")
		return
	}
	d.ddlPendingEvent = event
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
