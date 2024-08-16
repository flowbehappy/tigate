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
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/google/uuid"
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
	id        string
	eventCh   chan *common.TxnEvent // 转换成一个函数
	tableSpan *common.TableSpan
	sink      sink.Sink

	ddlActions            chan *heartbeatpb.DispatcherAction
	acks                  chan *heartbeatpb.ACK
	tableSpanStatusesChan chan *heartbeatpb.TableSpanStatus

	//SyncPointInfo *SyncPointInfo

	//MemoryUsage *MemoryUsage

	componentStatus *ComponentStateWithMutex

	filter filter.Filter

	resolvedTs *TsWithMutex // 用来记 eventChan 中目前收到的 event 中收到的最大的 commitTs - 1,不代表 dispatcher 的 checkpointTs

	cancel context.CancelFunc
	wg     sync.WaitGroup

	ddlPendingEvent *common.TxnEvent
	ddlFinishCh     chan struct{}
	isRemoving      atomic.Bool
}

func NewDispatcher(tableSpan *common.TableSpan, sink sink.Sink, startTs uint64, tableSpanStatusesChan chan *heartbeatpb.TableSpanStatus, filter filter.Filter) *Dispatcher {
	ctx, cancel := context.WithCancel(context.Background())
	dispatcher := &Dispatcher{
		id:                    uuid.NewString(),
		eventCh:               make(chan *common.TxnEvent, 16),
		tableSpan:             tableSpan,
		sink:                  sink,
		ddlActions:            make(chan *heartbeatpb.DispatcherAction, 16),
		acks:                  make(chan *heartbeatpb.ACK, 16),
		tableSpanStatusesChan: tableSpanStatusesChan,
		//SyncPointInfo:   syncPointInfo,
		//MemoryUsage:     NewMemoryUsage(),
		componentStatus: newComponentStateWithMutex(heartbeatpb.ComponentState_Working),
		resolvedTs:      newTsWithMutex(startTs),
		cancel:          cancel,
		filter:          filter,
		ddlFinishCh:     make(chan struct{}),
		isRemoving:      atomic.Bool{},
	}

	dispatcher.sink.AddTableSpan(tableSpan)
	dispatcher.wg.Add(1)
	go dispatcher.DispatcherEvents(ctx)

	dispatcher.wg.Add(1)
	go dispatcher.HandleDDLActions(ctx)

	log.Info("dispatcher created", zap.Any("DispatcherID", dispatcher.id))

	return dispatcher
}

func (d *Dispatcher) DispatcherEvents(ctx context.Context) {
	defer d.wg.Done()
	tableSpan := d.GetTableSpan()
	sink := d.GetSink()
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-d.GetEventChan():
			if event.IsDMLEvent() {
				sink.AddDMLEvent(tableSpan, event)
			} else if event.IsDDLEvent() {
				d.AddDDLEventToSinkWhenAvailable(event)
			} else {
				d.resolvedTs.Set(event.ResolvedTs)
			}
		}
	}
}

func (d *Dispatcher) GetSink() sink.Sink {
	return d.sink
}

func (d *Dispatcher) GetTableSpan() *common.TableSpan {
	return d.tableSpan
}

func (d *Dispatcher) GetEventChan() chan *common.TxnEvent {
	return d.eventCh
}

func (d *Dispatcher) GetResolvedTs() uint64 {
	return d.resolvedTs.Get()
}

func (d *Dispatcher) GetCheckpointTs() uint64 {
	checkpointTs := d.GetSink().GetCheckpointTs(d.GetTableSpan())
	if checkpointTs == 0 {
		// 说明从没有数据写到过 sink，则选择用 resolveTs 作为 checkpointTs
		checkpointTs = d.GetResolvedTs()
	}
	return checkpointTs
}

func (d *Dispatcher) UpdateResolvedTs(ts uint64) {
	d.GetEventChan() <- &common.TxnEvent{ResolvedTs: ts}
}

func (d *Dispatcher) GetId() string {
	return d.id
}

// func (d *Dispatcher) GetDispatcherType() DispatcherType {
// 	return TableEventDispatcherType
// }

func (d *Dispatcher) GetDDLActions() chan *heartbeatpb.DispatcherAction {
	return d.ddlActions
}

func (d *Dispatcher) GetACKs() chan *heartbeatpb.ACK {
	return d.acks
}

func (d *Dispatcher) GetTableSpanStatusesChan() chan *heartbeatpb.TableSpanStatus {
	return d.tableSpanStatusesChan
}

//func (d *Dispatcher) GetSyncPointInfo() *SyncPointInfo {
// 	return d.syncPointInfo
// }

// func (d *Dispatcher) GetMemoryUsage() *MemoryUsage {
// 	return d.MemoryUsage
// }

func (d *Dispatcher) PushTxnEvent(event *common.TxnEvent) {
	d.GetEventChan() <- event
}

func (d *Dispatcher) Remove() {
	// TODO: 修改这个 dispatcher 的 status 为 removing
	d.cancel()
	d.sink.StopTableSpan(d.tableSpan)
	log.Info("table event dispatcher component status changed to stopping", zap.String("table", d.tableSpan.String()))
	d.isRemoving.Store(true)
}

func (d *Dispatcher) TryClose() (w heartbeatpb.Watermark, ok bool) {
	// removing 后每次收集心跳的时候，call TryClose, 来判断是否能关掉 dispatcher 了（sink.isEmpty)
	// 如果不能关掉，返回 0， false; 可以关掉的话，就返回 checkpointTs, true -- 这个要对齐过（startTs 和 checkpointTs 的关系）
	if d.sink.IsEmpty(d.tableSpan) {
		// calculate the checkpointTs, and clean the resource
		d.sink.RemoveTableSpan(d.tableSpan)
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

func (d *Dispatcher) GetWG() *sync.WaitGroup {
	return &d.wg
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

func (d *Dispatcher) GetDDLFinishCh() chan struct{} {
	return d.ddlFinishCh
}
func (d *Dispatcher) GetRemovingStatus() bool {
	return d.isRemoving.Load()
}

func (d *Dispatcher) HandleDDLActions(ctx context.Context) {
	defer d.GetWG().Done()
	sink := d.GetSink()
	tableSpan := d.GetTableSpan()
	for {
		select {
		case <-ctx.Done():
			return
		case dispatcherAction := <-d.GetDDLActions():
			event := d.GetDDLPendingEvent()
			if event == nil {
				// 只可能出现在 event 已经推进了，但是还重复收到了 action 消息的时候，则重发包含 checkpointTs 的心跳
				d.GetTableSpanStatusesChan() <- &heartbeatpb.TableSpanStatus{
					Span:            tableSpan.TableSpan,
					ComponentStatus: heartbeatpb.ComponentState_Working,
					CheckpointTs:    d.GetCheckpointTs(),
				}
				continue
			}
			if dispatcherAction.CommitTs == event.CommitTs {
				if dispatcherAction.Action == heartbeatpb.Action_Write {
					sink.AddDDLAndSyncPointEvent(tableSpan, event) // 这个是同步写，所以写完的时候 sink 也 available 了
				} else {
					sink.PassDDLAndSyncPointEvent(tableSpan, event) // 为了更新 tableProgress，避免 checkpointTs 计算的 corner case
				}
				d.GetTableSpanStatusesChan() <- &heartbeatpb.TableSpanStatus{
					Span:            tableSpan.TableSpan,
					ComponentStatus: heartbeatpb.ComponentState_Working,
					CheckpointTs:    d.GetCheckpointTs(),
				}
				d.GetDDLFinishCh() <- struct{}{}
				return
			}
		}
	}
}

//  1. 如果是单表内的 ddl，达到下推的条件为： sink 中没有还没执行完的当前表的 event
//  2. 如果是多表内的 ddl 或者是表间的 ddl，则需要满足的条件为：
//     2.1 sink 中没有还没执行完的当前表的 event
//     2.2 maintainer 通知自己可以 write 或者 pass event
//
// TODO:特殊处理有 add index 的逻辑
func (d *Dispatcher) AddDDLEventToSinkWhenAvailable(event *common.TxnEvent) {
	// 根据 filter 过滤 query 中不需要 send to downstream 的数据
	// 但应当不出现整个 query 都不需要 send to downstream 的 ddl，这种 ddl 不应该发给 dispatcher
	// TODO: ddl 影响到的 tableSpan 也在 filter 中过滤一遍
	filter := d.GetFilter()
	err := filter.FilterDDLQuery(event.GetDDLEvent())
	if err != nil {
		log.Error("filter ddl query failed", zap.Error(err))
		// 这里怎么处理更合适呢？有错然后反上去让 changefeed 报错
		return
	}

	sink := d.GetSink()
	tableSpan := d.GetTableSpan()
	if event.IsSingleTableDDL() {
		if sink.IsEmpty(tableSpan) {
			sink.AddDDLAndSyncPointEvent(tableSpan, event)
			return
		} else {
			// TODO:先写一个 定时 check 的逻辑，后面用 dynamic stream 改造
			timer := time.NewTimer(time.Millisecond * 50)
			for {
				select {
				case <-timer.C:
					if sink.IsEmpty(tableSpan) {
						sink.AddDDLAndSyncPointEvent(tableSpan, event)
						return
					}
				}
			}
		}
	}

	d.SetDDLPendingEvent(event)

	// TODO:消息需要保证发送后收到 ack 才可以停止重发，具体重发时间需要调整
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
	timer := time.NewTimer(time.Millisecond * 100)
loop:
	for {
		select {
		case <-timer.C:
			// 重发消息
			d.GetTableSpanStatusesChan() <- message
		case ack := <-d.GetACKs():
			if ack.CommitTs == event.CommitTs {
				break loop
			}
		}
	}

	// 收到 ack 以后可以开始等 actions 来进行处理,等待 finish 信号
	<-d.GetDDLFinishCh()
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
