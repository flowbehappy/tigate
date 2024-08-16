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

	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/filter"
	"go.uber.org/zap"
)

/*
TableEventDispatcher is dispatcher the event of a normal tableSpan in a changefeed.
It is responsible for getting the events about the tableSpan from the Logservice and sending them to the Sink in an appropriate order.

It communicates with the Maintainer periodically to report self progress,
and get the other dispatcher's progress and action of the blocked event.

Each EventDispatcherManager can have multiple TableEventDispatcher.
*/
type TableEventDispatcher struct {
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

func NewTableEventDispatcher(tableSpan *common.TableSpan, sink sink.Sink, startTs uint64, syncPointInfo *SyncPointInfo, tableSpanStatusesChan chan *heartbeatpb.TableSpanStatus, filter filter.Filter) *TableEventDispatcher {
	ctx, cancel := context.WithCancel(context.Background())
	tableEventDispatcher := &TableEventDispatcher{
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

	tableEventDispatcher.sink.AddTableSpan(tableSpan)
	tableEventDispatcher.wg.Add(1)
	go tableEventDispatcher.DispatcherEvents(ctx)

	tableEventDispatcher.wg.Add(1)
	go HandleDDLActions(tableEventDispatcher, ctx)

	log.Info("table event dispatcher created", zap.Any("DispatcherID", tableEventDispatcher.id))

	return tableEventDispatcher
}

func (d *TableEventDispatcher) DispatcherEvents(ctx context.Context) {
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
				AddDDLEventToSinkWhenAvailable(d, event)
			} else {
				d.resolvedTs.Set(event.ResolvedTs)
			}
		}
	}
}

func (d *TableEventDispatcher) GetSink() sink.Sink {
	return d.sink
}

func (d *TableEventDispatcher) GetTableSpan() *common.TableSpan {
	return d.tableSpan
}

func (d *TableEventDispatcher) GetEventChan() chan *common.TxnEvent {
	return d.eventCh
}

func (d *TableEventDispatcher) GetResolvedTs() uint64 {
	return d.resolvedTs.Get()
}

func (d *TableEventDispatcher) GetCheckpointTs() uint64 {
	checkpointTs := d.GetSink().GetCheckpointTs(d.GetTableSpan())
	if checkpointTs == 0 {
		// 说明从没有数据写到过 sink，则选择用 resolveTs 作为 checkpointTs
		checkpointTs = d.GetResolvedTs()
	}
	return checkpointTs
}

func (d *TableEventDispatcher) UpdateResolvedTs(ts uint64) {
	d.GetEventChan() <- &common.TxnEvent{ResolvedTs: ts}
}

func (d *TableEventDispatcher) GetId() string {
	return d.id
}

func (d *TableEventDispatcher) GetDispatcherType() DispatcherType {
	return TableEventDispatcherType
}

func (d *TableEventDispatcher) GetDDLActions() chan *heartbeatpb.DispatcherAction {
	return d.ddlActions
}

func (d *TableEventDispatcher) GetACKs() chan *heartbeatpb.ACK {
	return d.acks
}

func (d *TableEventDispatcher) GetTableSpanStatusesChan() chan *heartbeatpb.TableSpanStatus {
	return d.tableSpanStatusesChan
}

//func (d *TableEventDispatcher) GetSyncPointInfo() *SyncPointInfo {
// 	return d.syncPointInfo
// }

// func (d *TableEventDispatcher) GetMemoryUsage() *MemoryUsage {
// 	return d.MemoryUsage
// }

func (d *TableEventDispatcher) PushTxnEvent(event *common.TxnEvent) {
	d.GetEventChan() <- event
}

func (d *TableEventDispatcher) Remove() {
	// TODO: 修改这个 dispatcher 的 status 为 removing
	d.cancel()
	d.sink.StopTableSpan(d.tableSpan)
	log.Info("table event dispatcher component status changed to stopping", zap.String("table", d.tableSpan.String()))
	d.isRemoving.Store(true)
}

func (d *TableEventDispatcher) TryClose() (w heartbeatpb.Watermark, ok bool) {
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

func (d *TableEventDispatcher) GetComponentStatus() heartbeatpb.ComponentState {
	return d.componentStatus.Get()
}

func (d *TableEventDispatcher) GetFilter() filter.Filter {
	return d.filter
}

func (d *TableEventDispatcher) GetWG() *sync.WaitGroup {
	return &d.wg
}

func (d *TableEventDispatcher) GetDDLPendingEvent() *common.TxnEvent {
	return d.ddlPendingEvent
}

func (d *TableEventDispatcher) SetDDLPendingEvent(event *common.TxnEvent) {
	if d.ddlPendingEvent != nil {
		log.Error("there is already a pending ddl event, can not set a new one")
		return
	}
	d.ddlPendingEvent = event
}
func (d *TableEventDispatcher) GetDDLFinishCh() chan struct{} {
	return d.ddlFinishCh
}
func (d *TableEventDispatcher) GetRemovingStatus() bool {
	return d.isRemoving.Load()
}
