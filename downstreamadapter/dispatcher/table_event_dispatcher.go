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
	"time"

	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"go.uber.org/zap"
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
TableEventDispatcher implements the Dispatcher interface.

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
	tableSpanStatusesChan chan *heartbeatpb.TableSpanStatus

	//SyncPointInfo *SyncPointInfo

	//MemoryUsage *MemoryUsage

	componentStatus *ComponentStateWithMutex

	resolvedTs *TsWithMutex // 用来记 eventChan 中目前收到的 event 中收到的最大的 commitTs - 1,不代表 dispatcher 的 checkpointTs

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewTableEventDispatcher(tableSpan *common.TableSpan, sink sink.Sink, startTs uint64, syncPointInfo *SyncPointInfo, tableSpanStatusesChan chan *heartbeatpb.TableSpanStatus) *TableEventDispatcher {
	ctx, cancel := context.WithCancel(context.Background())
	tableEventDispatcher := &TableEventDispatcher{
		id:                    uuid.NewString(),
		eventCh:               make(chan *common.TxnEvent, 16),
		tableSpan:             tableSpan,
		sink:                  sink,
		ddlActions:            make(chan *heartbeatpb.DispatcherAction, 16),
		tableSpanStatusesChan: tableSpanStatusesChan,
		//SyncPointInfo:   syncPointInfo,
		//MemoryUsage:     NewMemoryUsage(),
		componentStatus: newComponentStateWithMutex(heartbeatpb.ComponentState_Working),
		resolvedTs:      newTsWithMutex(startTs),
		cancel:          cancel,
	}
	tableEventDispatcher.sink.AddTableSpan(tableSpan)
	tableEventDispatcher.wg.Add(1)
	go tableEventDispatcher.DispatcherEvents(ctx)

	log.Info("table event dispatcher created", zap.Any("DispatcherID", tableEventDispatcher.id))

	return tableEventDispatcher
}

//  1. 如果是单表内的 ddl，达到下推的条件为： sink 中没有还没执行完的当前表的 event
//  2. 如果是多表内的 ddl 或者是表间的 ddl，则需要满足的条件为：
//     2.1 sink 中没有还没执行完的当前表的 event
//     2.2 maintainer 通知自己可以 write 或者 pass event
func (d *TableEventDispatcher) AddDDLEventToSinkWhenAvailable(event *common.TxnEvent) {
	if event.IsSingleTableDDL() {
		if d.sink.IsEmpty(d.tableSpan) {
			d.sink.AddDMLEvent(d.tableSpan, event)
			return
		} else {
			// TODO:先写一个 定时 check 的逻辑，后面用 dynamic stream 改造
			timer := time.NewTimer(time.Millisecond * 50)
			for {
				select {
				case <-timer.C:
					if d.sink.IsEmpty(d.tableSpan) {
						d.sink.AddDMLEvent(d.tableSpan, event)
						return
					}
				}
			}
		}
	}

	d.tableSpanStatusesChan <- &heartbeatpb.TableSpanStatus{
		Span:            d.tableSpan.TableSpan,
		ComponentStatus: heartbeatpb.ComponentState_Working,
		State: &heartbeatpb.State{
			IsBlocked:            true,
			BlockTs:              event.CommitTs,
			BlockTableSpan:       event.GetBlockedTableSpan(), // 这个包含自己的 span 是不是也无所谓，不然就要剔除掉
			NeedDroppedTableSpan: event.GetNeedDroppedTableSpan(),
			NeedAddedTableSpan:   event.GetNeedAddedTableSpan(),
		},
	}

	for {
		dispatcherAction := <-d.ddlActions
		if dispatcherAction.CommitTs == event.CommitTs {
			if dispatcherAction.Action == heartbeatpb.Action_Write {
				d.sink.AddDDLAndSyncPointEvent(d.tableSpan, event) // 这个是同步写，所以写完的时候 sink 也 available 了
			}
			return
		}
	}
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
				d.AddDDLEventToSinkWhenAvailable(event)
			} else {
				// resolvedTs
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
	d.componentStatus.Set(heartbeatpb.ComponentState_Stopping)
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
