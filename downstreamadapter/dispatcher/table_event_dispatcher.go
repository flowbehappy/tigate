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
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/eventpb"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
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
	id        common.DispatcherID
	eventCh   chan *common.TxnEvent // 转换成一个函数
	tableSpan *common.TableSpan
	sink      sink.Sink

	state *State

	// 搞个 channel 来接收 heartbeat 产生的 信息，然后下推数据这个就可以做成 await 了
	// heartbeat 会更新依赖的 tableSpan 的 状态，然后满足了就删掉，下次发送就不用发了，但最终推动他变化的还是要收到 action
	heartbeatChan chan *HeartBeatResponseMessage

	//SyncPointInfo *SyncPointInfo

	//MemoryUsage *MemoryUsage

	tableInfo *common.TableInfo // TODO:后续做成一整个 tableInfo Struct

	componentStatus *ComponentStateWithMutex

	resolvedTs *TsWithMutex // 用来记 eventChan 中目前收到的 event 中收到的最大的 commitTs - 1,不代表 dispatcher 的 checkpointTs

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewTableEventDispatcher(tableSpan *common.TableSpan, sink sink.Sink, startTs uint64, syncPointInfo *SyncPointInfo) *TableEventDispatcher {
	ctx, cancel := context.WithCancel(context.Background())
	tableEventDispatcher := &TableEventDispatcher{
		id:            common.DispatcherID(uuid.New()),
		eventCh:       make(chan *common.TxnEvent, 1000),
		tableSpan:     tableSpan,
		sink:          sink,
		state:         NewState(),
		heartbeatChan: make(chan *HeartBeatResponseMessage, 100),
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

func (d *TableEventDispatcher) DispatcherEvents(ctx context.Context) {
	defer d.wg.Done()
	tableSpan := d.GetTableSpan()
	sink := d.GetSink()
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-d.GetEventChan():
			//log.Info("TableEventDispatcher get events from eventChan", zap.Any("event is DML", event.IsDMLEvent()), zap.Any("dispatcher table id", d.tableSpan.TableID))
			if event.IsDMLEvent() {
				sink.AddDMLEvent(tableSpan, event)
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

func (d *TableEventDispatcher) GetState() *State {
	return d.state
}

func (d *TableEventDispatcher) GetEventChan() chan *common.TxnEvent {
	return d.eventCh
}

func (d *TableEventDispatcher) GetResolvedTs() uint64 {
	return d.resolvedTs.Get()
}

func (d *TableEventDispatcher) GetCheckpointTs() uint64 {
	var checkpointTs uint64
	smallestCommitTsInSink := d.GetSink().GetSmallestCommitTs(d.GetTableSpan())
	if smallestCommitTsInSink == 0 {
		// state := d.GetState()
		// if state.pengdingEvent != nil {
		// 	checkpointTs = state.pengdingEvent.CommitTs - 1
		// } else {
		checkpointTs = d.GetResolvedTs()
		//}
	} else {
		checkpointTs = smallestCommitTsInSink - 1
	}
	return checkpointTs
}

func (d *TableEventDispatcher) UpdateResolvedTs(ts uint64) {
	d.GetEventChan() <- &common.TxnEvent{ResolvedTs: ts}
}

func (d *TableEventDispatcher) GetId() common.DispatcherID {
	return d.id
}

func (d *TableEventDispatcher) GetDispatcherType() DispatcherType {
	return TableEventDispatcherType
}

func (d *TableEventDispatcher) GetHeartBeatChan() chan *HeartBeatResponseMessage {
	return d.heartbeatChan
}

//func (d *TableEventDispatcher) GetSyncPointInfo() *SyncPointInfo {
// 	return d.syncPointInfo
// }

// func (d *TableEventDispatcher) GetMemoryUsage() *MemoryUsage {
// 	return d.MemoryUsage
// }

func (d *TableEventDispatcher) decodeEvent(rawTxnEvent *eventpb.TxnEvent) (*common.TxnEvent, error) {
	txnEvent := &common.TxnEvent{}
	for _, rawEvent := range rawTxnEvent.Events {
		key, physicalTableID, err := decodeTableID(rawEvent.Key)
		if err != nil {
			return nil, err
		}

		if len(rawEvent.OldValue) == 0 && len(rawEvent.Value) == 0 {
			log.Warn("empty value and old value",
				zap.Any("raw event", rawEvent))
		}

		baseInfo := baseKVEntry{
			StartTs:         rawTxnEvent.StartTs,
			CRTs:            rawTxnEvent.CommitTs,
			PhysicalTableID: physicalTableID,
			Delete:          rawEvent.OpType == eventpb.OpType_OpTypeDelete,
		}

		row, _ := func() (*model.RowChangedEvent, error) {
			if bytes.HasPrefix(key, recordPrefix) {
				rowKV, err := unmarshalRowKVEntry(d.tableInfo, rawEvent.Key, rawEvent.Value, rawEvent.OldValue, baseInfo)
				if err != nil {
					return nil, errors.Trace(err)
				}
				if rowKV == nil {
					return nil, nil
				}
				row, _, err := mountRowKVEntry(d.tableInfo, rowKV)
				if err != nil {
					return nil, err
				}
				return row, nil
			}
			return nil, nil
		}()

		if err != nil {
			return nil, err
		}
		txnEvent.Rows = append(txnEvent.Rows, &common.RowChangedEvent{
			PhysicalTableID: row.PhysicalTableID,
			TableInfo:       d.tableInfo,
			ReplicatingTs:   row.ReplicatingTs,
			Columns:         common.ColumnDatas2Columns(row.Columns, d.tableInfo),
			PreColumns:      common.ColumnDatas2Columns(row.PreColumns, d.tableInfo),
		})
	}
	return txnEvent, nil
}

func (d *TableEventDispatcher) PushEvent(rawTxnEvent *eventpb.TxnEvent) {
	// decode the raw event to common.TxnEvent
	event, _ := d.decodeEvent(rawTxnEvent)
	//d.GetMemoryUsage().Add(event.CommitTs, event.MemoryCost())
	d.GetEventChan() <- event // 换成一个函数
}

func (d *TableEventDispatcher) PushTxnEvent(event *common.TxnEvent) {
	d.GetEventChan() <- event
}

func (d *TableEventDispatcher) InitTableInfo(tableInfo *eventpb.TableInfo) {
	//d.tableInfo = decodeTableInfo(tableInfo) // TODO
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
		state := d.GetState()
		if state.pengdingEvent != nil {
			w.CheckpointTs = state.pengdingEvent.CommitTs - 1
		} else {
			w.CheckpointTs = d.GetCheckpointTs()
		}
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
