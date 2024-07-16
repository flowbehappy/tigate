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
	"sync"
	"time"

	"github.com/flowbehappy/tigate/coordinator"
	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/eventpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/google/uuid"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

type SyncPointInfo struct {
	EnableSyncPoint   bool
	SyncPointInterval time.Duration
	NextSyncPointTs   uint64
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
	Id        common.DispatcherID
	Ch        chan *common.TxnEvent // 转换成一个函数
	TableSpan *common.TableSpan
	Sink      sink.Sink

	State      *State
	ResolvedTs uint64

	// 搞个 channel 来接收 heartbeat 产生的 信息，然后下推数据这个就可以做成 await 了
	// heartbeat 会更新依赖的 tableSpan 的 状态，然后满足了就删掉，下次发送就不用发了，但最终推动他变化的还是要收到 action
	HeartbeatChan chan *HeartBeatResponseMessage

	SyncPointInfo *SyncPointInfo

	MemoryUsage *MemoryUsage

	task *EventDispatcherTask

	tableInfo *common.TableInfo // TODO:后续做成一整个 tableInfo Struct

	statusMutex     sync.Mutex
	componentStatus coordinator.ComponentStatus
}

func NewTableEventDispatcher(tableSpan *common.TableSpan, sink sink.Sink, startTs uint64, syncPointInfo *SyncPointInfo) *TableEventDispatcher {
	tableEventDispatcher := &TableEventDispatcher{
		Id:              common.DispatcherID(uuid.New()),
		Ch:              make(chan *common.TxnEvent, 1000),
		TableSpan:       tableSpan,
		Sink:            sink,
		State:           NewState(),
		ResolvedTs:      startTs,
		HeartbeatChan:   make(chan *HeartBeatResponseMessage, 100),
		SyncPointInfo:   syncPointInfo,
		MemoryUsage:     NewMemoryUsage(),
		componentStatus: coordinator.ComponentStatusWorking,
	}
	tableEventDispatcher.Sink.AddTableSpan(tableSpan)
	tableEventDispatcher.task = NewEventDispatcherTask(tableEventDispatcher)
	threadpool.GetTaskSchedulerInstance().EventDispatcherTaskScheduler.Submit(tableEventDispatcher.task, threadpool.CPUTask, time.Time{})
	// 马上触发 心跳
	return tableEventDispatcher
}

func (d *TableEventDispatcher) GetSink() sink.Sink {
	return d.Sink
}

func (d *TableEventDispatcher) GetTableSpan() *common.TableSpan {
	return d.TableSpan
}

func (d *TableEventDispatcher) GetState() *State {
	return d.State
}

func (d *TableEventDispatcher) GetEventChan() chan *common.TxnEvent {
	return d.Ch
}

func (d *TableEventDispatcher) GetResolvedTs() uint64 {
	return d.ResolvedTs
}

func (d *TableEventDispatcher) GetId() common.DispatcherID {
	return d.Id
}

func (d *TableEventDispatcher) GetDispatcherType() DispatcherType {
	return TableEventDispatcherType
}

func (d *TableEventDispatcher) GetHeartBeatChan() chan *HeartBeatResponseMessage {
	return d.HeartbeatChan
}

func (d *TableEventDispatcher) UpdateResolvedTs(ts uint64) {
	d.ResolvedTs = ts
}

func (d *TableEventDispatcher) GetSyncPointInfo() *SyncPointInfo {
	return d.SyncPointInfo
}

func (d *TableEventDispatcher) GetMemoryUsage() *MemoryUsage {
	return d.MemoryUsage
}

func (d *TableEventDispatcher) decodeEvent(rawTxnEvent *eventpb.TxnEvent) (*common.TxnEvent, error) {
	var txnEvent *common.TxnEvent

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

		row, err := func() (*model.RowChangedEvent, error) {
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
	d.GetMemoryUsage().Add(event.CommitTs, event.MemoryCost())
	d.Ch <- event // 换成一个函数
}

func (d *TableEventDispatcher) InitTableInfo(tableInfo *eventpb.TableInfo) {
	//d.tableInfo = decodeTableInfo(tableInfo) // TODO
}

func (d *TableEventDispatcher) Remove() {
	// TODO: 修改这个 dispatcher 的 status 为 removing
	d.task.Cancel()
	d.Sink.StopTableSpan(d.TableSpan)

	d.statusMutex.Lock()
	defer d.statusMutex.Unlock()
	d.componentStatus = coordinator.ComponentStatusStopping
}

func (d *TableEventDispatcher) TryClose() (uint64, bool) {
	// removing 后每次收集心跳的时候，call TryClose, 来判断是否能关掉 dispatcher 了（sink.isEmpty)
	// 如果不能关掉，返回 0， false; 可以关掉的话，就返回 checkpointTs, true -- 这个要对齐过（startTs 和 checkpointTs 的关系）
	if d.Sink.IsEmpty(d.TableSpan) {
		// calculate the checkpointTs, and clean the resource
		d.Sink.RemoveTableSpan(d.TableSpan)
		var checkpointTs uint64
		state := d.GetState()
		if state.pengdingEvent == nil {
			checkpointTs = state.pengdingEvent.CommitTs - 1
		} else {
			checkpointTs = d.GetResolvedTs()
		}

		d.MemoryUsage.Clear()
		d.componentStatus = coordinator.ComponentStatusStopped
		return checkpointTs, true
	}
	return 0, false
}

func (d *TableEventDispatcher) GetComponentStatus() coordinator.ComponentStatus {
	d.statusMutex.Lock()
	defer d.statusMutex.Unlock()
	return d.componentStatus
}
