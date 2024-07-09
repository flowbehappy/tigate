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
	"time"

	"github.com/flowbehappy/tigate/common"
	"github.com/flowbehappy/tigate/downstreamadapter/sink"
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
	Id        uint64
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

func (d *TableEventDispatcher) GetId() uint64 {
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
