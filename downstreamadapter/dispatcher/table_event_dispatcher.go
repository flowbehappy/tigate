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
	"new_arch/downstreamadapter/sink"
	"time"
)

type TableSpanProgress struct {
	Span         *TableSpan
	IsBlocked    bool
	BlockTs      uint64
	CheckpointTs uint64
}

type HeartBeatResponseMessage struct { // 最好需要一个对应，对应 blocked by 什么 event 的 信号，避免出现乱序的问题
	Action             Action
	OtherTableProgress []*TableSpanProgress
}

type SyncPointInfo struct {
	EnableSyncPoint   bool
	SyncPointInterval time.Duration
	NextSyncPointTs   uint64
}

type TableEventDispatcher struct {
	Id        uint64
	Ch        <-chan *Event // 转换成一个函数
	TableSpan *Span
	Sink      sink.Sink

	State      *State
	ResolvedTs uint64

	// 搞个 channel 来接收 heartbeat 产生的 信息，然后下推数据这个就可以做成 await 了
	// heartbeat 会更新依赖的 tableSpan 的 状态，然后满足了就删掉，下次发送就不用发了，但最终推动他变化的还是要收到 action
	HeartbeatChan chan *HeartBeatResponseMessage

	SyncPointInfo *SyncPointInfo
}

func (d TableEventDispatcher) GetSink() sink.Sink {
	return d.Sink
}

func (d TableEventDispatcher) GetTableSpan() *TableSpan {
	return d.TableSpan
}

func (d TableEventDispatcher) GetState() *State {
	return d.State
}

func (d TableEventDispatcher) GetEventChan() chan *Event {
	return d.Ch
}

func (d TableEventDispatcher) GetResolvedTs() uint64 {
	return d.ResolvedTs
}

func (d TableEventDispatcher) GetId() uint64 {
	return d.Id
}

func (d TableEventDispatcher) GetDispatcherType() string {
	return "table_event_dispatcher"
}

func (d TableEventDispatcher) GetHeartBeatChan() chan *HeartBeatResponseMessage {
	return d.HeartbeatChan
}

func (d TableEventDispatcher) UpdateResolvedTs(ts uint64) {
	d.ResolvedTs = ts
}

func (d TableEventDispatcher) GetSyncPointInfo() *SyncPointInfo {
	return d.SyncPointInfo
}
