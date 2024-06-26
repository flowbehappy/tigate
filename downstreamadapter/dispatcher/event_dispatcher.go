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

type Action uint64

const (
	None  Action = 0
	Write Action = 1
	Pass  Action = 2
)

type State struct {
	// true 表示 event list 要推动需要有条件满足
	// 具体的条件就是满足 sinkAvaible， blockTs 以及 blockTableSpan
	isBlocked      bool
	pengdingEvent  *Event       // 被block到的 event，也可以是空，说明是 ddl 下推 block 了后续的 event
	blockTableSpan []*TableSpan // 需要等达到 ts 的 table span
	blockTs        uint64       // 需要等到其他 table 的 ts
	sinkAvailable  bool         // true 表示 sink 里已经没有没有 flush 下去的 event
	action         Action       //
}

func NewState() *State {
	return &State{
		isBlocked:      false,
		pengdingEvent:  nil,
		blockTableSpan: nil,
		blockTs:        0,
		sinkAvailable:  false,
		action:         None,
	}
}

func (s *State) clear() {
	s.isBlocked = false
	s.pengdingEvent = nil
	s.blockTableSpan = nil
	s.blockTs = 0
	s.sinkAvailable = false
	s.action = None
}

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

type HeartBeatInfo struct {
	IsBlocked      bool
	BlockTs        uint64
	BlockTableSpan []*TableSpan
	CheckpointTs   uint64
}

func (e *TableEventDispatcher) CollectHeartbeatInfo() *HeartBeatInfo {
	// dispatcher 中 event 存在两个地方，一个是 sink 里，一个是 pendingEvent，一个是 ch 里
	var checkpointTs uint64
	smallestCommitTsInSink := (*e.sink).GetSmallestCommitTs()
	if smallestCommitTsInSink == 0 {
		// 说明 sink 里没有数据了
		if e.State.pengdingEvent != nil {
			checkpointTs = e.state.pengdingEvent.CommitTs - 1
		} else {
			select {
			case event <- e.Ch:
				e.state.pengdingEvent = event
				checkpointTs = e.state.pengdingEvent.CommitTs - 1
			default:
				// 毫无 event，用 resolvedTs
				checkpointTs = e.ResolvedTs
			}
		}
	} else {
		checkpointTs = smallestCommitTsInSink - 1
	}
	return &HeartBeatInfo{
		IsBlocked:      e.state.isBlocked,
		BlockTs:        e.state.blockTs,
		BlockTableSpan: e.state.blockTableSpan,
		CheckpointTs:   checkpointTs,
	}
}
