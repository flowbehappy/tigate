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

import "new_arch/downstreamadapter/sink"

type Dispatcher interface {
	GetSink() sink.Sink
	GetTableSpan() *Span
	GetState() *State
	GetEventChan() chan *Event
	GetResolvedTs() uint64
	GetId() uint64
}

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

type HeartBeatInfo struct {
	IsBlocked      bool
	BlockTs        uint64
	BlockTableSpan []*TableSpan
	CheckpointTs   uint64
	TableSpan      *TableSpan
	Id             uint64
}

func CollectDispatcherHeartBeatInfo(d Dispatcher) *HeartBeatInfo {
	var checkpointTs uint64
	smallestCommitTsInSink := d.GetSink().GetSmallestCommitTs(d.GetTableSpan())
	if smallestCommitTsInSink == 0 {
		state := d.GetState()
		// 说明 sink 里没有数据了
		if state.pengdingEvent != nil {
			checkpointTs = state.pengdingEvent.CommitTs - 1
		} else {
			select {
			case event := <-d.GetEventChan():
				if event.IsDMLEvent() {
					state.pengdingEvent = event
				} else {
					state = &State{
						isBlocked:      true,
						pengdingEvent:  event,
						blockTableSpan: event.GetTableSpans(),
						blockTs:        event.CommitTs(),
						action:         None,
						sinkAvailable:  false,
					}
				}

				checkpointTs = event.CommitTs - 1
			default:
				// 毫无 event，用 resolvedTs
				checkpointTs = d.GetResolvedTs()
			}
		}
	} else {
		checkpointTs = smallestCommitTsInSink - 1
	}
	state := d.GetState()
	return &HeartBeatInfo{
		IsBlocked:      state.isBlocked,
		BlockTs:        state.blockTs,
		BlockTableSpan: state.blockTableSpan,
		CheckpointTs:   checkpointTs,
		TableSpan:      d.GetTableSpan(),
		Id:             d.GetID(),
	}
}
