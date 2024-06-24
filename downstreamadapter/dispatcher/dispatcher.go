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
	"new_arch/utils/threadpool"
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

func (s *State) clear() {
	s.isBlocked = false
	s.pengdingEvent = nil
	s.blockTableSpan = nil
	s.blockTs = 0
	s.sinkAvailable = false
	s.action = None
}

type TableSpanProgress struct {
	span         *TableSpan
	isBlocked    bool
	blockTs      uint64
	checkpointTs uint64
}

type HeartBeatResponseMessage struct { // 最好需要一个对应，对应 blocked by 什么 event 的 信号，避免出现乱序的问题
	action             Action
	otherTableProgress []*TableSpanProgress
}

type TableEventDispatcher struct {
	Ch        <-chan *Event // 转换成一个函数
	tableSpan *Span
	sink      *sink.Sink

	state      *State
	ResolvedTs uint64

	// 搞个 channel 来接收 heartbeat 产生的 信息，然后下推数据这个就可以做成 await 了
	// heartbeat 会更新依赖的 tableSpan 的 状态，然后满足了就删掉，下次发送就不用发了，但最终推动他变化的还是要收到 action
	heartbeatChan <-chan *HeartBeatResponseMessage
}

func newTableEventDispatcher() *TableEventDispatcher {
	// 创建新的 event dispatcher，同时需要把这个去 logService 注册，并且把自己加到对应的某个处理 thread 里
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
		if e.state.pengdingEvent != nil {
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

// one dispatcher only corresponds to one event dispatcher task
// the task will create when the dispatcher creates
// and finish when the dispatcher is closed
type EventDispatcherTask struct {
	dispatcher *TableEventDispatcher
	infos      []*HeartBeatResponseMessage
}

func (t *EventDispatcherTask) updateState(state *State, heartBeatResponseMessages []*HeartBeatResponseMessage) {
	for _, heartBeatResponseMessage := range heartBeatResponseMessages {
		otherTableProgress := heartBeatResponseMessage.otherTableProgress
		spanToProgressMap := make(map[*TableSpan]*TableSpanProgress)
		for _, progress := range otherTableProgress {
			span := progress.span
			spanToProgressMap[span] = progress
		}

		for _, span := range state.blockTableSpan {
			if progress, ok := spanToProgressMap[span]; ok {
				if progress.isBlocked {
					if progress.blockTs >= state.blockTs {
						delete(state.blockTableSpan, span)
					}
				} else {
					if progress.checkpointTs >= state.blockTs {
						delete(state.blockTableSpan, span)
					}
				}
			}
		}
		action := heartBeatResponseMessage.action
		// 这边理论上要保证 action 复写没有问题
		if state.action != action {
			state.action = action
		}
	}

	if !state.sinkAvailable { // 没达到 ture 的时候都要做检查
		if (*t.dispatcher.sink).IsEmpty(t.dispatcher.tableSpan) {
			state.sinkAvailable = true
		}
	}

}

// TODO:逻辑重新整一下，太乱了
func (t *EventDispatcherTask) execute(timeout time.Duration) threadpool.TaskStatus {
	timer := time.NewTimer(timeout)

	// 1. 先检查是否在 blocked 状态
	state := t.dispatcher.state
	if state.isBlocked {
		// 拿着 infos 更新
		if len(t.infos) > 0 {
			updateState(state, t.infos)
		}
		t.infos = t.infos[:0] //上锁

		if state.blockTableSpan != nil || state.action == None {
			return threadpool.Waiting
		}

		if state.sinkAvailable == false { // 只有这个不满足的话就在这轮不用去 wait
			return threadpool.Running
		}

		// 都达到的话就改成 isBlock = true
		state.isBlocked = true
	}

	// 先检查 pendingEvent 有没有，有的话就先执行这个，通过 action 来确定 ddl / syncPoint 的执行模式

	if state.pengdingEvent != nil {
		if state.pengdingEvent.isDDLEvent() || state.pengdingEvent.isSyncPointEvent() {
			if state.action == Pass {
				// 扔掉这条，开始写后续的 event
			} else if state.action == Write {
				// 下推
				(*t.dispatcher.sink).AddDDLAndSyncPointEvent(t.dispatcher.tableSpan, state.pengdingEvent)
				t.dispatcher.state.clear()
				t.dispatcher.state = &State{
					isBlocked: true,
				}
				return threadpool.Running
			}
		} else {
			// DML
			// 直接下推
			(*t.dispatcher.sink).AddDMLEvent(t.dispatcher.tableSpan, state.pengdingEvent)
			t.dispatcher.state.clear()
		}
	}

	/* 还能继续写
	   1. pending 的是 ddl /syncpoint,但是 pass）
	   2. pending 的是 dml，继续写
	   3. 没有 pending, 本身就在等前面 ddl / syncpoint 写完而已
	*/
	for {
		select {
		case event := <-t.dispatcher.Ch:
			if event.isDMLEvent() {
				(*t.dispatcher.sink).AddDMLEvent(t.dispatcher.tableSpan, event)
			} else if event.isDDLEvent() {
				// DDL
				if event.isCrossTableDDL() {
					tableSpans := event.getOtherTableSpans() // 获得这个 ddl 中涉及到的其他 table 的 span
					t.dispatcher.state = &State{
						isBlocked:      true,
						pengdingEvent:  event,
						blockTableSpan: tableSpans,
						blockTs:        event.CommitTs(),
					}
					return threadpool.Waiting
				} else {
					if (*t.dispatcher.sink).IsEmpty(t.dispatcher.tableSpan) {
						(*t.dispatcher.sink).AddDDLAndSyncPointEvent(t.dispatcher.tableSpan, event)
						t.dispatcher.state = &State{
							isBlocked: true,
						}
					} else {
						t.dispatcher.state = &State{
							isBlocked:     true,
							pengdingEvent: event,
						}
					}
					return threadpool.Running
				}
			} else {
				// syncpoint event, TODO:要处理一下只有一张表的情况
				t.dispatcher.state = &State{
					isBlocked:      true,
					pengdingEvent:  event,
					blockTableSpan: AllTables, // special span
					blockTs:        event.CommitTs(),
				}
				return threadpool.Waiting
			}
		case <-timer.C:
			return threadpool.Running
		default:
			if !timer.Stop() {
				<-timer.C
			}
			return threadpool.Running
		}
	}
}
func (t *EventDispatcherTask) await(timeout time.Duration) threadpool.TaskStatus {
	select {
	case info := <-t.dispatcher.heartbeatChan:
		// 把目前现有的全部拿出来
		t.infos = append(t.infos, info)
		for {
			select {
			case info := <-t.dispatcher.heartbeatChan:
				t.infos = append(t.infos, info)
			default:
				return threadpool.Running
			}
		}
	default:
		// 如果不是在等待其他 table 状态而是 sink 本身的话，就也切回去
		if t.dispatcher.state.blockTableSpan == nil && t.dispatcher.state.sinkAvailable == false {
			return threadpool.Running
		}
	}
}

func (t *EventDispatcherTask) release() {

}
