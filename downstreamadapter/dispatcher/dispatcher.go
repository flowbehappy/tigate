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

	"github.com/ngaut/log"
)

type State struct {
	// true 表示 event list 要推动需要有条件满足
	// 具体的条件就是满足 sinkAvaible， blockTs 以及 blockTableSpan
	isBlocked      bool
	pengdingEvent  *Event       // 被block到的 event，也可以是空，说明是 ddl 下推 block 了后续的 event
	blockTableSpan []*TableSpan // 需要等达到 ts 的 table span
	blockTs        uint64       // 需要等到其他 table 的 ts
	sinkAvailable  bool         // true 表示 sink 里已经没有没有 flush 下去的 event
}

func (s *State) clear() {
	s.isBlocked = false
	s.pengdingEvent = nil
	s.blockTableSpan = nil
	s.blockTs = 0
	s.sinkAvailable = false
}

type TableEventDispatcher struct {
	ch        <-chan *Event
	tableSpan *Span
	sink      *sink.Sink
	//waitingDDL bool // true 表示有 ddl event 或者 sync point event 下推到 sink 中了，所以需要等 ddl 彻底落盘，后面的 event 才能往下推
	state *State

	otherTableStates map[*tableSpan]*State
	needFlush        bool // 这个后面要跟 otherTableStates 放到一个结构里面，这个用来标志当前 dispatcher 在达到状态后是否需要 flush 这条 ddl，还是直接丢弃就可以了
}

// one dispatcher only corresponds to one event dispatcher task
// the task will create when the dispatcher creates
// and finish when the dispatcher is closed
type EventDispatcherTask struct {
	dispatcher *TableEventDispatcher
}

func (t *EventDispatcherTask) execute(timeout time.Duration) threadpool.TaskStatus {
	timer := time.NewTimer(timeout)
	for {
		// 这边逻辑写太复杂了，后面把他拆成几个 函数出来
		state := t.dispatcher.state
		if state.isBlocked {
			// 不能直接下推
			// 先检查 sink 是否 available
			if state.sinkAvailable {
				if !(*t.dispatcher.sink).IsEmpty(t.dispatcher.tableSpan) {
					return threadpool.Running // TODO:这个逻辑应该在 await,后面再改
				}
				// 就可以把这个状态改回去了
				state.sinkAvailable = false
			}

			// 检查 block 的 table 的 ts
			if state.blockTableSpan != nil {
				// 检查每一个 span
				for _, tableSpan := range state.blockTableSpan {
					if t.dispatcher.otherTableStates[tableSpan].blockTs >= state.blockTs {
						// 达到条件,删除
						delete(state.blockTableSpan, tableSpan)
					}
				}
			}

			if state.blockTableSpan != nil {
				return threadpool.Running
			}

			// 说明 block table ts 条件也达到了

			// 可以下推了
			if state.pengdingEvent != nil {
				if state.pengdingEvent.isDDLEvent() {
					// pending 的是 DDL，那还要决定是否是他要下推
					if t.dispatcher.needFlush {
						// 自己负责下推
						(*t.dispatcher.sink).AddDDLAndSyncPointEvent(t.dispatcher.tableSpan, state.pengdingEvent)
						t.dispatcher.state = &State{
							isBlocked:     true,
							sinkAvailable: true,
						}
						return threadpool.Running
					}
					// 不用下推那就可以去推后面的 event 了
				} else if state.pengdingEvent.isSyncPointEvent() {
					if t.dispatcher.needFlush {
						(*t.dispatcher.sink).AddDDLAndSyncPointEvent(t.dispatcher.tableSpan, state.pengdingEvent)
						t.dispatcher.state = &State{ // 后面最好不要用这种写法，直接原地改
							isBlocked:     true,
							sinkAvailable: true,
						}
						return threadpool.Running
					}
					// 不用下推那就可以去推后面的 event 了
				} else {
					// dml event
					(*t.dispatcher.sink).AddDMLEvent(t.dispatcher.tableSpan, state.pengdingEvent)
				}
				t.dispatcher.state.clear()
			} else {
				// 是 nil 就说明是前面推了 ddl 或者 sync point 下去，那直接 clear state 就可以
				t.dispatcher.state.clear()
			}
		}

		select {
		case event := <-t.dispatcher.ch:
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
						sinkAvailable:  true,
					}
					// 也更新 dispatcher other table state -- 因为是个单线程操作， 所以不用上锁，而且等待的也是唯一的
					for _, tableSpan := range tableSpans {
						t.dispatcher.otherTableStates[tableSpan] = &State{}
					}
					return threadpool.Running
				} else {
					if (*t.dispatcher.sink).IsEmpty(t.dispatcher.tableSpan) {
						(*t.dispatcher.sink).AddDDLAndSyncPointEvent(t.dispatcher.tableSpan, event)
						t.dispatcher.state = &State{
							isBlocked:     true,
							sinkAvailable: true,
						}
					} else {
						t.dispatcher.state = &State{
							isBlocked:     true,
							pengdingEvent: event,
							sinkAvailable: true,
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
					sinkAvailable:  true,
				}
				t.dispatcher.otherTableStates[AllTables] = &State{}
				return threadpool.Running
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
	log.Error("Event Dispatcher Task should not call await")
	return threadpool.Failed
}

func (t *EventDispatcherTask) release() {

}
