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
	"github.com/flowbehappy/tigate/utils/threadpool"
)

/*
One dispatcher corresponds to an event dispatcher task.
The task will create when the dispatcher creates and finish when the dispatcher is closed.

EventDispatcherTask is responsible for determining whether the current event can be pushed down to the sink,
and pushes the event down to the sink if it is eligible.
*/
type EventDispatcherTask struct {
	dispatcher Dispatcher
	//infos      []*HeartBeatResponseMessage
	taskStatus threadpool.TaskStatus
}

func NewEventDispatcherTask(dispatcher Dispatcher) *EventDispatcherTask {
	return &EventDispatcherTask{
		dispatcher: dispatcher,
		//infos:      make([]*HeartBeatResponseMessage, 0),
		taskStatus: threadpool.Running,
	}
}

func (t *EventDispatcherTask) GetStatus() threadpool.TaskStatus {
	return t.taskStatus
}

func (t *EventDispatcherTask) SetStatus(taskStatus threadpool.TaskStatus) {
	t.taskStatus = taskStatus
}

func (t *EventDispatcherTask) updateState(state *State, heartBeatResponseMessages []*HeartBeatResponseMessage) {
	for _, heartBeatResponseMessage := range heartBeatResponseMessages {
		otherTableProgress := heartBeatResponseMessage.OtherTableProgress
		spanToProgressMap := make(map[*common.TableSpan]*TableSpanProgress)
		for _, progress := range otherTableProgress {
			span := progress.Span
			spanToProgressMap[span] = progress
		}

		newBlockTableSpan := make([]*common.TableSpan, 0)
		for _, span := range state.blockTableSpan {
			remove := false
			if progress, ok := spanToProgressMap[span]; ok {
				if progress.IsBlocked {
					if progress.BlockTs >= state.blockTs {
						remove = true
					}
				} else {
					if progress.CheckpointTs >= state.blockTs {
						remove = true
					}
				}
			}
			if !remove {
				newBlockTableSpan = append(newBlockTableSpan, span)
			}
		}
		state.blockTableSpan = newBlockTableSpan

		action := heartBeatResponseMessage.Action
		// 这边理论上要保证 action 复写没有问题
		if state.action != action {
			state.action = action
		}
	}

}

// TODO:这边后面需要列一下每一种情况
func (t *EventDispatcherTask) Execute(timeout time.Duration) threadpool.TaskStatus {
	timer := time.NewTimer(timeout)

	// 1. 先检查是否在 blocked 状态
	state := t.dispatcher.GetState()
	tableSpan := t.dispatcher.GetTableSpan()
	//dispatcherType := t.dispatcher.GetDispatcherType()
	sink := t.dispatcher.GetSink()

	/*
		if state.isBlocked {
			// 拿着 infos 更新
			if len(t.infos) > 0 {
				t.updateState(state, t.infos)
			}
			t.infos = t.infos[:0] //上锁

			if !state.sinkAvailable { // 没达到 ture 的时候都要做检查
				if sink.IsEmpty(tableSpan) {
					state.sinkAvailable = true
				}
			}

			if state.blockTableSpan != nil || (state.pengdingEvent != nil && state.action == None) {
				return threadpool.Waiting
			}

			if state.sinkAvailable == false { // 只有这个不满足的话就在这轮不用去 wait
				return threadpool.Running
			}
		}
	*/

	// 先检查 pendingEvent 有没有，有的话就先执行这个，通过 action 来确定 ddl / syncPoint 的执行模式

	if state.pengdingEvent != nil {
		/*
			if state.pengdingEvent.IsDDLEvent() || state.pengdingEvent.IsSyncPointEvent() {
				if state.action == Pass {
					// 扔掉这条，开始写后续的 event
				} else if state.action == Write {
					// 下推
					sink.AddDDLAndSyncPointEvent(tableSpan, state.pengdingEvent)
					state.clear()

					if dispatcherType == TableTriggerEventDispatcherType {
						// 如果是 table trigger, 那就 进入后续处理，尝试拿取下一条 event 开始处理
					} else if dispatcherType == TableEventDispatcherType {
						// state 改为 block ，等到下一轮发现 sink available 以后重新开始拿数据往下同步
						state = &State{
							isBlocked: true,
						}
						return threadpool.Running
					}
				}
			} else {
				// DML
				// 直接下推
				sink.AddDMLEvent(tableSpan, state.pengdingEvent)
				state.clear()
			}*/
		sink.AddDMLEvent(tableSpan, state.pengdingEvent)
		state.clear()
	}

	/* 还能继续写
	   1. pending 的是 ddl /syncpoint,但是 pass）
	   2. pending 的是 dml，继续写
	   3. 没有 pending, 本身就在等前面 ddl / syncpoint 写完而已
	*/
	for {
		select {
		case event := <-t.dispatcher.GetEventChan():
			if event.IsDMLEvent() {
				sink.AddDMLEvent(tableSpan, event)
			}
			/*
				else if event.IsDDLEvent() {
					// DDL
					if event.IsCrossTableDDL() { // cross 这个到时候还要加入 table trigger 会涉及到的，也算 cross. 也就是 table trigger 只能走到这个里面
						// TODO: 先不跑除了 create table 以外的 DDL，所以不会有 crossTable 出现
						// tableSpans := event.GetTableSpans() // 获得这个 ddl 中涉及到的其他 table 的 span，这边后面写的时候也要考虑一下 table trigger 这个特殊的啊
						// state = &State{
						// 	isBlocked:      true,
						// 	pengdingEvent:  event,
						// 	blockTableSpan: tableSpans,
						// 	blockTs:        event.CommitTs,
						// }
						return threadpool.Waiting
					} else {
						if sink.IsEmpty(tableSpan) {
							sink.AddDDLAndSyncPointEvent(tableSpan, event)
							state = &State{
								isBlocked: true,
							}
						} else {
							state = &State{
								isBlocked:     true,
								pengdingEvent: event,
							}
						}
						return threadpool.Running
					}
				} else {
					// syncpoint event, TODO:要处理一下只有一张表的情况
					state = &State{
						isBlocked:     true,
						pengdingEvent: event,
						// blockTableSpan: AllTables, // special span : TODO syncpointTs
						blockTs: event.CommitTs,
					}
					return threadpool.Waiting
				}
			*/
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
func (t *EventDispatcherTask) Await() threadpool.TaskStatus {
	/*
		select {
		case info := <-t.dispatcher.GetHeartBeatChan():
			// 把目前现有的全部拿出来
			t.infos = append(t.infos, info)
			for {
				select {
				case info := <-t.dispatcher.GetHeartBeatChan():
					t.infos = append(t.infos, info)
				default:
					return threadpool.Running
				}
			}
		default:
			// 如果不是在等待其他 table / action 状态而是 sink 本身的话，就也切回去
			state := t.dispatcher.GetState()
			if state.blockTableSpan == nil && (state.pengdingEvent == nil || state.action != None) && state.sinkAvailable == false {
				return threadpool.Running
			}
			return threadpool.Waiting
		}
	*/
	return threadpool.Failed
}

func (t *EventDispatcherTask) Release() {

}
