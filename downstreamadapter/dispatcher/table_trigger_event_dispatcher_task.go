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
	"new_arch/utils/threadpool"
	"time"
)

// 后面把这个跟 event dispatcher task 抽出来合并一下

// start when table trigger event dispatcher creates
type TableTriggerEventDispatcherTask struct {
	dispatcher *TableTriggerEventDispatcher
	infos      []*HeartBeatResponseMessage
	taskStatus threadpool.TaskStatus
}

func NewTableTriggerEventDispatcherTask(dispatcher *TableTriggerEventDispatcher) *TableTriggerEventDispatcherTask {
	return &TableTriggerEventDispatcherTask{
		dispatcher: dispatcher,
		infos:      make([]*HeartBeatResponseMessage, 0),
		taskStatus: threadpool.Running,
	}
}

func (t *TableTriggerEventDispatcherTask) GetStatus() threadpool.TaskStatus {
	return t.taskStatus
}

func (t *TableTriggerEventDispatcherTask) updateState(state *State, heartBeatResponseMessages []*HeartBeatResponseMessage) {
	for _, heartBeatResponseMessage := range heartBeatResponseMessages {
		tableProgress := heartBeatResponseMessage.OtherTableProgress
		spanToProgressMap := make(map[*TableSpan]*TableSpanProgress)
		for _, progress := range tableProgress {
			span := progress.Span
			spanToProgressMap[span] = progress
		}

		for _, span := range state.blockTableSpan {
			if progress, ok := spanToProgressMap[span]; ok {
				if progress.IsBlocked {
					if progress.BlockTs >= state.blockTs {
						delete(state.blockTableSpan, span)
					}
				} else {
					if progress.CheckpointTs >= state.blockTs {
						delete(state.blockTableSpan, span)
					}
				}
			}
		}
		action := heartBeatResponseMessage.Action
		// 这边理论上要保证 action 复写没有问题
		if state.action != action {
			state.action = action
		}
	}

	if !state.sinkAvailable { // 没达到 ture 的时候都要做检查
		if t.dispatcher.Sink.IsEmpty(t.dispatcher.TableSpan) {
			state.sinkAvailable = true
		}
	}
}

func (t *TableTriggerEventDispatcherTask) Execute(timeout time.Duration) threadpool.TaskStatus {
	state := t.dispatcher.State
	if state.isBlocked {
		if len(t.infos) > 0 {
			t.updateState(state, t.infos)
		}
		t.infos = t.infos[:0] //上锁

		if state.blockTableSpan != nil || state.action == None {
			return threadpool.Waiting
		}

		if state.sinkAvailable == false { // 只有这个不满足的话就在这轮不用去 wait
			return threadpool.Running
		}
	}

	if state.pengdingEvent != nil {
		if state.action == Pass {
			// 不用写这条 ddl 了
			state.clear()
			// 获取下一条 ddl
			select {
			case event := <-t.dispatcher.Ch:
				state = &State{
					isBlocked:      true,
					pengdingEvent:  event,
					blockTableSpan: event.GetTableSpans(),
					blockTs:        event.CommitTs(),
					action:         None,
					sinkAvailable:  false,
				}
				return threadpool.Waiting
			default:
				// 目前没有 ddl,task 扔回去
				return threadpool.Running
			}
		} else if state.action == Write {
			t.dispatcher.Sink.AddDDLAndSyncPointEvent(t.dispatcher.TableSpan, state.pengdingEvent)
			state.clear()
			// 获取下一条 ddl
			select {
			case event := <-t.dispatcher.Ch:
				state = &State{
					isBlocked:      true,
					pengdingEvent:  event,
					blockTableSpan: event.GetTableSpans(),
					blockTs:        event.CommitTs(),
					action:         None,
					sinkAvailable:  false,
				}
				return threadpool.Waiting
			default:
				// 目前没有 ddl,task 扔回去
				return threadpool.Running
			}
		}
	}
	// 重新拿 ddl 同步
	select {
	case event := <-t.dispatcher.Ch:
		state = &State{
			isBlocked:      true,
			pengdingEvent:  event,
			blockTableSpan: event.GetTableSpans(),
			blockTs:        event.CommitTs(),
			action:         None,
			sinkAvailable:  false,
		}
		return threadpool.Waiting
	default:
		// 目前没有 ddl,task 扔回去
		return threadpool.Running
	}
}

func (t *TableTriggerEventDispatcherTask) Await() threadpool.TaskStatus {
	select {
	case info := <-t.dispatcher.HeartbeatChan:
		// 把目前现有的全部拿出来
		t.infos = append(t.infos, info)
		for {
			select {
			case info := <-t.dispatcher.HeartbeatChan:
				t.infos = append(t.infos, info)
			default:
				return threadpool.Running
			}
		}
	default:
		// 如果不是在等待其他 table / action 状态而是 sink 本身的话，就也切回去
		if t.dispatcher.State.blockTableSpan == nil && t.dispatcher.State.action != None && t.dispatcher.State.sinkAvailable == false {
			return threadpool.Running
		}
		return threadpool.Waiting
	}
}
func (t *TableTriggerEventDispatcherTask) Release() {

}
