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

	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
)

/*
Dispatcher is responsible for getting events from LogService and sending them to Sink in appropriate order.
Each dispatcher only deal with the events of one tableSpan in one changefeed.
Each dispatcher corresponds to an event dispatcher task, working for the core work of the dispatcher.
All dispatchers in the changefeed of the same node will share the same Sink.
All dispatchers will communicate with the Maintainer about self progress and whether can push down the blocked event.

Because Sink does not flush events to the downstream in strict order.
the dispatcher can't send event to Sink continuously all the time,
1. The ddl event/sync point event can be send to Sink only when the previous event has beed flushed to downstream successfully.
2. Only when the ddl event/sync point event is flushed to downstream successfully, the dispatcher can send the following event to Sink.
3. For the cross table ddl event/sync point event, dispatcher needs to negotiate with the maintainer to decide whether and when send it to Sink.

The workflow related to the dispatcher is as follows:

		+------------+       +----------------+       +------------+       +--------+       +--------+       +------------+
		| LogService |  -->  | EventCollector |  -->  | Dispatcher |  -->  |  Sink  |  -->  | Worker |  -->  | Downstream |
		+------------+       +----------------+       +------------+       +--------+       +--------+       +------------+
	                                                        |
										  HeartBeatResponse | HeartBeatRequest
															|
	                                              +--------------------+
	                                              | HeartBeatCollector |
												  +--------------------+
												            |
															|
												      +------------+
	                                                  | Maintainer |
												      +------------+
*/
type Dispatcher interface {
	GetSink() sink.Sink
	GetTableSpan() *common.TableSpan
	GetEventChan() chan *common.TxnEvent
	GetResolvedTs() uint64
	UpdateResolvedTs(uint64)
	GetCheckpointTs() uint64
	GetId() string
	GetDispatcherType() DispatcherType
	GetDDLActions() chan *heartbeatpb.DispatcherAction
	//GetSyncPointInfo() *SyncPointInfo
	//GetMemoryUsage() *MemoryUsage
	// PushEvent(event *eventpb.TxnEvent)
	PushTxnEvent(event *common.TxnEvent)
	GetComponentStatus() heartbeatpb.ComponentState
	GetTableSpanStatusesChan() chan *heartbeatpb.TableSpanStatus

	TryClose() (w heartbeatpb.Watermark, ok bool)
}

type DispatcherType uint64

const (
	TableEventDispatcherType        DispatcherType = 0
	TableTriggerEventDispatcherType DispatcherType = 1
)

/*
HeartBeatInfo is used to collect the message for HeartBeatRequest for each dispatcher.
Mainly about the progress of each dispatcher:
1. whether the dispatcher is blocked ? If blocked, the info about blocked should be collected.
2. The checkpointTs of the dispatcher, shows that all the events whose ts <= checkpointTs are flushed to downstream successfully.
*/
type HeartBeatInfo struct {
	// IsBlocked      bool
	// BlockTs        uint64
	// BlockTableSpan []*common.TableSpan
	// TableSpan      *common.TableSpan
	heartbeatpb.Watermark
	Id              string
	TableSpan       *common.TableSpan
	ComponentStatus heartbeatpb.ComponentState
}

//  1. 如果是单表内的 ddl，达到下推的条件为： sink 中没有还没执行完的当前表的 event
//  2. 如果是多表内的 ddl 或者是表间的 ddl，则需要满足的条件为：
//     2.1 sink 中没有还没执行完的当前表的 event
//     2.2 maintainer 通知自己可以 write 或者 pass event
func AddDDLEventToSinkWhenAvailable(d Dispatcher, event *common.TxnEvent) {
	sink := d.GetSink()
	tableSpan := d.GetTableSpan()
	if event.IsSingleTableDDL() {
		if sink.IsEmpty(tableSpan) {
			sink.AddDMLEvent(tableSpan, event)
			return
		} else {
			// TODO:先写一个 定时 check 的逻辑，后面用 dynamic stream 改造
			timer := time.NewTimer(time.Millisecond * 50)
			for {
				select {
				case <-timer.C:
					if sink.IsEmpty(tableSpan) {
						sink.AddDMLEvent(tableSpan, event)
						return
					}
				}
			}
		}
	}

	d.GetTableSpanStatusesChan() <- &heartbeatpb.TableSpanStatus{
		Span:            tableSpan.TableSpan,
		ComponentStatus: heartbeatpb.ComponentState_Working,
		State: &heartbeatpb.State{
			IsBlocked:            true,
			BlockTs:              event.CommitTs,
			BlockTableSpan:       event.GetBlockedTableSpan(), // 这个包含自己的 span 是不是也无所谓，不然就要剔除掉
			NeedDroppedTableSpan: event.GetNeedDroppedTableSpan(),
			NeedAddedTableSpan:   event.GetNeedAddedTableSpan(),
		},
	}

	for {
		dispatcherAction := <-d.GetDDLActions()
		if dispatcherAction.CommitTs == event.CommitTs {
			if dispatcherAction.Action == heartbeatpb.Action_Write {
				sink.AddDDLAndSyncPointEvent(tableSpan, event) // 这个是同步写，所以写完的时候 sink 也 available 了
				// 写完马上通知 maintainer 推进 checkpointTs
				d.GetTableSpanStatusesChan() <- &heartbeatpb.TableSpanStatus{
					Span:            tableSpan.TableSpan,
					ComponentStatus: heartbeatpb.ComponentState_Working,
					CheckpointTs:    d.GetCheckpointTs(),
				}
			}
			return
		}
	}
}

func CollectDispatcherHeartBeatInfo(d Dispatcher, h *HeartBeatInfo) {

	// use checkpointTs to release memory usage
	//d.GetMemoryUsage().Release(checkpointTs)

	//state := d.GetState()
	// return &HeartBeatInfo{
	// 	// IsBlocked:      state.isBlocked,
	// 	// BlockTs:        state.blockTs,
	// 	// BlockTableSpan: state.blockTableSpan,
	// 	CheckpointTs: checkpointTs,
	// 	//TableSpan:    d.GetTableSpan(),
	// 	Id: d.GetId(),
	// }
	h.Watermark.CheckpointTs = d.GetCheckpointTs()
	h.Watermark.ResolvedTs = d.GetResolvedTs()
	h.Id = d.GetId()
	h.ComponentStatus = d.GetComponentStatus()
	h.TableSpan = d.GetTableSpan()
}
