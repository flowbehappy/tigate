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
	"context"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/tiflow/pkg/filter"
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

	+--------------+       +----------------+       +------------+       +--------+       +--------+       +------------+
	| EventService |  -->  | EventCollector |  -->  | Dispatcher |  -->  |  Sink  |  -->  | Worker |  -->  | Downstream |
	+--------------+       +----------------+       +------------+       +--------+       +--------+       +------------+
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
	GetACKs() chan *heartbeatpb.ACK
	//GetSyncPointInfo() *SyncPointInfo
	//GetMemoryUsage() *MemoryUsage
	// PushEvent(event *eventpb.TxnEvent)
	PushTxnEvent(event *common.TxnEvent)
	GetComponentStatus() heartbeatpb.ComponentState
	GetTableSpanStatusesChan() chan *heartbeatpb.TableSpanStatus

	TryClose() (w heartbeatpb.Watermark, ok bool)
	GetFilter() filter.Filter
	GetWG() *sync.WaitGroup
	GetDDLPendingEvent() *common.TxnEvent
	SetDDLPendingEvent(event *common.TxnEvent)
	GetDDLFinishCh() chan struct{}
	Remove()
}

type DispatcherType uint64

const (
	TableEventDispatcherType        DispatcherType = 0
	TableTriggerEventDispatcherType DispatcherType = 1
)

/*
HeartBeatInfo is used to collect the message for HeartBeatRequest for each dispatcher.
Mainly about the progress of each dispatcher:
1. The checkpointTs of the dispatcher, shows that all the events whose ts <= checkpointTs are flushed to downstream successfully.
*/
type HeartBeatInfo struct {
	heartbeatpb.Watermark
	Id              string
	TableSpan       *common.TableSpan
	ComponentStatus heartbeatpb.ComponentState
}

func HandleDDLActions(d Dispatcher, ctx context.Context) {
	defer d.GetWG().Done()
	sink := d.GetSink()
	tableSpan := d.GetTableSpan()
	for {
		select {
		case <-ctx.Done():
			return
		case dispatcherAction := <-d.GetDDLActions():
			event := d.GetDDLPendingEvent()
			if event == nil {
				// 只可能出现在 event 已经推进了，但是还重复收到了 action 消息的时候，则重发包含 checkpointTs 的心跳
				d.GetTableSpanStatusesChan() <- &heartbeatpb.TableSpanStatus{
					Span:            tableSpan.TableSpan,
					ComponentStatus: heartbeatpb.ComponentState_Working,
					CheckpointTs:    d.GetCheckpointTs(),
				}
				continue
			}
			if dispatcherAction.CommitTs == event.CommitTs {
				if dispatcherAction.Action == heartbeatpb.Action_Write {
					sink.AddDDLAndSyncPointEvent(tableSpan, event) // 这个是同步写，所以写完的时候 sink 也 available 了
				} else {
					sink.PassDDLAndSyncPointEvent(tableSpan, event) // 为了更新 tableProgress，避免 checkpointTs 计算的 corner case
				}
				d.GetTableSpanStatusesChan() <- &heartbeatpb.TableSpanStatus{
					Span:            tableSpan.TableSpan,
					ComponentStatus: heartbeatpb.ComponentState_Working,
					CheckpointTs:    d.GetCheckpointTs(),
				}
				d.GetDDLFinishCh() <- struct{}{}
				return
			}
		}
	}
}

//  1. 如果是单表内的 ddl，达到下推的条件为： sink 中没有还没执行完的当前表的 event
//  2. 如果是多表内的 ddl 或者是表间的 ddl，则需要满足的条件为：
//     2.1 sink 中没有还没执行完的当前表的 event
//     2.2 maintainer 通知自己可以 write 或者 pass event
//
// TODO:特殊处理有 add index 的逻辑
func AddDDLEventToSinkWhenAvailable(d Dispatcher, event *common.TxnEvent) {
	//filter := d.GetFilter()
	// TODO: filter 支持
	// 判断 ddl 是否需要处理，如果不需要处理，直接返回
	// if filter.ShouldIgnoreDDLEvent(event.GetDDLEvent()) {
	// 	return
	// }

	// 需要根据 filter 来判断 ddl.Query 中是否需要调整，只针对 query 中包含多个 sql 语句。所以 的ddl 传来的时候，需要对应传 sql 对应的 table id 信息，用于过滤

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

	d.SetDDLPendingEvent(event)

	// TODO:消息需要保证发送后收到 ack 才可以停止重发，具体重发时间需要调整
	message := &heartbeatpb.TableSpanStatus{
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
	d.GetTableSpanStatusesChan() <- message
	timer := time.NewTimer(time.Millisecond * 100)
loop:
	for {
		select {
		case <-timer.C:
			// 重发消息
			d.GetTableSpanStatusesChan() <- message
		case ack := <-d.GetACKs():
			if ack.CommitTs == event.CommitTs {
				break loop
			}
		}
	}

	// 收到 ack 以后可以开始等 actions 来进行处理,等待 finish 信号
	<-d.GetDDLFinishCh()
}

func CollectDispatcherHeartBeatInfo(d Dispatcher, h *HeartBeatInfo) {
	// use checkpointTs to release memory usage
	//d.GetMemoryUsage().Release(checkpointTs)

	h.Watermark.CheckpointTs = d.GetCheckpointTs()
	h.Watermark.ResolvedTs = d.GetResolvedTs()
	h.Id = d.GetId()
	h.ComponentStatus = d.GetComponentStatus()
	h.TableSpan = d.GetTableSpan()
}

type SyncPointInfo struct {
	EnableSyncPoint   bool
	SyncPointInterval time.Duration
	NextSyncPointTs   uint64
}

type ComponentStateWithMutex struct {
	mutex           sync.Mutex
	componentStatus heartbeatpb.ComponentState
}

func newComponentStateWithMutex(status heartbeatpb.ComponentState) *ComponentStateWithMutex {
	return &ComponentStateWithMutex{
		componentStatus: status,
	}
}

func (s *ComponentStateWithMutex) Set(status heartbeatpb.ComponentState) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.componentStatus = status
}

func (s *ComponentStateWithMutex) Get() heartbeatpb.ComponentState {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.componentStatus
}

type TsWithMutex struct {
	mutex sync.Mutex
	ts    uint64
}

func newTsWithMutex(ts uint64) *TsWithMutex {
	return &TsWithMutex{
		ts: ts,
	}
}

func (r *TsWithMutex) Set(ts uint64) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.ts = ts
}

func (r *TsWithMutex) Get() uint64 {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.ts
}
