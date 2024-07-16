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
	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/eventpb"
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
	GetState() *State
	GetEventChan() chan *common.TxnEvent
	GetResolvedTs() uint64
	UpdateResolvedTs(uint64)
	GetId() common.DispatcherID
	GetDispatcherType() DispatcherType
	GetHeartBeatChan() chan *HeartBeatResponseMessage
	GetSyncPointInfo() *SyncPointInfo
	GetMemoryUsage() *MemoryUsage
	PushEvent(event *eventpb.TxnEvent)
	GetComponentStatus() heartbeatpb.ComponentState
}

type DispatcherType uint64

const (
	TableEventDispatcherType        DispatcherType = 0
	TableTriggerEventDispatcherType DispatcherType = 1
)

type Action uint64

const (
	None  Action = 0
	Write Action = 1
	Pass  Action = 2
)

/*
State displays the status of advancing event and State is used to indicate whether the current pendingEvent can be advanced.
Only the following events can be blocked:
1. All the DDL Events
2. All the Sync Point Events
3. The Event after the DDL Event or the Sync Point Event
*/
type State struct {
	// False means the events are being pushed down continuously
	// True means there is an event being blocked
	isBlocked bool
	// The blocked Event.
	// pendingEvent could be nil when isBlocked is true.
	// Such as one ddl event is sent to downstream,
	// so the following events can be pushed down only when
	// the ddl event is flushed to downstream successfully (that means sink is available).
	pengdingEvent *common.TxnEvent
	// The pendingEvent is waiting for the progress of these tableSpans to reach the blockTs.
	blockTableSpan []*common.TableSpan
	// the commitTs of the pendingEvent, also the ts the tableSpan in the blockTableSpan should reach.
	blockTs uint64
	// True means the sink flushes all the previous event successfully,
	// there is no event of these tableSpan in the Sink now.
	sinkAvailable bool
	// The action for the pendingEvent,
	// it is used to decide whether the pendingEvent should write or just pass.
	action Action //
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
	CheckpointTs    uint64
	Id              common.DispatcherID
	TableSpan       *common.TableSpan
	ComponentStatus heartbeatpb.ComponentState
}

func CollectDispatcherHeartBeatInfo(d Dispatcher) *HeartBeatInfo {
	var checkpointTs uint64
	// The event in dispatcher could be in
	// 1. Sink
	// 2. State.pengingEvent
	// 3. dispatcher.Ch
	// If there exists an event in the dispatcher, the checkpointTs should be the event with the smallest commitTs - 1
	// If there is no event in the dispatcher now, the checkpointTs should be the resolvedTs of the dispatcher.
	smallestCommitTsInSink := d.GetSink().GetSmallestCommitTs(d.GetTableSpan())
	if smallestCommitTsInSink == 0 {
		state := d.GetState()
		if state.pengdingEvent != nil {
			checkpointTs = state.pengdingEvent.CommitTs - 1
		} else {
			select {
			case event := <-d.GetEventChan():
				if event.IsDMLEvent() {
					state.pengdingEvent = event
				} else {
					// TODO:先 hack 了 blockTableSpan 的值获取
					state = &State{
						isBlocked:     true,
						pengdingEvent: event,
						// blockTableSpan: event.GetTableSpans(),
						blockTs:       event.CommitTs,
						action:        None,
						sinkAvailable: false,
					}
				}
				checkpointTs = event.CommitTs - 1
			default:
				checkpointTs = d.GetResolvedTs()
			}
		}
	} else {
		checkpointTs = smallestCommitTsInSink - 1
	}

	// use checkpointTs to release memory usage
	d.GetMemoryUsage().Release(checkpointTs)

	//state := d.GetState()
	// return &HeartBeatInfo{
	// 	// IsBlocked:      state.isBlocked,
	// 	// BlockTs:        state.blockTs,
	// 	// BlockTableSpan: state.blockTableSpan,
	// 	CheckpointTs: checkpointTs,
	// 	//TableSpan:    d.GetTableSpan(),
	// 	Id: d.GetId(),
	// }
	return &HeartBeatInfo{
		ComponentStatus: d.GetComponentStatus(),
		CheckpointTs:    checkpointTs,
		TableSpan:       d.GetTableSpan(),
		Id:              d.GetId(),
	}
}

/*
TableSpanProgress shows the progress of the other tableSpan, including:
1. Whether the tableSpan is blocked, and the ts of blocked event
2. The checkpointTs of the tableSpan
*/
type TableSpanProgress struct {
	Span         *common.TableSpan
	IsBlocked    bool
	BlockTs      uint64
	CheckpointTs uint64
}

/*
HeartBeatReponseMessage includes the message from the HeartBeatResponse, including:
1. The action for the blocked event
2. the progress of other tableSpan, which the dispatcher is waiting for.
*/
type HeartBeatResponseMessage struct { // 最好需要一个对应，对应 blocked by 什么 event 的 信号，避免出现乱序的问题
	Action             Action
	OtherTableProgress []*TableSpanProgress
}
