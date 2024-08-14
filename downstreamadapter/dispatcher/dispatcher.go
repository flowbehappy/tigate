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
