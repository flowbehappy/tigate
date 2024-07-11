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

package dispatchermanager

import (
	"math"
	"net/url"
	"time"

	"github.com/flowbehappy/tigate/common"
	"github.com/flowbehappy/tigate/downstreamadapter/dispatcher"
	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/downstreamadapter/writer"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/node"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/google/uuid"
	"github.com/ngaut/log"
	"go.uber.org/zap"

	"github.com/tikv/client-go/v2/oracle"
)

const workerCount = 8

/*
EventDispatcherManager is responsible for managing the dispatchers of a changefeed in the instance.
EventDispatcherManager is working on:
1. Collecting all the heartbeat messages from all the dispatchers to make HeartBeatRequest.
2. Sending the HeartBeatResponse to each dispatcher.
3. Createing the new dispatchers.

One changefeed in one instance can only have one EventDispatcherManager.
One EventDispatcherManager can only have one Sink.
*/
type EventDispatcherManager struct {
	DispatcherMap               map[common.DispatcherID]*dispatcher.TableEventDispatcher
	TableTriggerEventDispatcher *dispatcher.TableTriggerEventDispatcher
	EventCollector              *node.EventCollector
	HeartbeatResponseQueue      *HeartbeatResponseQueue
	HeartbeatRequestQueue       *HeartbeatRequestQueue
	Id                          uint64
	ChangefeedID                uint64
	SinkType                    string
	Sink                        sink.Sink
	EnableSyncPoint             bool
	SyncPointInterval           time.Duration
	//filter                      *Filter
}

func (e *EventDispatcherManager) Init(startTs uint64, sinkURI *url.URL) {
	// Init Sink
	if e.SinkType == "Mysql" {
		cfg, db, err := writer.NewMysqlConfigAndDB(sinkURI)
		if err != nil {
			log.Error("create mysql sink failed", zap.Error(err))
		}
		e.Sink = sink.NewMysqlSink(workerCount, cfg, db)
	}

	// Init Table Trigger Event Dispatcher, TODO: in demo we don't need deal with ddl
	//e.TableTriggerEventDispatcher = e.newTableTriggerEventDispatcher(startTs)

	// init heartbeat recv and send task
	// No need to run recv task when there is no ddl event
	//threadpool.GetTaskSchedulerInstance().HeartbeatTaskScheduler.Submit(newHeartbeatRecvTask(e))
	threadpool.GetTaskSchedulerInstance().HeartbeatTaskScheduler.Submit(newHeartBeatSendTask(e))
}

func calculateStartSyncPointTs(startTs uint64, syncPointInterval time.Duration) uint64 {
	k := oracle.GetTimeFromTS(startTs).Sub(time.Unix(0, 0)) / syncPointInterval
	if oracle.GetTimeFromTS(startTs).Sub(time.Unix(0, 0))%syncPointInterval != 0 || oracle.ExtractLogical(startTs) != 0 {
		k += 1
	}
	return oracle.GoTimeToTS(time.Unix(0, 0).Add(k * syncPointInterval))
}

// 收到 rpc 请求创建，需要通过 event dispatcher manager 来
func (e *EventDispatcherManager) NewTableEventDispatcher(tableSpan *common.TableSpan, startTs uint64, sinkURI *url.URL) *dispatcher.TableEventDispatcher {
	// 创建新的 event dispatcher，同时需要把这个去 logService 注册，并且把自己加到对应的某个处理 thread 里
	if len(e.DispatcherMap) == 0 {
		e.Init(startTs, sinkURI)
	}

	var syncPointInfo *dispatcher.SyncPointInfo
	if e.EnableSyncPoint {
		syncPointInfo.EnableSyncPoint = true
		syncPointInfo.SyncPointInterval = e.SyncPointInterval
		syncPointInfo.NextSyncPointTs = calculateStartSyncPointTs(startTs, e.SyncPointInterval)
	} else {
		syncPointInfo.EnableSyncPoint = false
	}
	tableEventDispatcher := dispatcher.NewTableEventDispatcher(tableSpan, e.Sink, startTs, syncPointInfo)

	e.EventCollector.RegisterDispatcher(tableEventDispatcher, startTs, nil)

	// 注意先后顺序，可以从后往前加

	// 加入 manager 的 dispatcherMap 中
	e.DispatcherMap[tableEventDispatcher.Id] = tableEventDispatcher

	return tableEventDispatcher
}

func (e *EventDispatcherManager) newTableTriggerEventDispatcher(startTs uint64) *dispatcher.TableTriggerEventDispatcher {
	tableTriggerEventDispatcher := &dispatcher.TableTriggerEventDispatcher{
		Id: common.DispatcherID(uuid.New()),
		//Filter:        e.filter,
		Ch:            make(chan *common.TxnEvent, 1000),
		ResolvedTs:    startTs,
		HeartbeatChan: make(chan *dispatcher.HeartBeatResponseMessage, 100),
		Sink:          e.Sink,
		TableSpan:     &common.DDLSpan,
		State:         dispatcher.NewState(),
		MemoryUsage:   dispatcher.NewMemoryUsage(),
	}
	threadpool.GetTaskSchedulerInstance().EventDispatcherTaskScheduler.Submit(dispatcher.NewEventDispatcherTask(tableTriggerEventDispatcher))
	//e.EventCollector.RegisterDispatcher(tableTriggerEventDispatcher, startTs, e.filter)
	return tableTriggerEventDispatcher

}

func convertToHeartBeatPBTableSpans(tableSpans []*common.TableSpan) []*heartbeatpb.TableSpan {
	pbTableSpans := make([]*heartbeatpb.TableSpan, 0)
	for _, tableSpan := range tableSpans {
		pbTableSpans = append(pbTableSpans, &heartbeatpb.TableSpan{
			TableID:  tableSpan.TableID,
			StartKey: tableSpan.StartKey,
			EndKey:   tableSpan.EndKey,
		})
	}
	return pbTableSpans
}

func (e *EventDispatcherManager) CollectHeartbeatInfo() *heartbeatpb.HeartBeatRequest {
	/* 这里定义一下 checkpointTs
	   依旧表示的是小于这个 ts 的值已经落盘到 downstream 了。
	   我们计算的方式为：
	   1. 如果 dispatcher 中目前还有 event，则我们就拿要往下写的最大的 commitTs 的那条 event.CommitTs - 1 为 checkpointTs
	   2. dispatcher 在从 logService 拉数据的时候，会标记到 resolvedTs 为多少（这里指的是完整的 resolvedTs），所以 checkpointTs <= resolvedTs

	   另外我们对于 目前处于 blocked 状态的 dispatcher，我们也会记录他 blocked 住的 ts，以及 blocked 住的 tableSpan
	   后面我们要测一下这个 msg 的大小，以及 collect 的耗时
	*/

	var minCheckpointTs uint64 = math.MaxUint64
	for _, tableEventDispatcher := range e.DispatcherMap {
		checkpointTs := dispatcher.CollectDispatcherCheckpointTs(tableEventDispatcher)
		if minCheckpointTs > checkpointTs {
			minCheckpointTs = checkpointTs
		}
	}
	var message heartbeatpb.HeartBeatRequest = heartbeatpb.HeartBeatRequest{
		EventDispatcherManagerID: e.Id,
		CheckpointTs:             minCheckpointTs,
	}
	// heartbeatInfo := dispatcher.CollectDispatcherHeartBeatInfo(e.TableTriggerEventDispatcher)
	// message.Progress = append(message.Progress, &heartbeatpb.TableSpanProgress{
	// 	CheckpointTs:   heartbeatInfo.CheckpointTs,
	// 	BlockTs:        heartbeatInfo.BlockTs,
	// 	BlockTableSpan: convertToHeartBeatPBTableSpans(heartbeatInfo.BlockTableSpan),
	// 	IsBlocked:      heartbeatInfo.IsBlocked,
	// })
	return &message
}
