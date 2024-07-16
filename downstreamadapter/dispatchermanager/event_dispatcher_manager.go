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

	"github.com/flowbehappy/tigate/coordinator"
	"github.com/flowbehappy/tigate/downstreamadapter/dispatcher"
	"github.com/flowbehappy/tigate/downstreamadapter/eventcollector"
	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/downstreamadapter/writer"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/google/uuid"
	"github.com/ngaut/log"
	"github.com/pingcap/tiflow/cdc/model"
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
	DispatcherMap               map[*common.TableSpan]*dispatcher.TableEventDispatcher
	TableTriggerEventDispatcher *dispatcher.TableTriggerEventDispatcher
	HeartbeatResponseQueue      *HeartbeatResponseQueue
	HeartbeatRequestQueue       *HeartbeatRequestQueue
	ChangefeedID                model.ChangeFeedID
	SinkType                    string
	SinkURI                     *url.URL
	Sink                        sink.Sink
	EnableSyncPoint             bool
	SyncPointInterval           time.Duration
	ClusterID                   messaging.ServerId
	MaintainerID                messaging.ServerId

	// 接收一些特定状态下要发出去的 heartbeat info 的 tableSpan status 信息
	TableSpanStatusesChan chan *heartbeatpb.TableSpanStatus
	//filter                      *Filter
}

// for compiler
type ChangefeedConfig struct {
	sinkType string
	SinkURI  *url.URL
}

func NewEventDispatcherManager(changefeedID model.ChangeFeedID, config *ChangefeedConfig, clusterID messaging.ServerId, maintainerID messaging.ServerId) *EventDispatcherManager {
	eventDispatcherManager := EventDispatcherManager{
		DispatcherMap:          make(map[*common.TableSpan]*dispatcher.TableEventDispatcher),
		ChangefeedID:           changefeedID,
		HeartbeatResponseQueue: NewHeartbeatResponseQueue(),
		SinkType:               config.sinkType,
		SinkURI:                config.SinkURI,
		//SinkConfig:             config.SinkConfig,
		EnableSyncPoint:       false,
		ClusterID:             clusterID,
		MaintainerID:          maintainerID,
		TableSpanStatusesChan: make(chan *heartbeatpb.TableSpanStatus, 100),
	}
	return &eventDispatcherManager
}

func (e *EventDispatcherManager) Init(startTs uint64) {
	// Init Sink
	if e.SinkType == "Mysql" {
		cfg, db, err := writer.NewMysqlConfigAndDB(e.SinkURI)
		if err != nil {
			log.Error("create mysql sink failed", zap.Error(err))
		}
		e.Sink = sink.NewMysqlSink(workerCount, cfg, db)
	}

	// Init Table Trigger Event Dispatcher, TODO: in demo we don't need deal with ddl
	//e.TableTriggerEventDispatcher = e.newTableTriggerEventDispatcher(startTs)
	// e.EventCollector = downstreamadapter.NewEventCollector(e.messageCenter, 100*1024*1024*1024, e.ClusterID, e.ClusterID)
	// init heartbeat recv and send task
	// No need to run recv task when there is no ddl event
	//threadpool.GetTaskSchedulerInstance().HeartbeatTaskScheduler.Submit(newHeartbeatRecvTask(e))
	threadpool.GetTaskSchedulerInstance().HeartbeatTaskScheduler.Submit(newHeartBeatSendTask(e), threadpool.CPUTask, time.Time{})
}

func calculateStartSyncPointTs(startTs uint64, syncPointInterval time.Duration) uint64 {
	k := oracle.GetTimeFromTS(startTs).Sub(time.Unix(0, 0)) / syncPointInterval
	if oracle.GetTimeFromTS(startTs).Sub(time.Unix(0, 0))%syncPointInterval != 0 || oracle.ExtractLogical(startTs) != 0 {
		k += 1
	}
	return oracle.GoTimeToTS(time.Unix(0, 0).Add(k * syncPointInterval))
}

// 收到 rpc 请求创建，需要通过 event dispatcher manager 来
func (e *EventDispatcherManager) NewTableEventDispatcher(tableSpan *common.TableSpan, startTs uint64) *dispatcher.TableEventDispatcher {
	// 创建新的 event dispatcher，同时需要把这个去 logService 注册，并且把自己加到对应的某个处理 thread 里
	if len(e.DispatcherMap) == 0 {
		e.Init(startTs)
	}

	if _, ok := e.DispatcherMap[tableSpan]; ok {
		log.Warn("table span already exists", zap.Any("tableSpan", tableSpan))
		return nil
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

	context.GetService[*eventcollector.EventCollector]("eventCollector").RegisterDispatcher(tableEventDispatcher, startTs)

	e.DispatcherMap[tableSpan] = tableEventDispatcher

	return tableEventDispatcher
}

func (e *EventDispatcherManager) RemoveTableEventDispatcher(tableSpan *common.TableSpan) {
	if dispatcher, ok := e.DispatcherMap[tableSpan]; ok {
		// 判断一下状态，如果 removing 的话就不用管，
		context.GetService[*eventcollector.EventCollector]("eventCollector").RemoveDispatcher(dispatcher)
		dispatcher.Remove()
	} else {
		// 如果已经 removed ，就要在 返回的心跳里加一下这个checkpointTs 信息
		e.TableSpanStatusesChan <- &heartbeatpb.TableSpanStatus{
			Span:            tableSpan.TableSpan,
			ComponentStatus: int32(coordinator.ComponentStatusStopped),
		}
	}
}

func (e *EventDispatcherManager) CleanTableEventDispatcher(tableSpan *common.TableSpan) {
	delete(e.DispatcherMap, tableSpan)
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
	//threadpool.GetTaskSchedulerInstance().EventDispatcherTaskScheduler.Submit(dispatcher.NewEventDispatcherTask(tableTriggerEventDispatcher))
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

	var message heartbeatpb.HeartBeatRequest = heartbeatpb.HeartBeatRequest{
		ChangefeedID: e.ChangefeedID.String(),
	}

	var minCheckpointTs uint64 = math.MaxUint64
	for _, tableEventDispatcher := range e.DispatcherMap {
		// 判断状态，如果 removing 的话，加入 tryClose 检查，如果成功了就 清理其他的 （clean)，这边要加个 cache 存一下 tryClose 成功后的值，以防数据丢失
		dispatcherHeartBeatInfo := dispatcher.CollectDispatcherHeartBeatInfo(tableEventDispatcher)

		componentStatus := dispatcherHeartBeatInfo.ComponentStatus
		if componentStatus == coordinator.ComponentStatusStopping {
			checkpointTs, ok := tableEventDispatcher.TryClose()
			if ok {
				// remove successfully
				if minCheckpointTs > checkpointTs {
					minCheckpointTs = checkpointTs
				}
				message.Statuses = append(message.Statuses, &heartbeatpb.TableSpanStatus{
					Span:            dispatcherHeartBeatInfo.TableSpan.TableSpan,
					ComponentStatus: int32(coordinator.ComponentStatusStopped),
				})
				e.CleanTableEventDispatcher(dispatcherHeartBeatInfo.TableSpan)
				continue
			}
		}

		if minCheckpointTs > dispatcherHeartBeatInfo.CheckpointTs {
			minCheckpointTs = dispatcherHeartBeatInfo.CheckpointTs
		}

		message.Statuses = append(message.Statuses, &heartbeatpb.TableSpanStatus{
			Span:            dispatcherHeartBeatInfo.TableSpan.TableSpan,
			ComponentStatus: int32(dispatcherHeartBeatInfo.ComponentStatus),
		})

	}

	message.CheckpointTs = minCheckpointTs
	return &message
}
