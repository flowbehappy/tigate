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
	"context"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/utils"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"

	"github.com/flowbehappy/tigate/downstreamadapter/dispatcher"
	"github.com/flowbehappy/tigate/downstreamadapter/eventcollector"
	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/downstreamadapter/writer"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

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
	dispatcherMap *DispatcherMap

	//tableTriggerEventDispatcher *dispatcher.TableTriggerEventDispatcher
	//heartbeatResponseQueue *HeartbeatResponseQueue
	heartbeatRequestQueue *HeartbeatRequestQueue
	//heartBeatSendTask     *HeartBeatSendTask
	cancel context.CancelFunc
	wg     sync.WaitGroup

	changefeedID model.ChangeFeedID
	config       *model.ChangefeedConfig
	//sinkType     string
	sink sink.Sink
	// enableSyncPoint       bool
	// syncPointInterval     time.Duration
	maintainerID messaging.ServerId

	// tableSpanStatusesChan will fetch the tableSpan status that need to contains in the heartbeat info.
	tableSpanStatusesChan chan *heartbeatpb.TableSpanStatus

	tableEventDispatcherCount prometheus.Gauge
	//filter                      *Filter
	metricCreateDispatcherDuration prometheus.Observer
	metricCheckpointTs             prometheus.Gauge
	metricResolveTs                prometheus.Gauge
}

// TODO:这个锁会在量级大于几万以后影响明显，10w 的 dispatcher同时创建需要预估10多分钟的开销。
// 1000个-- 500ms / 10000个 -- 44s, 指数影响
type DispatcherMap struct {
	mutex       sync.Mutex
	dispatchers *utils.BtreeMap[*common.TableSpan, *dispatcher.TableEventDispatcher]
}

func newDispatcherMap() *DispatcherMap {
	return &DispatcherMap{
		dispatchers: utils.NewBtreeMap[*common.TableSpan, *dispatcher.TableEventDispatcher](),
	}
}

func (d *DispatcherMap) Len() int {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.dispatchers.Len()
}

func (d *DispatcherMap) Get(tableSpan *common.TableSpan) (*dispatcher.TableEventDispatcher, bool) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.dispatchers.Get(tableSpan)
}

func (d *DispatcherMap) Delete(tableSpan *common.TableSpan) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.dispatchers.Delete(tableSpan)
}

func (d *DispatcherMap) Set(tableSpan *common.TableSpan, dispatcher *dispatcher.TableEventDispatcher) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.dispatchers.ReplaceOrInsert(tableSpan, dispatcher)
}

func (d *DispatcherMap) ForEach(fn func(tableSpan *common.TableSpan, dispatcher *dispatcher.TableEventDispatcher)) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.dispatchers.Ascend(func(tableSpan *common.TableSpan, dispatcherItem *dispatcher.TableEventDispatcher) bool {
		fn(tableSpan, dispatcherItem)
		return true
	})
}

func (d *DispatcherMap) GetAllDispatchers() []*dispatcher.TableEventDispatcher {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	var dispatchers []*dispatcher.TableEventDispatcher
	d.dispatchers.Ascend(func(tableSpan *common.TableSpan, dispatcherItem *dispatcher.TableEventDispatcher) bool {
		dispatchers = append(dispatchers, dispatcherItem)
		return true
	})
	return dispatchers
}

func NewEventDispatcherManager(changefeedID model.ChangeFeedID, config *model.ChangefeedConfig, clusterID messaging.ServerId, maintainerID messaging.ServerId) *EventDispatcherManager {
	ctx, cancel := context.WithCancel(context.Background())
	eventDispatcherManager := &EventDispatcherManager{
		dispatcherMap: newDispatcherMap(),
		changefeedID:  changefeedID,
		//heartbeatResponseQueue: NewHeartbeatResponseQueue(),
		//sinkType: config.sinkType,
		// sinkURI: config.SinkURI,
		//sinkConfig:             config.SinkConfig,
		//enableSyncPoint:       false,
		maintainerID:                   maintainerID,
		tableSpanStatusesChan:          make(chan *heartbeatpb.TableSpanStatus, 1000000),
		cancel:                         cancel,
		config:                         config,
		tableEventDispatcherCount:      TableEventDispatcherCount.WithLabelValues(changefeedID.String()),
		metricCreateDispatcherDuration: metrics.CreateDispatcherDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricCheckpointTs:             metrics.EventDispatcherManagerCheckpointTsGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricResolveTs:                metrics.EventDispatcherManagerResolvedTsGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
	}

	appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RegisterEventDispatcherManager(eventDispatcherManager)
	eventDispatcherManager.wg.Add(1)
	go func(ctx context.Context, e *EventDispatcherManager) {
		defer e.wg.Done()
		counter := 0
		ticker := time.NewTicker(time.Second * 1)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				counter = (counter + 1) % 10
				needCompleteStatus := counter == 0
				message := e.CollectHeartbeatInfo(needCompleteStatus)
				e.GetHeartbeatRequestQueue().Enqueue(&HeartBeatRequestWithTargetID{TargetID: eventDispatcherManager.GetMaintainerID(), Request: message})
			}
		}
	}(ctx, eventDispatcherManager)

	eventDispatcherManager.Init()

	eventDispatcherManager.wg.Add(1)
	go eventDispatcherManager.CollectHeartbeatInfoWhenStatesChanged(ctx)
	return eventDispatcherManager
}

// func (e *EventDispatcherManager) Init(startTs uint64) error {
func (e *EventDispatcherManager) Init() error {
	// Init Sink
	//if e.sinkType == "Mysql" {
	// if e.config.SinkURI == "" {
	// 	defaultSinkUri := "mysql://root@127.0.0.1:3306"
	// 	log.Info("init mysql sink, sink uri is empty, use default sink uri", zap.String("sinkURI", e.config.SinkURI), zap.String("defaultSinkUri", defaultSinkUri))
	// 	e.config.SinkURI = defaultSinkUri
	// }
	cfg, db, err := writer.NewMysqlConfigAndDB(e.config.SinkURI)
	if err != nil {
		log.Error("create mysql sink failed", zap.Error(err))
		return err
	}
	// e.sink = sink.NewMysqlSink(*e.config.SinkConfig.MySQLConfig.WorkerCount, cfg, db)
	e.sink = sink.NewMysqlSink(e.changefeedID, 16, cfg, db)
	return nil
	//}

	// Init Table Trigger Event Dispatcher, TODO: in demo we don't need deal with ddl
	//e.TableTriggerEventDispatcher = e.newTableTriggerEventDispatcher(startTs)

	// init heartbeat recv and send task
	// No need to run recv task when there is no ddl event
	//threadpool.GetTaskSchedulerInstance().HeartbeatTaskScheduler.Submit(newHeartbeatRecvTask(e))
}

/*
func calculateStartSyncPointTs(startTs uint64, syncPointInterval time.Duration) uint64 {
	k := oracle.GetTimeFromTS(startTs).Sub(time.Unix(0, 0)) / syncPointInterval
	if oracle.GetTimeFromTS(startTs).Sub(time.Unix(0, 0))%syncPointInterval != 0 || oracle.ExtractLogical(startTs) != 0 {
		k += 1
	}
	return oracle.GoTimeToTS(time.Unix(0, 0).Add(k * syncPointInterval))
}
*/

// 收到 rpc 请求创建，需要通过 event dispatcher manager 来
func (e *EventDispatcherManager) NewTableEventDispatcher(tableSpan *common.TableSpan, startTs uint64) *dispatcher.TableEventDispatcher {
	start := time.Now()
	// 创建新的 event dispatcher，同时需要把这个去 logService 注册，并且把自己加到对应的某个处理 thread 里
	// if e.dispatcherMap.Len() == 0 {
	// 	err := e.Init(startTs)
	// 	if err != nil {
	// 		log.Error("init sink failed", zap.Error(err))
	// 		return nil
	// 	}
	// }

	if _, ok := e.dispatcherMap.Get(tableSpan); ok {
		log.Debug("table span already exists", zap.Any("tableSpan", tableSpan))
		return nil
	}

	/*
		var syncPointInfo *dispatcher.SyncPointInfo
		if e.EnableSyncPoint {
			syncPointInfo.EnableSyncPoint = true
			syncPointInfo.SyncPointInterval = e.SyncPointInterval
			syncPointInfo.NextSyncPointTs = calculateStartSyncPointTs(startTs, e.SyncPointInterval)
		} else {
			syncPointInfo.EnableSyncPoint = false
		}
	*/
	tableEventDispatcher := dispatcher.NewTableEventDispatcher(tableSpan, e.sink, startTs, nil)

	appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RegisterDispatcher(tableEventDispatcher, startTs)

	e.dispatcherMap.Set(tableSpan, tableEventDispatcher)
	e.GetTableSpanStatusesChan() <- &heartbeatpb.TableSpanStatus{
		Span:            tableSpan.TableSpan,
		ComponentStatus: heartbeatpb.ComponentState_Working,
	}

	e.tableEventDispatcherCount.Inc()
	log.Info("new table event dispatcher created", zap.Any("tableSpan", tableSpan),
		zap.Int64("cost(ns)", time.Since(start).Nanoseconds()), zap.Time("start", start))
	e.metricCreateDispatcherDuration.Observe(float64(time.Since(start).Seconds()))
	return tableEventDispatcher
}

// CollectHeartbeatInfoWhenStatesChanged use to collect the heartbeat info when GetTableSpanStatusesChan() get infos
// It happenes when some dispatchers change status, such as --> working; --> stopped; --> stopping
// Considering collect the heartbeat info is a time-consuming operation(we need to scan all the dispatchers),
// We will not collect the heartbeat info as soon as we receive it, but will batch it appropriately.
func (e *EventDispatcherManager) CollectHeartbeatInfoWhenStatesChanged(ctx context.Context) {
	defer e.wg.Done()

	statusMessage := make([]*heartbeatpb.TableSpanStatus, 0)
	for {
		select {
		case <-ctx.Done():
			return
		case tableSpanStatus := <-e.GetTableSpanStatusesChan():
			statusMessage = append(statusMessage, tableSpanStatus)

			delay := time.NewTimer(10 * time.Millisecond)
		loop:
			for {
				select {
				case tableSpanStatus := <-e.GetTableSpanStatusesChan():
					statusMessage = append(statusMessage, tableSpanStatus)
				case <-delay.C:
					break loop
				}
			}

			// Release resources promptly
			if !delay.Stop() {
				select {
				case <-delay.C:
				default:
				}
			}

			var message heartbeatpb.HeartBeatRequest
			message.ChangefeedID = e.changefeedID.ID
			//message := e.CollectHeartbeatInfo(false)
			message.Statuses = statusMessage
			e.GetHeartbeatRequestQueue().Enqueue(&HeartBeatRequestWithTargetID{TargetID: e.GetMaintainerID(), Request: &message})
			statusMessage = statusMessage[:0]
		}
	}
}

func (e *EventDispatcherManager) RemoveTableEventDispatcher(tableSpan *common.TableSpan) {
	dispatcher, ok := e.dispatcherMap.Get(tableSpan)
	if ok {
		if dispatcher.GetComponentStatus() == heartbeatpb.ComponentState_Stopping {
			e.GetTableSpanStatusesChan() <- &heartbeatpb.TableSpanStatus{
				Span:            tableSpan.TableSpan,
				ComponentStatus: heartbeatpb.ComponentState_Stopping,
			}
			return
		}
		appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RemoveDispatcher(dispatcher)
		dispatcher.Remove()
	} else {
		e.GetTableSpanStatusesChan() <- &heartbeatpb.TableSpanStatus{
			Span:            tableSpan.TableSpan,
			ComponentStatus: heartbeatpb.ComponentState_Stopped,
		}
	}
}

// Only called when the dispatcher is removed successfully.
func (e *EventDispatcherManager) cleanTableEventDispatcher(tableSpan *common.TableSpan) {
	e.dispatcherMap.Delete(tableSpan)
	e.tableEventDispatcherCount.Dec()
	log.Info("table event dispatcher completely stopped, and delete it from event dispatcher manager", zap.Any("tableSpan", tableSpan))
}

/*
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
*/

// CollectHeartbeatInfo collects the heartbeat info of all the dispatchers and returns a HeartBeatRequest message.
// When needCompleteStatus is true, it will collect the complete table status of the dispatchers,
// otherwise, it will only calculate the min checkpointTs to cutoff the message size.
func (e *EventDispatcherManager) CollectHeartbeatInfo(needCompleteStatus bool) *heartbeatpb.HeartBeatRequest {
	/* 这里定义一下 checkpointTs
	   依旧表示的是小于这个 ts 的值已经落盘到 downstream 了。
	   我们计算的方式为：
	   1. 如果 dispatcher 中目前还有 event，则我们就拿要往下写的最大的 commitTs 的那条 event.CommitTs - 1 为 checkpointTs
	   2. dispatcher 在从 logService 拉数据的时候，会标记到 resolvedTs 为多少（这里指的是完整的 resolvedTs），所以 checkpointTs <= resolvedTs

	   另外我们对于 目前处于 blocked 状态的 dispatcher，我们也会记录他 blocked 住的 ts，以及 blocked 住的 tableSpan
	   后面我们要测一下这个 msg 的大小，以及 collect 的耗时
	*/

	message := heartbeatpb.HeartBeatRequest{
		ChangefeedID:    e.changefeedID.ID,
		CompeleteStatus: needCompleteStatus,
		Watermark:       heartbeatpb.NewMaxWatermark(),
	}

	toReomveTableSpans := make([]*common.TableSpan, 0)
	allDispatchers := e.dispatcherMap.GetAllDispatchers()
	// e.dispatcherMap.ForEach(func(tableSpan *common.TableSpan, tableEventDispatcher *dispatcher.TableEventDispatcher) {
	for _, tableEventDispatcher := range allDispatchers {
		// If the dispatcher is in removing state, we need to check if it's closed successfully.
		// If it's closed successfully, we could clean it up.
		// TODO: we need to consider how to deal with the checkpointTs of the removed dispatcher if the message will be discarded.
		dispatcherHeartBeatInfo := dispatcher.CollectDispatcherHeartBeatInfo(tableEventDispatcher)

		componentStatus := dispatcherHeartBeatInfo.ComponentStatus
		if componentStatus == heartbeatpb.ComponentState_Stopping {
			watermark, ok := tableEventDispatcher.TryClose()
			if ok {
				// remove successfully
				message.Watermark.UpdateMin(watermark)
				// If the dispatcher is removed successfully, we need to add the tableSpan into message whether needCompleteStatus is true or not.
				message.Statuses = append(message.Statuses, &heartbeatpb.TableSpanStatus{
					Span:            dispatcherHeartBeatInfo.TableSpan.TableSpan,
					ComponentStatus: heartbeatpb.ComponentState_Stopped,
				})
				toReomveTableSpans = append(toReomveTableSpans, tableEventDispatcher.GetTableSpan())
			}
		}

		message.Watermark.UpdateMin(dispatcherHeartBeatInfo.Watermark)

		if needCompleteStatus {
			message.Statuses = append(message.Statuses, &heartbeatpb.TableSpanStatus{
				Span:            dispatcherHeartBeatInfo.TableSpan.TableSpan,
				ComponentStatus: dispatcherHeartBeatInfo.ComponentStatus,
			})
		}
	}

	for _, tableSpan := range toReomveTableSpans {
		e.cleanTableEventDispatcher(tableSpan)
	}

	e.metricCheckpointTs.Set(float64(oracle.ExtractPhysical(message.Watermark.CheckpointTs)))
	e.metricResolveTs.Set(float64(oracle.ExtractPhysical(message.Watermark.ResolvedTs)))
	return &message
}

func (e *EventDispatcherManager) GetDispatcherMap() *DispatcherMap {
	return e.dispatcherMap
}

func (e *EventDispatcherManager) GetMaintainerID() messaging.ServerId {
	return e.maintainerID
}

func (e *EventDispatcherManager) GetChangeFeedID() model.ChangeFeedID {
	return e.changefeedID
}

func (e *EventDispatcherManager) GetHeartbeatRequestQueue() *HeartbeatRequestQueue {
	return e.heartbeatRequestQueue
}

func (e *EventDispatcherManager) SetHeartbeatRequestQueue(heartbeatRequestQueue *HeartbeatRequestQueue) {
	e.heartbeatRequestQueue = heartbeatRequestQueue
}

func (e *EventDispatcherManager) GetTableSpanStatusesChan() chan *heartbeatpb.TableSpanStatus {
	return e.tableSpanStatusesChan
}

func (e *EventDispatcherManager) SetMaintainerID(maintainerID messaging.ServerId) {
	e.maintainerID = maintainerID
}
