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
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"

	"github.com/flowbehappy/tigate/downstreamadapter/dispatcher"
	"github.com/flowbehappy/tigate/downstreamadapter/eventcollector"
	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/downstreamadapter/writer"
	"github.com/flowbehappy/tigate/eventpb"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/flowbehappy/tigate/utils"
	"github.com/pingcap/tiflow/cdc/model"
	cfg "github.com/pingcap/tiflow/pkg/config"
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

	tableTriggerEventDispatcher *dispatcher.TableTriggerEventDispatcher
	heartbeatResponseQueue      *HeartbeatResponseQueue
	heartbeatRequestQueue       *HeartbeatRequestQueue
	//heartBeatSendTask     *HeartBeatSendTask
	cancel context.CancelFunc
	wg     sync.WaitGroup

	changefeedID model.ChangeFeedID
	config       *config.ChangefeedConfig

	sink sink.Sink
	// enableSyncPoint       bool
	// syncPointInterval     time.Duration
	maintainerID messaging.ServerId

	// tableSpanStatusesChan will fetch the tableSpan status that need to contains in the heartbeat info.
	tableSpanStatusesChan chan *heartbeatpb.TableSpanStatus

	tableEventDispatcherCount      prometheus.Gauge
	filter                         filter.Filter
	metricCreateDispatcherDuration prometheus.Observer
	metricCheckpointTs             prometheus.Gauge
	metricResolveTs                prometheus.Gauge

	closing bool
	closed  atomic.Bool
}

func NewEventDispatcherManager(changefeedID model.ChangeFeedID, changefeedConfig *config.ChangefeedConfig, maintainerID messaging.ServerId) *EventDispatcherManager {
	ctx, cancel := context.WithCancel(context.Background())
	eventDispatcherManager := &EventDispatcherManager{
		dispatcherMap:          newDispatcherMap(),
		changefeedID:           changefeedID,
		heartbeatResponseQueue: NewHeartbeatResponseQueue(),
		//enableSyncPoint:       false,
		maintainerID:                   maintainerID,
		tableSpanStatusesChan:          make(chan *heartbeatpb.TableSpanStatus, 10000),
		cancel:                         cancel,
		config:                         changefeedConfig,
		tableEventDispatcherCount:      metrics.TableEventDispatcherGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricCreateDispatcherDuration: metrics.CreateDispatcherDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricCheckpointTs:             metrics.EventDispatcherManagerCheckpointTsGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricResolveTs:                metrics.EventDispatcherManagerResolvedTsGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
	}

	// TODO: 最后去更新一下 filter 的内部 NewFilter 函数，现在是在套壳适配
	replicaConfig := cfg.ReplicaConfig{Filter: changefeedConfig.Filter}
	filter, err := filter.NewFilter(&replicaConfig, changefeedConfig.TimeZone)
	if err != nil {
		log.Error("create filter failed", zap.Error(err))
		return nil
	}
	eventDispatcherManager.filter = filter

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

	// TODO: 这个后续需要等有第一个 table 来的时候再初始化
	eventDispatcherManager.Init(eventDispatcherManager.config.StartTS)

	eventDispatcherManager.wg.Add(1)
	go eventDispatcherManager.CollectHeartbeatInfoWhenStatesChanged(ctx)

	return eventDispatcherManager
}

func (e *EventDispatcherManager) Init(startTs uint64) error {
	cfg, db, err := writer.NewMysqlConfigAndDB(e.config.SinkURI)
	if err != nil {
		log.Error("create mysql sink failed", zap.Error(err))
		return err
	}

	e.sink = sink.NewMysqlSink(e.changefeedID, 16, cfg, db)

	// get heartbeat response from HeartBeatResponseQueue, and send to each dispatcher
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			heartbeatResponse := e.GetHeartbeatResponseQueue().Dequeue()
			dispatcherActions := heartbeatResponse.Actions
			for _, dispatcherAction := range dispatcherActions {
				tableSpan := dispatcherAction.Span
				dispatcher, ok := e.dispatcherMap.Get(&common.TableSpan{TableSpan: tableSpan})
				if !ok {
					log.Error("dispatcher not found", zap.Any("tableSpan", tableSpan))
					continue
				}
				dispatcher.GetDDLActions() <- dispatcherAction
			}
		}
	}()

	//Init Table Trigger Event Dispatcher
	e.tableTriggerEventDispatcher = e.newTableTriggerEventDispatcher(startTs)

	return nil
}

func (e *EventDispatcherManager) TryClose() bool {
	if !e.closing {
		e.closing = true
		go e.close()
	}
	return e.closed.Load()
}

func (e *EventDispatcherManager) close() {
	log.Info("closing event dispatcher manager", zap.Stringer("changefeedID", e.changefeedID))
	e.cancel()
	e.wg.Wait()

	toCloseDispatchers := make([]dispatcher.Dispatcher, 0)
	e.dispatcherMap.ForEach(func(tableSpan *common.TableSpan, dispatcher *dispatcher.TableEventDispatcher) {
		dispatcher.Remove()
		_, ok := dispatcher.TryClose()
		if !ok {
			toCloseDispatchers = append(toCloseDispatchers, dispatcher)
		}
	})

	_, ok := e.tableTriggerEventDispatcher.TryClose()
	if !ok {
		toCloseDispatchers = append(toCloseDispatchers, e.tableTriggerEventDispatcher)
	}

	for _, dispatcher := range toCloseDispatchers {
		log.Info("waiting for dispatcher to close", zap.Any("tableSpan", dispatcher.GetTableSpan()))
		ok := false
		for !ok {
			_, ok = dispatcher.TryClose()
			time.Sleep(10 * time.Millisecond)
		}
	}

	e.sink.Close()
	metrics.CreateDispatcherDuration.DeleteLabelValues(e.changefeedID.Namespace, e.changefeedID.ID)
	metrics.EventDispatcherManagerCheckpointTsGauge.DeleteLabelValues(e.changefeedID.Namespace, e.changefeedID.ID)
	metrics.EventDispatcherManagerResolvedTsGauge.DeleteLabelValues(e.changefeedID.Namespace, e.changefeedID.ID)

	e.closed.Store(true)
	log.Info("event dispatcher manager closed", zap.Stringer("changefeedID", e.changefeedID))
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
	tableEventDispatcher := dispatcher.NewTableEventDispatcher(tableSpan, e.sink, startTs, nil, e.tableSpanStatusesChan)

	appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RegisterDispatcher(tableEventDispatcher, startTs, nil)

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

	for {
		statusMessage := make([]*heartbeatpb.TableSpanStatus, 0)
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
			message.Statuses = statusMessage
			e.GetHeartbeatRequestQueue().Enqueue(&HeartBeatRequestWithTargetID{TargetID: e.GetMaintainerID(), Request: &message})
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

func (e *EventDispatcherManager) newTableTriggerEventDispatcher(startTs uint64) *dispatcher.TableTriggerEventDispatcher {
	tableTriggerEventDispatcher := dispatcher.NewTableTriggerEventDispatcher(e.sink, startTs, e.tableSpanStatusesChan, e.filter)

	appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RegisterDispatcher(tableTriggerEventDispatcher, startTs, toFilterConfigPB(e.config.Filter))

	return tableTriggerEventDispatcher
}

func toFilterConfigPB(filter *cfg.FilterConfig) *eventpb.FilterConfig {
	filterConfig := &eventpb.FilterConfig{
		Rules:            filter.Rules,
		IgnoreTxnStartTs: filter.IgnoreTxnStartTs,
		EventFilters:     make([]*eventpb.EventFilterRule, len(filter.EventFilters)),
	}

	for _, eventFilter := range filter.EventFilters {
		ignoreEvent := make([]string, len(eventFilter.IgnoreEvent))
		for _, event := range eventFilter.IgnoreEvent {
			ignoreEvent = append(ignoreEvent, string(event))
		}
		filterConfig.EventFilters = append(filterConfig.EventFilters, &eventpb.EventFilterRule{
			Matcher:                  eventFilter.Matcher,
			IgnoreEvent:              ignoreEvent,
			IgnoreSql:                eventFilter.IgnoreSQL,
			IgnoreInsertValueExpr:    eventFilter.IgnoreInsertValueExpr,
			IgnoreUpdateNewValueExpr: eventFilter.IgnoreUpdateNewValueExpr,
			IgnoreUpdateOldValueExpr: eventFilter.IgnoreUpdateOldValueExpr,
			IgnoreDeleteValueExpr:    eventFilter.IgnoreDeleteValueExpr,
		})
	}

	return filterConfig
}

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
	dispatcherHeartBeatInfo := &dispatcher.HeartBeatInfo{}
	for _, tableEventDispatcher := range allDispatchers {
		// If the dispatcher is in removing state, we need to check if it's closed successfully.
		// If it's closed successfully, we could clean it up.
		// TODO: we need to consider how to deal with the checkpointTs of the removed dispatcher if the message will be discarded.
		dispatcher.CollectDispatcherHeartBeatInfo(tableEventDispatcher, dispatcherHeartBeatInfo)

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

func (e *EventDispatcherManager) GetHeartbeatResponseQueue() *HeartbeatResponseQueue {
	return e.heartbeatResponseQueue
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

type DispatcherMap struct {
	dispatcherCacheForRead []*dispatcher.TableEventDispatcher
	mutex                  sync.Mutex
	dispatchers            *utils.BtreeMap[*common.TableSpan, *dispatcher.TableEventDispatcher]
}

func newDispatcherMap() *DispatcherMap {
	return &DispatcherMap{
		dispatcherCacheForRead: make([]*dispatcher.TableEventDispatcher, 0, 1024),
		dispatchers:            utils.NewBtreeMap[*common.TableSpan, *dispatcher.TableEventDispatcher](),
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

func (d *DispatcherMap) resetDispatcherCache() {
	if cap(d.dispatcherCacheForRead) > 2048 && cap(d.dispatcherCacheForRead) > d.dispatchers.Len()*2 {
		d.dispatcherCacheForRead = make([]*dispatcher.TableEventDispatcher, 0, d.dispatchers.Len())
	}
	d.dispatcherCacheForRead = d.dispatcherCacheForRead[:0]
}

func (d *DispatcherMap) GetAllDispatchers() []*dispatcher.TableEventDispatcher {
	d.resetDispatcherCache()

	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.dispatchers.Ascend(func(tableSpan *common.TableSpan, dispatcherItem *dispatcher.TableEventDispatcher) bool {
		d.dispatcherCacheForRead = append(d.dispatcherCacheForRead, dispatcherItem)
		return true
	})
	return d.dispatcherCacheForRead
}
