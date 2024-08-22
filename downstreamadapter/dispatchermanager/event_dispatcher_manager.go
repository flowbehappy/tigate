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

	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/log"
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

	heartbeatRequestQueue *HeartbeatRequestQueue

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
	filter                filter.Filter

	closing bool
	closed  atomic.Bool

	heartBeatTask *HeartBeatTask

	tableEventDispatcherCount      prometheus.Gauge
	metricCreateDispatcherDuration prometheus.Observer
	metricCheckpointTs             prometheus.Gauge
	metricCheckpointTsLag          prometheus.Gauge
	metricResolveTs                prometheus.Gauge
	metricResolvedTsLag            prometheus.Gauge
}
type HeartBeatTask struct {
	taskHandle             *threadpool.TaskHandle
	eventDispatcherManager *EventDispatcherManager
	counter                int
}

func newHeartBeatTask(eventDispatcherManager *EventDispatcherManager) *HeartBeatTask {
	taskScheduler := appcontext.GetService[*threadpool.TaskScheduler](appcontext.HeartBeatTaskScheduler)
	t := &HeartBeatTask{
		eventDispatcherManager: eventDispatcherManager,
		counter:                0,
	}
	t.taskHandle = taskScheduler.Submit(t, threadpool.CPUTask, time.Now().Add(time.Second*1))
	return t
}

func (t *HeartBeatTask) Execute() (threadpool.TaskStatus, time.Time) {
	if t.eventDispatcherManager.closed.Load() {
		return threadpool.Done, time.Time{}
	}
	t.counter = (t.counter + 1) % 10
	needCompleteStatus := t.counter == 0
	message := t.eventDispatcherManager.CollectHeartbeatInfo(needCompleteStatus)
	t.eventDispatcherManager.GetHeartbeatRequestQueue().Enqueue(&HeartBeatRequestWithTargetID{TargetID: t.eventDispatcherManager.GetMaintainerID(), Request: message})
	return threadpool.CPUTask, time.Now().Add(time.Second * 1)
}

func (t *HeartBeatTask) Cancel() {
	t.taskHandle.Cancel()
}

func NewEventDispatcherManager(changefeedID model.ChangeFeedID, changefeedConfig *config.ChangefeedConfig, maintainerID messaging.ServerId, createTableTriggerEventDispatcher bool) *EventDispatcherManager {
	ctx, cancel := context.WithCancel(context.Background())
	eventDispatcherManager := &EventDispatcherManager{
		dispatcherMap: newDispatcherMap(),
		changefeedID:  changefeedID,
		//enableSyncPoint:       false,
		maintainerID:                   maintainerID,
		tableSpanStatusesChan:          make(chan *heartbeatpb.TableSpanStatus, 10000),
		cancel:                         cancel,
		config:                         changefeedConfig,
		tableEventDispatcherCount:      metrics.TableEventDispatcherGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricCreateDispatcherDuration: metrics.CreateDispatcherDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricCheckpointTs:             metrics.EventDispatcherManagerCheckpointTsGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricCheckpointTsLag:          metrics.EventDispatcherManagerCheckpointTsLagGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricResolveTs:                metrics.EventDispatcherManagerResolvedTsGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricResolvedTsLag:            metrics.EventDispatcherManagerResolvedTsLagGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
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

	if createTableTriggerEventDispatcher {
		dispatcher := eventDispatcherManager.NewDispatcher(&common.DDLSpan, eventDispatcherManager.config.StartTS)
		eventDispatcherManager.dispatcherMap.Set(&common.DDLSpan, dispatcher)
	}

	// TODO: 这些后续需要等有第一个 table 来的时候再初始化, 对于纯空的 event dispatcher manager 不要直接创建为好

	eventDispatcherManager.heartBeatTask = newHeartBeatTask(eventDispatcherManager)

	eventDispatcherManager.InitSink()

	eventDispatcherManager.wg.Add(1)
	go eventDispatcherManager.CollectHeartbeatInfoWhenStatesChanged(ctx)

	return eventDispatcherManager
}

func (e *EventDispatcherManager) InitSink() error {
	cfg, db, err := writer.NewMysqlConfigAndDB(e.config.SinkURI)
	if err != nil {
		log.Error("create mysql sink failed", zap.Error(err))
		return err
	}

	e.sink = sink.NewMysqlSink(e.changefeedID, 16, cfg, db)
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

	toCloseDispatchers := make([]*dispatcher.Dispatcher, 0)
	e.dispatcherMap.ForEach(func(tableSpan *common.TableSpan, dispatcher *dispatcher.Dispatcher) {
		dispatcher.Remove()
		_, ok := dispatcher.TryClose()
		if !ok {
			toCloseDispatchers = append(toCloseDispatchers, dispatcher)
		}
	})

	for _, dispatcher := range toCloseDispatchers {
		log.Info("waiting for dispatcher to close", zap.Any("tableSpan", dispatcher.GetTableSpan()))
		ok := false
		for !ok {
			_, ok = dispatcher.TryClose()
			time.Sleep(10 * time.Millisecond)
		}
	}

	e.heartBeatTask.Cancel()

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

func (e *EventDispatcherManager) NewDispatcher(tableSpan *common.TableSpan, startTs uint64) *dispatcher.Dispatcher {
	start := time.Now()
	if _, ok := e.dispatcherMap.Get(tableSpan); ok {
		log.Debug("table span already exists", zap.Any("tableSpan", tableSpan))
		return nil
	}

	dispatcher := dispatcher.NewDispatcher(tableSpan, e.sink, startTs, e.tableSpanStatusesChan, e.filter)

	// TODO:暂时不收 ddl 的 event
	if tableSpan != &common.DDLSpan {
		appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RegisterDispatcher(
			eventcollector.RegisterInfo{
				Dispatcher:   dispatcher,
				StartTs:      startTs,
				FilterConfig: toFilterConfigPB(e.config.Filter),
			},
		)
	}

	e.dispatcherMap.Set(tableSpan, dispatcher)
	e.GetTableSpanStatusesChan() <- &heartbeatpb.TableSpanStatus{
		Span:            tableSpan.TableSpan,
		ComponentStatus: heartbeatpb.ComponentState_Working,
	}

	//TODO:区分 tableTriggerEventDIspatcher 的 metrics
	e.tableEventDispatcherCount.Inc()

	log.Info("new dispatcher created", zap.Any("tableSpan", tableSpan),
		zap.Int64("cost(ns)", time.Since(start).Nanoseconds()), zap.Time("start", start))
	e.metricCreateDispatcherDuration.Observe(float64(time.Since(start).Seconds()))

	return dispatcher
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

func (e *EventDispatcherManager) RemoveDispatcher(tableSpan *common.TableSpan) {
	dispatcher, ok := e.dispatcherMap.Get(tableSpan)

	if ok {
		if dispatcher.GetRemovingStatus() {
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
	message := heartbeatpb.HeartBeatRequest{
		ChangefeedID:    e.changefeedID.ID,
		CompeleteStatus: needCompleteStatus,
		Watermark:       heartbeatpb.NewMaxWatermark(),
	}

	toReomveTableSpans := make([]*common.TableSpan, 0)
	allDispatchers := e.dispatcherMap.GetAllDispatchers()
	dispatcherHeartBeatInfo := &dispatcher.HeartBeatInfo{}
	for _, dispatcherItem := range allDispatchers {
		// TODO:ddlSpan先不参与
		if dispatcherItem.GetTableSpan() == &common.DDLSpan {
			continue
		}
		// If the dispatcher is in removing state, we need to check if it's closed successfully.
		// If it's closed successfully, we could clean it up.
		// TODO: we need to consider how to deal with the checkpointTs of the removed dispatcher if the message will be discarded.
		dispatcherItem.CollectDispatcherHeartBeatInfo(dispatcherHeartBeatInfo)
		if dispatcherHeartBeatInfo.IsRemoving {
			watermark, ok := dispatcherItem.TryClose()
			if ok {
				// remove successfully
				message.Watermark.UpdateMin(watermark)
				// If the dispatcher is removed successfully, we need to add the tableSpan into message whether needCompleteStatus is true or not.
				message.Statuses = append(message.Statuses, &heartbeatpb.TableSpanStatus{
					Span:            dispatcherHeartBeatInfo.TableSpan.TableSpan,
					ComponentStatus: heartbeatpb.ComponentState_Stopped,
					CheckpointTs:    watermark.CheckpointTs,
				})
				toReomveTableSpans = append(toReomveTableSpans, dispatcherItem.GetTableSpan())
			}
		}

		message.Watermark.UpdateMin(dispatcherHeartBeatInfo.Watermark)

		if needCompleteStatus {
			message.Statuses = append(message.Statuses, &heartbeatpb.TableSpanStatus{
				Span:            dispatcherHeartBeatInfo.TableSpan.TableSpan,
				ComponentStatus: dispatcherHeartBeatInfo.ComponentStatus,
				CheckpointTs:    dispatcherHeartBeatInfo.Watermark.CheckpointTs,
			})
		}
	}

	for _, tableSpan := range toReomveTableSpans {
		e.cleanTableEventDispatcher(tableSpan)
	}
	ckptTs := oracle.ExtractPhysical(message.Watermark.CheckpointTs)
	e.metricCheckpointTs.Set(float64(ckptTs))
	lag := (oracle.GetPhysical(time.Now()) - ckptTs) / 1e3
	e.metricCheckpointTsLag.Set(float64(lag))
	resolvedTs := oracle.ExtractPhysical(message.Watermark.ResolvedTs)
	e.metricResolveTs.Set(float64(resolvedTs))
	lag = (oracle.GetPhysical(time.Now()) - resolvedTs) / 1e3
	e.metricResolvedTsLag.Set(float64(lag))
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

type DispatcherMap struct {
	dispatcherCacheForRead []*dispatcher.Dispatcher
	mutex                  sync.Mutex
	dispatchers            *utils.BtreeMap[*common.TableSpan, *dispatcher.Dispatcher]
}

func newDispatcherMap() *DispatcherMap {
	return &DispatcherMap{
		dispatcherCacheForRead: make([]*dispatcher.Dispatcher, 0, 1024),
		dispatchers:            utils.NewBtreeMap[*common.TableSpan, *dispatcher.Dispatcher](),
	}
}

func (d *DispatcherMap) Len() int {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.dispatchers.Len()
}

func (d *DispatcherMap) Get(tableSpan *common.TableSpan) (*dispatcher.Dispatcher, bool) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.dispatchers.Get(tableSpan)
}

func (d *DispatcherMap) Delete(tableSpan *common.TableSpan) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.dispatchers.Delete(tableSpan)
}

func (d *DispatcherMap) Set(tableSpan *common.TableSpan, dispatcher *dispatcher.Dispatcher) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.dispatchers.ReplaceOrInsert(tableSpan, dispatcher)
}

func (d *DispatcherMap) ForEach(fn func(tableSpan *common.TableSpan, dispatcher *dispatcher.Dispatcher)) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.dispatchers.Ascend(func(tableSpan *common.TableSpan, dispatcherItem *dispatcher.Dispatcher) bool {
		fn(tableSpan, dispatcherItem)
		return true
	})
}

func (d *DispatcherMap) resetDispatcherCache() {
	if cap(d.dispatcherCacheForRead) > 2048 && cap(d.dispatcherCacheForRead) > d.dispatchers.Len()*2 {
		d.dispatcherCacheForRead = make([]*dispatcher.Dispatcher, 0, d.dispatchers.Len())
	}
	d.dispatcherCacheForRead = d.dispatcherCacheForRead[:0]
}

func (d *DispatcherMap) GetAllDispatchers() []*dispatcher.Dispatcher {
	d.resetDispatcherCache()

	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.dispatchers.Ascend(func(tableSpan *common.TableSpan, dispatcherItem *dispatcher.Dispatcher) bool {
		d.dispatcherCacheForRead = append(d.dispatcherCacheForRead, dispatcherItem)
		return true
	})
	return d.dispatcherCacheForRead
}
