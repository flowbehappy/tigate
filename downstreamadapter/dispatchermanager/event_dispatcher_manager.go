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

	"github.com/flowbehappy/tigate/pkg/node"

	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"

	"github.com/flowbehappy/tigate/downstreamadapter/dispatcher"
	"github.com/flowbehappy/tigate/downstreamadapter/eventcollector"
	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/eventpb"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/metrics"
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
	maintainerID node.ID

	// statusesChan will fetch the tableSpan status that need to contains in the heartbeat info.
	statusesChan chan *heartbeatpb.TableSpanStatus
	filter       filter.Filter

	closing bool
	closed  atomic.Bool

	heartBeatTask *HeartBeatTask

	schemaIDToDispatchers *SchemaIDToDispatchers

	tableTriggerEventDispatcher *dispatcher.Dispatcher

	tableEventDispatcherCount      prometheus.Gauge
	metricCreateDispatcherDuration prometheus.Observer
	metricCheckpointTs             prometheus.Gauge
	metricCheckpointTsLag          prometheus.Gauge
	metricResolveTs                prometheus.Gauge
	metricResolvedTsLag            prometheus.Gauge
}

func NewEventDispatcherManager(changefeedID model.ChangeFeedID,
	cfConfig *config.ChangefeedConfig,
	maintainerID node.ID) *EventDispatcherManager {
	ctx, cancel := context.WithCancel(context.Background())
	manager := &EventDispatcherManager{
		dispatcherMap: newDispatcherMap(),
		changefeedID:  changefeedID,
		//enableSyncPoint:       false,
		maintainerID:                   maintainerID,
		statusesChan:                   make(chan *heartbeatpb.TableSpanStatus, 10000),
		cancel:                         cancel,
		config:                         cfConfig,
		schemaIDToDispatchers:          newSchemaIDToDispatchers(),
		tableEventDispatcherCount:      metrics.TableEventDispatcherGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricCreateDispatcherDuration: metrics.CreateDispatcherDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricCheckpointTs:             metrics.EventDispatcherManagerCheckpointTsGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricCheckpointTsLag:          metrics.EventDispatcherManagerCheckpointTsLagGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricResolveTs:                metrics.EventDispatcherManagerResolvedTsGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricResolvedTsLag:            metrics.EventDispatcherManagerResolvedTsLagGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
	}

	// TODO: 最后去更新一下 filter 的内部 NewFilter 函数，现在是在套壳适配
	replicaConfig := cfg.ReplicaConfig{Filter: cfConfig.Filter}
	filter, err := filter.NewFilter(replicaConfig.Filter, cfConfig.TimeZone, replicaConfig.CaseSensitive)
	if err != nil {
		log.Error("create filter failed", zap.Error(err))
		return nil
	}
	manager.filter = filter

	appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RegisterEventDispatcherManager(manager)

	// TODO: 这些后续需要等有第一个 table 来的时候再初始化, 对于纯空的 event dispatcher manager 不要直接创建为好
	manager.heartBeatTask = newHeartBeatTask(manager)

	manager.InitSink()

	manager.wg.Add(1)
	go manager.CollectHeartbeatInfoWhenStatesChanged(ctx)
	manager.updateMetrics(ctx)
	return manager
}

func (e *EventDispatcherManager) InitSink() error {
	sink, err := sink.NewSink(e.config, e.changefeedID)
	if err != nil {
		log.Error("create sink failed", zap.Error(err))
		return err
	}
	e.sink = sink
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
	e.dispatcherMap.ForEach(func(id common.DispatcherID, dispatcher *dispatcher.Dispatcher) {
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

func (e *EventDispatcherManager) NewDispatcher(id common.DispatcherID, tableSpan *heartbeatpb.TableSpan, startTs uint64, schemaID int64) *dispatcher.Dispatcher {
	start := time.Now()
	if _, ok := e.dispatcherMap.Get(id); ok {
		log.Debug("table span already exists", zap.Any("tableSpan", tableSpan))
		return nil
	}

	dispatcher := dispatcher.NewDispatcher(id, tableSpan, e.sink, startTs, e.statusesChan, e.filter, schemaID)

	if tableSpan.Equal(heartbeatpb.DDLSpan) {
		e.tableTriggerEventDispatcher = dispatcher
	} else {
		e.schemaIDToDispatchers.Set(schemaID, id)
	}

	appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RegisterDispatcher(
		eventcollector.RegisterInfo{
			Dispatcher:   dispatcher,
			StartTs:      startTs,
			FilterConfig: toFilterConfigPB(e.config.Filter),
		},
	)
	e.dispatcherMap.Set(id, dispatcher)
	e.GetStatusesChan() <- &heartbeatpb.TableSpanStatus{
		ID:              id.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
	}

	//TODO:区分 tableTriggerEventDIspatcher 的 metrics
	e.tableEventDispatcherCount.Inc()

	log.Info("new dispatcher created",
		zap.String("ID", id.String()),
		zap.Any("tableSpan", tableSpan.String()),
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
		case tableSpanStatus := <-e.GetStatusesChan():
			statusMessage = append(statusMessage, tableSpanStatus)

			delay := time.NewTimer(10 * time.Millisecond)
		loop:
			for {
				select {
				case tableSpanStatus := <-e.GetStatusesChan():
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

func (e *EventDispatcherManager) RemoveDispatcher(id common.DispatcherID) {
	dispatcher, ok := e.dispatcherMap.Get(id)

	if ok {
		if dispatcher.GetRemovingStatus() {
			return
		}
		appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RemoveDispatcher(dispatcher)
		dispatcher.Remove()
	} else {
		e.GetStatusesChan() <- &heartbeatpb.TableSpanStatus{
			ID:              id.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Stopped,
		}
	}
}

// Only called when the dispatcher is removed successfully.
func (e *EventDispatcherManager) cleanTableEventDispatcher(id common.DispatcherID, schemaID int64) {
	e.dispatcherMap.Delete(id)
	e.schemaIDToDispatchers.Delete(schemaID, id)
	if e.tableTriggerEventDispatcher != nil && e.tableTriggerEventDispatcher.GetId() == id {
		e.tableTriggerEventDispatcher = nil
	}
	e.tableEventDispatcherCount.Dec()
	log.Info("table event dispatcher completely stopped, and delete it from event dispatcher manager", zap.Any("dispatcher id", id))
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

	toReomveDispatcherIDs := make([]common.DispatcherID, 0)
	removeDispatcherSchemaIDs := make([]int64, 0)
	heartBeatInfo := &dispatcher.HeartBeatInfo{}
	e.dispatcherMap.ForEach(func(id common.DispatcherID, dispatcherItem *dispatcher.Dispatcher) {
		// If the dispatcher is in removing state, we need to check if it's closed successfully.
		// If it's closed successfully, we could clean it up.
		// TODO: we need to consider how to deal with the checkpointTs of the removed dispatcher if the message will be discarded.
		dispatcherItem.CollectDispatcherHeartBeatInfo(heartBeatInfo)
		if heartBeatInfo.IsRemoving {
			watermark, ok := dispatcherItem.TryClose()
			if ok {
				// remove successfully
				message.Watermark.UpdateMin(watermark)
				// If the dispatcher is removed successfully, we need to add the tableSpan into message whether needCompleteStatus is true or not.
				message.Statuses = append(message.Statuses, &heartbeatpb.TableSpanStatus{
					ID:              id.ToPB(),
					ComponentStatus: heartbeatpb.ComponentState_Stopped,
					CheckpointTs:    watermark.CheckpointTs,
				})
				toReomveDispatcherIDs = append(toReomveDispatcherIDs, id)
				removeDispatcherSchemaIDs = append(removeDispatcherSchemaIDs, dispatcherItem.GetSchemaID())
			}
		}

		message.Watermark.UpdateMin(heartBeatInfo.Watermark)

		if needCompleteStatus {
			message.Statuses = append(message.Statuses, &heartbeatpb.TableSpanStatus{
				ID:              id.ToPB(),
				ComponentStatus: heartBeatInfo.ComponentStatus,
				CheckpointTs:    heartBeatInfo.Watermark.CheckpointTs,
			})
		}
	})

	for idx, id := range toReomveDispatcherIDs {
		e.cleanTableEventDispatcher(id, removeDispatcherSchemaIDs[idx])
	}
	return &message
}

func (e *EventDispatcherManager) GetDispatcherMap() *DispatcherMap {
	return e.dispatcherMap
}

func (e *EventDispatcherManager) GetSchemaIDToDispatchers() *SchemaIDToDispatchers {
	return e.schemaIDToDispatchers
}

func (e *EventDispatcherManager) GetMaintainerID() node.ID {
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

func (e *EventDispatcherManager) GetStatusesChan() chan *heartbeatpb.TableSpanStatus {
	return e.statusesChan
}

func (e *EventDispatcherManager) SetMaintainerID(maintainerID node.ID) {
	e.maintainerID = maintainerID
}

// Get all dispatchers id of the specified schemaID. Including the tableTriggerEventDispatcherID if exists.
func (e *EventDispatcherManager) GetAllDispatchers(schemaID int64) []common.DispatcherID {
	dispatcherIDs := e.GetSchemaIDToDispatchers().GetDispatcherIDs(schemaID)
	if e.tableTriggerEventDispatcher != nil {
		dispatcherIDs = append(dispatcherIDs, e.tableTriggerEventDispatcher.GetId())
	}
	return dispatcherIDs
}

func (e *EventDispatcherManager) updateMetrics(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Second)
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				minResolvedTs := uint64(0)
				e.dispatcherMap.ForEach(func(_ common.DispatcherID, dispatcherItem *dispatcher.Dispatcher) {
					if minResolvedTs == 0 || dispatcherItem.GetResolvedTs() < minResolvedTs {
						minResolvedTs = dispatcherItem.GetResolvedTs()
					}
				})
				if minResolvedTs == 0 {
					continue
				}
				phyResolvedTs := oracle.ExtractPhysical(minResolvedTs)
				lag := (oracle.GetPhysical(time.Now()) - phyResolvedTs) / 1e3
				e.metricResolvedTsLag.Set(float64(lag))
			}
		}
	}()
	return nil
}

type DispatcherMap struct {
	m sync.Map
}

func newDispatcherMap() *DispatcherMap {
	return &DispatcherMap{
		m: sync.Map{},
	}
}

func (d *DispatcherMap) Len() int {
	var len = 0
	d.m.Range(func(_, _ interface{}) bool {
		len++
		return true
	})
	return len
}

func (d *DispatcherMap) Get(id common.DispatcherID) (*dispatcher.Dispatcher, bool) {
	dispatcherItem, ok := d.m.Load(id)
	if ok {
		return dispatcherItem.(*dispatcher.Dispatcher), ok
	}
	return nil, false
}

func (d *DispatcherMap) Delete(id common.DispatcherID) {
	d.m.Delete(id)
}

func (d *DispatcherMap) Set(id common.DispatcherID, dispatcher *dispatcher.Dispatcher) {
	d.m.Store(id, dispatcher)
}

func (d *DispatcherMap) ForEach(fn func(id common.DispatcherID, dispatcher *dispatcher.Dispatcher)) {
	d.m.Range(func(key, value interface{}) bool {
		fn(key.(common.DispatcherID), value.(*dispatcher.Dispatcher))
		return true
	})
}

type SchemaIDToDispatchers struct {
	mutex sync.RWMutex
	m     map[int64]map[common.DispatcherID]interface{}
}

func newSchemaIDToDispatchers() *SchemaIDToDispatchers {
	return &SchemaIDToDispatchers{
		m: make(map[int64]map[common.DispatcherID]interface{}),
	}
}

func (s *SchemaIDToDispatchers) Set(schemaID int64, dispatcherID common.DispatcherID) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.m[schemaID]; !ok {
		s.m[schemaID] = make(map[common.DispatcherID]interface{})
	}
	s.m[schemaID][dispatcherID] = struct{}{}
}

func (s *SchemaIDToDispatchers) Delete(schemaID int64, dispatcherID common.DispatcherID) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.m[schemaID]; ok {
		delete(s.m[schemaID], dispatcherID)
	}
}

func (s *SchemaIDToDispatchers) GetDispatcherIDs(schemaID int64) []common.DispatcherID {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if ids, ok := s.m[schemaID]; ok {
		dispatcherIDs := make([]common.DispatcherID, 0, len(ids))
		for id := range ids {
			dispatcherIDs = append(dispatcherIDs, id)
		}
		return dispatcherIDs
	}
	return nil
}
