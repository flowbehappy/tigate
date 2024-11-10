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

	"github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/node"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/eventcollector"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

/*
EventDispatcherManager is responsible for managing the dispatchers of a changefeed in the instance.
EventDispatcherManager is working on:
1. Collecting all the heartbeat messages from all the dispatchers to make HeartBeatRequest.
2. Sending the HeartBeatResponse to each dispatcher.
3. Create and remove dispatchers.
One changefeed in one instance has one EventDispatcherManager.
One EventDispatcherManager has one backend sink.
*/
type EventDispatcherManager struct {
	dispatcherMap *DispatcherMap

	heartbeatRequestQueue   *HeartbeatRequestQueue
	blockStatusRequestQueue *BlockStatusRequestQueue

	cancel context.CancelFunc
	wg     sync.WaitGroup

	changefeedID common.ChangeFeedID
	config       *config.ChangefeedConfig

	sink         sink.Sink
	maintainerID node.ID

	// statusesChan will fetch the tableSpan status that need to contains in the heartbeat info.
	statusesChan chan *heartbeatpb.TableSpanStatus
	// blockStatusesChan will fetch the tableSpan block status about ddl event and sync point event
	// that need to report to maintainer
	blockStatusesChan chan *heartbeatpb.TableSpanBlockStatus
	// dispatcherActionChan
	dispatcherActionChan chan common.DispatcherAction

	filter filter.Filter

	closing bool
	closed  atomic.Bool

	heartBeatTask *HeartBeatTask

	schemaIDToDispatchers *dispatcher.SchemaIDToDispatchers

	tableTriggerEventDispatcher *dispatcher.Dispatcher

	// only not nil when enable sync point
	// TODO: changefeed update config
	syncPointConfig *syncpoint.SyncPointConfig

	// collect the error in all the dispatchers and sink module
	// report the error to the maintainer
	errCh chan error

	tableEventDispatcherCount      prometheus.Gauge
	metricCreateDispatcherDuration prometheus.Observer
	metricCheckpointTs             prometheus.Gauge
	metricCheckpointTsLag          prometheus.Gauge
	metricResolvedTs               prometheus.Gauge
	metricResolvedTsLag            prometheus.Gauge
}

// return actual startTs of the table trigger event dispatcher
// when the table trigger event dispatcher is in this event dispatcher manager
func NewEventDispatcherManager(changefeedID common.ChangeFeedID,
	cfConfig *config.ChangefeedConfig,
	tableTriggerEventDispatcherID *heartbeatpb.DispatcherID,
	startTs uint64,
	maintainerID node.ID) (*EventDispatcherManager, uint64, error) {
	ctx, cancel := context.WithCancel(context.Background())
	manager := &EventDispatcherManager{
		dispatcherMap:                  newDispatcherMap(),
		changefeedID:                   changefeedID,
		maintainerID:                   maintainerID,
		statusesChan:                   make(chan *heartbeatpb.TableSpanStatus, 8192),
		blockStatusesChan:              make(chan *heartbeatpb.TableSpanBlockStatus, 1024*1024),
		dispatcherActionChan:           make(chan common.DispatcherAction, 1024*1024),
		errCh:                          make(chan error, 16),
		cancel:                         cancel,
		config:                         cfConfig,
		schemaIDToDispatchers:          dispatcher.NewSchemaIDToDispatchers(),
		tableEventDispatcherCount:      metrics.TableEventDispatcherGauge.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
		metricCreateDispatcherDuration: metrics.CreateDispatcherDuration.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
		metricCheckpointTs:             metrics.EventDispatcherManagerCheckpointTsGauge.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
		metricCheckpointTsLag:          metrics.EventDispatcherManagerCheckpointTsLagGauge.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
		metricResolvedTs:               metrics.EventDispatcherManagerResolvedTsGauge.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
		metricResolvedTsLag:            metrics.EventDispatcherManagerResolvedTsLagGauge.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
	}

	// Set Sync Point Config
	if cfConfig.EnableSyncPoint {
		// TODO:确认一下参数设置的地方会检查正确性，这里不需要再次检查了
		manager.syncPointConfig = &syncpoint.SyncPointConfig{
			SyncPointInterval:  util.GetOrZero(cfConfig.SyncPointInterval),
			SyncPointRetention: util.GetOrZero(cfConfig.SyncPointRetention),
		}
	}

	// Set Filter
	// TODO: 最后去更新一下 filter 的内部 NewFilter 函数，现在是在套壳适配
	replicaConfig := config.ReplicaConfig{Filter: cfConfig.Filter}
	filter, err := filter.NewFilter(replicaConfig.Filter, cfConfig.TimeZone, replicaConfig.CaseSensitive)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	manager.filter = filter

	err = manager.InitSink(ctx)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	// Register Event Dispatcher Manager in HeartBeatCollector, which is reponsible for communication with the maintainer.
	err = appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RegisterEventDispatcherManager(manager)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	// collector heart beat info from all dispatchers
	manager.wg.Add(1)
	go func() {
		defer manager.wg.Done()
		manager.CollectHeartbeatInfoWhenStatesChanged(ctx)
	}()

	manager.wg.Add(1)
	go func() {
		defer manager.wg.Done()
		select {
		case <-ctx.Done():
			return
		case err := <-manager.errCh:
			if errors.Cause(err) != context.Canceled {
				log.Error("Event Dispatcher Manager Meets Error",
					zap.String("changefeedID", manager.changefeedID.String()),
					zap.Error(err))

				// report error to maintainer
				var message heartbeatpb.HeartBeatRequest
				message.ChangefeedID = manager.changefeedID.ToPB()
				message.Err = &heartbeatpb.RunningError{
					Time:    time.Now().String(),
					Node:    appcontext.GetID(),
					Code:    string(apperror.ErrorCode(err)),
					Message: err.Error(),
				}
				manager.heartbeatRequestQueue.Enqueue(&HeartBeatRequestWithTargetID{TargetID: manager.GetMaintainerID(), Request: &message})
			}
		}
	}()

	// collector block status from all dispatchers
	manager.wg.Add(1)
	go func() {
		defer manager.wg.Done()
		manager.CollectBlockStatusRequest(ctx)
	}()

	manager.wg.Add(1)
	go func() {
		defer manager.wg.Done()
		manager.CollectDispatcherAction(ctx)
	}()

	// create tableTriggerEventDispatcher if it is not nil
	if tableTriggerEventDispatcherID != nil {
		err := manager.NewDispatchers([]DispatcherCreateInfo{
			{
				Id:        common.NewDispatcherIDFromPB(tableTriggerEventDispatcherID),
				TableSpan: heartbeatpb.DDLSpan,
				StartTs:   startTs,
				SchemaID:  0,
			},
		})
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
		return manager, manager.tableTriggerEventDispatcher.GetStartTs(), nil
	}

	return manager, 0, nil
}

func (e *EventDispatcherManager) InitSink(ctx context.Context) error {
	sink, err := sink.NewSink(ctx, e.config, e.changefeedID, e.errCh)
	if err != nil {
		return err
	}
	e.sink = sink
	return nil
}

func (e *EventDispatcherManager) TryClose(remove bool) bool {
	if !e.closing {
		e.closing = true
		go e.close(remove)
	}
	return e.closed.Load()
}

func (e *EventDispatcherManager) close(remove bool) {
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
	err := appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RemoveEventDispatcherManager(e)
	if err != nil {
		log.Error("remove event dispatcher manager from heartbeat collector failed", zap.Error(err))
		return
	}

	err = e.sink.Close(remove)
	if err != nil && errors.Cause(err) != context.Canceled {
		log.Error("close sink failed", zap.Error(err))
		return
	}

	metrics.CreateDispatcherDuration.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.EventDispatcherManagerCheckpointTsGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.EventDispatcherManagerResolvedTsGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())

	e.closed.Store(true)
	log.Info("event dispatcher manager closed", zap.Stringer("changefeedID", e.changefeedID))
}

type DispatcherCreateInfo struct {
	Id        common.DispatcherID
	TableSpan *heartbeatpb.TableSpan
	StartTs   uint64
	SchemaID  int64
}

func (e *EventDispatcherManager) NewDispatchers(infos []DispatcherCreateInfo) error {
	start := time.Now()

	dispatcherIds := make([]common.DispatcherID, 0, len(infos))
	tableIds := make([]int64, 0, len(infos))
	startTsList := make([]int64, 0, len(infos))
	tableSpans := make([]*heartbeatpb.TableSpan, 0, len(infos))
	schemaIds := make([]int64, 0, len(infos))
	for _, info := range infos {
		id := info.Id
		if _, ok := e.dispatcherMap.Get(id); ok {
			continue
		}
		dispatcherIds = append(dispatcherIds, id)
		tableIds = append(tableIds, info.TableSpan.TableID)
		startTsList = append(startTsList, int64(info.StartTs))
		tableSpans = append(tableSpans, info.TableSpan)
		schemaIds = append(schemaIds, info.SchemaID)
	}

	// we batch the creatation for the dispatchers,
	// mainly because we need to batch the query for startTs
	newStartTsList, err := e.sink.CheckStartTsList(tableIds, startTsList)
	if err != nil {
		return errors.Trace(err)
	}

	for idx, id := range dispatcherIds {
		if newStartTsList[idx] == -1 {
			e.statusesChan <- &heartbeatpb.TableSpanStatus{
				ID:              id.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Removed,
			}
			log.Info("this table is dropped, skip it and return removed status to maintainer",
				zap.Any("tableSpan", tableSpans[idx]),
				zap.Any("changefeedID", e.changefeedID.Name()),
				zap.Any("namespace", e.changefeedID.Namespace()))
			continue
		}

		syncPointInfo := syncpoint.SyncPointInfo{
			SyncPointConfig: e.syncPointConfig,
			EnableSyncPoint: false,
		}

		if e.syncPointConfig != nil {
			syncPointInfo.EnableSyncPoint = true
			syncPointInfo.InitSyncPointTs = syncpoint.CalculateStartSyncPointTs(uint64(newStartTsList[idx]), e.syncPointConfig.SyncPointInterval)
		}

		d := dispatcher.NewDispatcher(
			e.changefeedID,
			id, tableSpans[idx], e.sink,
			uint64(newStartTsList[idx]), e.dispatcherActionChan, e.blockStatusesChan,
			e.filter, schemaIds[idx], e.schemaIDToDispatchers, &syncPointInfo, e.errCh)

		if e.heartBeatTask == nil {
			e.heartBeatTask = newHeartBeatTask(e)
		}

		if tableSpans[idx].Equal(heartbeatpb.DDLSpan) {
			e.tableTriggerEventDispatcher = d
		} else {
			e.schemaIDToDispatchers.Set(schemaIds[idx], id)
		}

		appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).SendDispatcherRequest(
			eventcollector.DispatcherRequest{
				Dispatcher:   d,
				StartTs:      d.GetStartTs(),
				ActionType:   eventpb.ActionType_ACTION_TYPE_REGISTER,
				FilterConfig: toFilterConfigPB(e.config.Filter),
			},
		)

		e.dispatcherMap.Set(id, d)
		e.statusesChan <- &heartbeatpb.TableSpanStatus{
			ID:              id.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
		}

		e.tableEventDispatcherCount.Inc()

		log.Info("new dispatcher created",
			zap.String("ID", id.String()),
			zap.Any("changefeedID", e.changefeedID.Name()),
			zap.Any("namespace", e.changefeedID.Namespace()),
			zap.Any("tableSpan", tableSpans[idx]),
			zap.Any("startTs", newStartTsList[idx]))

	}
	e.metricCreateDispatcherDuration.Observe(float64(time.Since(start).Seconds()) / float64(len(dispatcherIds)))
	log.Info("batch create new dispatchers",
		zap.Any("changefeedID", e.changefeedID.Name()),
		zap.Any("namespace", e.changefeedID.Namespace()),
		zap.Int("count", len(dispatcherIds)),
		zap.Duration("duration", time.Since(start)))
	return nil
}

func (e *EventDispatcherManager) CollectBlockStatusRequest(ctx context.Context) {
	for {
		blockStatusMessage := make([]*heartbeatpb.TableSpanBlockStatus, 0)
		select {
		case <-ctx.Done():
			return
		case blockStatus := <-e.blockStatusesChan:
			blockStatusMessage = append(blockStatusMessage, blockStatus)

			delay := time.NewTimer(10 * time.Millisecond)
		loop:
			for {
				select {
				case blockStatus := <-e.blockStatusesChan:
					blockStatusMessage = append(blockStatusMessage, blockStatus)
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

			var message heartbeatpb.BlockStatusRequest
			message.ChangefeedID = e.changefeedID.ToPB()
			message.BlockStatuses = blockStatusMessage
			e.blockStatusRequestQueue.Enqueue(&BlockStatusRequestWithTargetID{TargetID: e.GetMaintainerID(), Request: &message})
		}
	}
}

// CollectHeartbeatInfoWhenStatesChanged use to collect the heartbeat info when GetTableSpanStatusesChan() get infos
// It happens when some dispatchers change status, such as --> working; --> stopped; --> stopping
// Considering collect the heartbeat info is a time-consuming operation(we need to scan all the dispatchers),
// We will not collect the heartbeat info as soon as we receive it, but will batch it appropriately.
func (e *EventDispatcherManager) CollectHeartbeatInfoWhenStatesChanged(ctx context.Context) {
	for {
		statusMessage := make([]*heartbeatpb.TableSpanStatus, 0)
		select {
		case <-ctx.Done():
			return
		case tableSpanStatus := <-e.statusesChan:
			statusMessage = append(statusMessage, tableSpanStatus)

			delay := time.NewTimer(10 * time.Millisecond)
		loop:
			for {
				select {
				case tableSpanStatus := <-e.statusesChan:
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
			message.ChangefeedID = e.changefeedID.ToPB()
			message.Statuses = statusMessage
			e.heartbeatRequestQueue.Enqueue(&HeartBeatRequestWithTargetID{TargetID: e.GetMaintainerID(), Request: &message})
		}
	}
}

// CollectDispatcherAction is used to collect the dispatcher action from the dispatcher action channel.
// The action could be pause, resume, reset.
func (e *EventDispatcherManager) CollectDispatcherAction(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case a := <-e.dispatcherActionChan:
			log.Info("collect dispatcher action", zap.String("action", a.String()))
			d, ok := e.dispatcherMap.Get(a.DispatcherID)
			// The dispatcher must in the dispatcherMap or equal to the tableTriggerEventDispatcher
			if !ok {
				if a.DispatcherID == e.tableTriggerEventDispatcher.GetId() {
					d = e.tableTriggerEventDispatcher
				}
			}
			if d == nil {
				log.Info("Dispatcher not found in dispatcherMap", zap.Any("dispatcher", a.DispatcherID))
				continue
			}

			var req eventcollector.DispatcherRequest
			switch a.Action {
			case common.ActionPause:
				// Get eventCollector
				req = eventcollector.DispatcherRequest{
					Dispatcher: d,
					ActionType: eventpb.ActionType_ACTION_TYPE_PAUSE,
				}
			case common.ActionResume:
				req = eventcollector.DispatcherRequest{
					Dispatcher: d,
					ActionType: eventpb.ActionType_ACTION_TYPE_PAUSE,
				}
				// Get eventCollector
			case common.ActionReset:
				req = eventcollector.DispatcherRequest{
					Dispatcher: d,
					StartTs:    d.GetStartTs(),
					ActionType: eventpb.ActionType_ACTION_TYPE_PAUSE,
				}
			default:
				log.Panic("unknown action type", zap.Any("action", a))
			}
			appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).SendDispatcherRequest(req)
		}
	}
}

func (e *EventDispatcherManager) RemoveDispatcher(id common.DispatcherID) {
	dispatcher, ok := e.dispatcherMap.Get(id)
	if ok {
		if dispatcher.GetRemovingStatus() {
			return
		}
		req := eventcollector.DispatcherRequest{
			Dispatcher: dispatcher,
			ActionType: eventpb.ActionType_ACTION_TYPE_REMOVE,
		}
		appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).SendDispatcherRequest(req)
		dispatcher.Remove()
	} else {
		e.statusesChan <- &heartbeatpb.TableSpanStatus{
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

func toFilterConfigPB(filter *config.FilterConfig) *eventpb.FilterConfig {
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
		ChangefeedID:    e.changefeedID.ToPB(),
		CompeleteStatus: needCompleteStatus,
		Watermark:       heartbeatpb.NewMaxWatermark(),
	}

	toRemoveDispatcherIDs := make([]common.DispatcherID, 0)
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
				toRemoveDispatcherIDs = append(toRemoveDispatcherIDs, id)
				removeDispatcherSchemaIDs = append(removeDispatcherSchemaIDs, dispatcherItem.GetSchemaID())
			}
		}
		message.Watermark.UpdateMin(heartBeatInfo.Watermark)

		if needCompleteStatus {
			message.Statuses = append(message.Statuses, &heartbeatpb.TableSpanStatus{
				ID:                 id.ToPB(),
				ComponentStatus:    heartBeatInfo.ComponentStatus,
				CheckpointTs:       heartBeatInfo.Watermark.CheckpointTs,
				EventSizePerSecond: heartBeatInfo.EventSizePerSecond,
			})
		}
	})

	for idx, id := range toRemoveDispatcherIDs {
		e.cleanTableEventDispatcher(id, removeDispatcherSchemaIDs[idx])
	}

	e.metricCheckpointTs.Set(float64(message.Watermark.CheckpointTs))
	e.metricResolvedTs.Set(float64(message.Watermark.ResolvedTs))

	phyCheckpointTs := oracle.ExtractPhysical(message.Watermark.CheckpointTs)
	phyResolvedTs := oracle.ExtractPhysical(message.Watermark.ResolvedTs)

	e.metricCheckpointTsLag.Set(float64(oracle.GetPhysical(time.Now())-phyCheckpointTs) / 1e3)
	e.metricResolvedTsLag.Set(float64(oracle.GetPhysical(time.Now())-phyResolvedTs) / 1e3)

	return &message
}

func (e *EventDispatcherManager) GetDispatcherMap() *DispatcherMap {
	return e.dispatcherMap
}

func (e *EventDispatcherManager) GetMaintainerID() node.ID {
	return e.maintainerID
}

func (e *EventDispatcherManager) GetChangeFeedID() common.ChangeFeedID {
	return e.changefeedID
}

func (e *EventDispatcherManager) SetHeartbeatRequestQueue(heartbeatRequestQueue *HeartbeatRequestQueue) {
	e.heartbeatRequestQueue = heartbeatRequestQueue
}

func (e *EventDispatcherManager) SetBlockStatusRequestQueue(blockStatusRequestQueue *BlockStatusRequestQueue) {
	e.blockStatusRequestQueue = blockStatusRequestQueue
}

func (e *EventDispatcherManager) GetBlockStatuses() chan *heartbeatpb.TableSpanBlockStatus {
	return e.blockStatusesChan
}

func (e *EventDispatcherManager) SetMaintainerID(maintainerID node.ID) {
	e.maintainerID = maintainerID
}

// Get all dispatchers id of the specified schemaID. Including the tableTriggerEventDispatcherID if exists.
func (e *EventDispatcherManager) GetAllDispatchers(schemaID int64) []common.DispatcherID {
	dispatcherIDs := e.schemaIDToDispatchers.GetDispatcherIDs(schemaID)
	if e.tableTriggerEventDispatcher != nil {
		dispatcherIDs = append(dispatcherIDs, e.tableTriggerEventDispatcher.GetId())
	}
	return dispatcherIDs
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
