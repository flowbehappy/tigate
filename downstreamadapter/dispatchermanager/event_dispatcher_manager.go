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

	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/node"

	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/eventcollector"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
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
 1. Init sink for the changefeed.
 2. Register in the HeartBeatCollector, which is responsible for communication with the maintainer.
    And collecting and batch the messages that need to communicate with the maintainer from all dispatchers,
    These messages will be truly sent to the maintainer by heartbeat collector.
    Messages include: 1. table status 2. block status 3. heartbeats.
 3. Create and remove all dispatchers, including table trigger event dispatcher.
 4. Collect the error from all the dispatchers and sink module, and report to the maintainer.

One changefeed in one instance has one EventDispatcherManager.
One EventDispatcherManager has one backend sink.
*/
type EventDispatcherManager struct {
	changefeedID common.ChangeFeedID
	maintainerID node.ID

	config       *config.ChangefeedConfig
	filterConfig *eventpb.FilterConfig
	// only not nil when enable sync point
	// TODO: changefeed update config
	syncPointConfig *syncpoint.SyncPointConfig

	// tableTriggerEventDispatcher is a special dispatcher, that is responsible for helping handling ddl events.
	tableTriggerEventDispatcher *dispatcher.Dispatcher
	// dispatcherMap restore all the dispatchers in the EventDispatcherManager, including table trigger event dispatcher
	dispatcherMap *DispatcherMap
	// schemaIDToDispatchers is store the schemaID info for all normal dispatchers.
	schemaIDToDispatchers *dispatcher.SchemaIDToDispatchers

	// statusesChan is used to store the status of dispatchers when status changed
	// and push to heartbeatRequestQueue
	statusesChan chan TableSpanStatusWithSeq
	// heartbeatRequestQueue is used to store the heartbeat request from all the dispatchers.
	// heartbeat collector will consume the heartbeat request from the queue and send the response to each dispatcher.
	heartbeatRequestQueue *HeartbeatRequestQueue

	// heartBeatTask is responsible for collecting the heartbeat info from all the dispatchers
	// and report to the maintainer periodicity.
	heartBeatTask *HeartBeatTask

	// blockStatusesChan will fetch the block status about ddl event and sync point event
	// and push to blockStatusRequestQueue
	blockStatusesChan chan *heartbeatpb.TableSpanBlockStatus
	// blockStatusRequestQueue is used to store the block status request from all the dispatchers.
	// heartbeat collector will consume the block status request from the queue and report to the maintainer.
	blockStatusRequestQueue *BlockStatusRequestQueue

	// sink is used to send all the events to the downstream.
	sink sink.Sink

	latestWatermark Watermark

	// collect the error in all the dispatchers and sink module
	// when we get the error, we will report the error to the maintainer
	errCh chan error

	closing atomic.Bool
	closed  atomic.Bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	metricTableTriggerEventDispatcherCount prometheus.Gauge
	metricEventDispatcherCount             prometheus.Gauge
	metricCreateDispatcherDuration         prometheus.Observer
	metricCheckpointTs                     prometheus.Gauge
	metricCheckpointTsLag                  prometheus.Gauge
	metricResolvedTs                       prometheus.Gauge
	metricResolvedTsLag                    prometheus.Gauge
}

// return actual startTs of the table trigger event dispatcher
// when the table trigger event dispatcher is in this event dispatcher manager
func NewEventDispatcherManager(
	changefeedID common.ChangeFeedID,
	cfConfig *config.ChangefeedConfig,
	tableTriggerEventDispatcherID *heartbeatpb.DispatcherID,
	startTs uint64,
	maintainerID node.ID) (*EventDispatcherManager, uint64, error) {
	ctx, cancel := context.WithCancel(context.Background())
	manager := &EventDispatcherManager{
		dispatcherMap:                          newDispatcherMap(),
		changefeedID:                           changefeedID,
		maintainerID:                           maintainerID,
		statusesChan:                           make(chan TableSpanStatusWithSeq, 8192),
		blockStatusesChan:                      make(chan *heartbeatpb.TableSpanBlockStatus, 1024*1024),
		errCh:                                  make(chan error, 1),
		cancel:                                 cancel,
		config:                                 cfConfig,
		filterConfig:                           toFilterConfigPB(cfConfig.Filter),
		schemaIDToDispatchers:                  dispatcher.NewSchemaIDToDispatchers(),
		latestWatermark:                        NewWatermark(startTs),
		metricTableTriggerEventDispatcherCount: metrics.TableTriggerEventDispatcherGauge.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
		metricEventDispatcherCount:             metrics.EventDispatcherGauge.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
		metricCreateDispatcherDuration:         metrics.CreateDispatcherDuration.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
		metricCheckpointTs:                     metrics.EventDispatcherManagerCheckpointTsGauge.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
		metricCheckpointTsLag:                  metrics.EventDispatcherManagerCheckpointTsLagGauge.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
		metricResolvedTs:                       metrics.EventDispatcherManagerResolvedTsGauge.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
		metricResolvedTsLag:                    metrics.EventDispatcherManagerResolvedTsLagGauge.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
	}

	// Set Sync Point Config
	if cfConfig.EnableSyncPoint {
		// TODO: confirm that parameter validation is done at the setting location, so no need to check again here
		manager.syncPointConfig = &syncpoint.SyncPointConfig{
			SyncPointInterval:  util.GetOrZero(cfConfig.SyncPointInterval),
			SyncPointRetention: util.GetOrZero(cfConfig.SyncPointRetention),
		}
	}

	err := manager.initSink(ctx)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	// Register Event Dispatcher Manager in HeartBeatCollector,
	// which is responsible for communication with the maintainer.
	err = appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RegisterEventDispatcherManager(manager)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	// collect errors from error channel
	manager.wg.Add(1)
	go func() {
		defer manager.wg.Done()
		manager.collectErrors(ctx)
	}()

	// collect heart beat info from all dispatchers
	manager.wg.Add(1)
	go func() {
		defer manager.wg.Done()
		manager.collectComponentStatusWhenChanged(ctx)
	}()

	// collect block status from all dispatchers
	manager.wg.Add(1)
	go func() {
		defer manager.wg.Done()
		manager.collectBlockStatusRequest(ctx)
	}()

	var tableTriggerStartTs uint64 = 0
	// init table trigger event dispatcher when tableTriggerEventDispatcherID is not nil
	if tableTriggerEventDispatcherID != nil {
		tableTriggerStartTs, err = manager.NewTableTriggerEventDispatcher(tableTriggerEventDispatcherID, startTs)
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
	}
	log.Info("event dispatcher manager created",
		zap.Stringer("changefeedID", changefeedID),
		zap.Stringer("maintainerID", maintainerID),
		zap.Uint64("startTs", startTs),
		zap.Uint64("tableTriggerStartTs", tableTriggerStartTs))
	return manager, tableTriggerStartTs, nil
}

func (e *EventDispatcherManager) initSink(ctx context.Context) error {
	sink, err := sink.NewSink(ctx, e.config, e.changefeedID, e.errCh)
	if err != nil {
		return err
	}
	e.sink = sink
	return nil
}

func (e *EventDispatcherManager) TryClose(removeChangefeed bool) bool {
	if !e.closing.Load() {
		e.closing.Store(true)
		go e.close(removeChangefeed)
	}
	return e.closed.Load()
}

func (e *EventDispatcherManager) close(removeChangefeed bool) {
	log.Info("closing event dispatcher manager", zap.Stringer("changefeedID", e.changefeedID))

	toCloseDispatchers := make([]*dispatcher.Dispatcher, 0)
	e.dispatcherMap.ForEach(func(id common.DispatcherID, dispatcher *dispatcher.Dispatcher) {
		appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RemoveDispatcher(dispatcher)
		if dispatcher.IsTableTriggerEventDispatcher() && e.sink.SinkType() != common.MysqlSinkType {
			err := appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RemoveCheckpointTsMessage(e.changefeedID)
			if err != nil {
				log.Error("remove checkpointTs message failed", zap.Error(err), zap.Any("changefeedID", e.changefeedID))
			}
		}
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

	err = e.sink.Close(removeChangefeed)
	if err != nil && errors.Cause(err) != context.Canceled {
		log.Error("close sink failed", zap.Error(err))
		return
	}

	e.cancel()
	e.wg.Wait()

	metrics.TableTriggerEventDispatcherGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.EventDispatcherGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.CreateDispatcherDuration.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.EventDispatcherManagerCheckpointTsGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.EventDispatcherManagerResolvedTsGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.EventDispatcherManagerCheckpointTsLagGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.EventDispatcherManagerResolvedTsLagGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())

	e.closed.Store(true)
	log.Info("event dispatcher manager closed", zap.Stringer("changefeedID", e.changefeedID))
}

type dispatcherCreateInfo struct {
	Id          common.DispatcherID
	TableSpan   *heartbeatpb.TableSpan
	StartTs     uint64
	SchemaID    int64
	CurrentPDTs uint64
}

func (e *EventDispatcherManager) NewTableTriggerEventDispatcher(id *heartbeatpb.DispatcherID, startTs uint64) (uint64, error) {
	err := e.newDispatchers([]dispatcherCreateInfo{
		{
			Id:          common.NewDispatcherIDFromPB(id),
			TableSpan:   heartbeatpb.DDLSpan,
			StartTs:     startTs,
			SchemaID:    0,
			CurrentPDTs: 0,
		},
	})
	if err != nil {
		return 0, errors.Trace(err)
	}
	log.Info("table trigger event dispatcher created",
		zap.Any("changefeedID", e.changefeedID.Name()),
		zap.Any("dispatcher", e.tableTriggerEventDispatcher.GetId()),
		zap.Uint64("startTs", e.tableTriggerEventDispatcher.GetStartTs()),
	)
	return e.tableTriggerEventDispatcher.GetStartTs(), nil
}

func (e *EventDispatcherManager) InitalizeTableTriggerEventDispatcher(schemaInfo []*heartbeatpb.SchemaInfo) error {
	if e.tableTriggerEventDispatcher == nil {
		return nil
	}
	err := e.tableTriggerEventDispatcher.InitalizeTableSchemaStore(schemaInfo)
	if err != nil {
		return errors.Trace(err)
	}
	// table trigger event dispatcher can register to event collector to receive events after finish the initial table schema store from the maintainer.
	appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).AddDispatcher(e.tableTriggerEventDispatcher, int(e.config.MemoryQuota))

	// when sink is not mysql-class, table trigger event dispatcher need to receive the checkpointTs message from maintainer.
	if e.sink.SinkType() != common.MysqlSinkType {
		appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RegisterCheckpointTsMessageDs(e)
	}

	return nil
}

func (e *EventDispatcherManager) newDispatchers(infos []dispatcherCreateInfo) error {
	start := time.Now()

	dispatcherIds := make([]common.DispatcherID, 0, len(infos))
	tableIds := make([]int64, 0, len(infos))
	startTsList := make([]int64, 0, len(infos))
	tableSpans := make([]*heartbeatpb.TableSpan, 0, len(infos))
	schemaIds := make([]int64, 0, len(infos))
	pdTsList := make([]uint64, 0, len(infos))
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
		pdTsList = append(pdTsList, info.CurrentPDTs)
	}

	if len(dispatcherIds) == 0 {
		return nil
	}

	// When sink is mysql-class, we need to query the startTs from the downstream.
	// Because we have to sync data at least from the last ddl commitTs to avoid write old data to new schema
	// While for other type sink, they don't have the problem of writing old data to new schema,
	// so we just return the startTs we get.
	// Besides, we batch the creatation for the dispatchers,
	// mainly because we need to batch the query for startTs when sink is mysql-class to reduce the time cost.
	var newStartTsList []int64
	var err error
	if e.sink.SinkType() == common.MysqlSinkType {
		newStartTsList, err = e.sink.(*sink.MysqlSink).GetStartTsList(tableIds, startTsList)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("calculate real startTs for dispatchers", zap.Any("receive startTs", startTsList), zap.Any("real startTs", newStartTsList))
	} else {
		newStartTsList = startTsList
	}

	for idx, id := range dispatcherIds {
		d := dispatcher.NewDispatcher(
			e.changefeedID,
			id, tableSpans[idx], e.sink,
			uint64(newStartTsList[idx]),
			e.blockStatusesChan,
			schemaIds[idx],
			e.schemaIDToDispatchers,
			e.syncPointConfig,
			e.filterConfig,
			pdTsList[idx],
			e.errCh)

		if e.heartBeatTask == nil {
			e.heartBeatTask = newHeartBeatTask(e)
		}

		if d.IsTableTriggerEventDispatcher() {
			e.tableTriggerEventDispatcher = d
		} else {
			e.schemaIDToDispatchers.Set(schemaIds[idx], id)
			// we don't register table trigger event dispatcher in event collector, when created.
			// Table trigger event dispatcher is a special dispatcher,
			// it need to wait get the initial table schema store from the maintainer, then will register to event collector to receive events.
			appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).AddDispatcher(d, int(e.config.MemoryQuota))
		}

		seq := e.dispatcherMap.Set(id, d)
		e.statusesChan <- TableSpanStatusWithSeq{
			TableSpanStatus: &heartbeatpb.TableSpanStatus{
				ID:              id.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Working,
			},
			StartTs: uint64(newStartTsList[idx]),
			Seq:     seq,
		}

		if d.IsTableTriggerEventDispatcher() {
			e.metricTableTriggerEventDispatcherCount.Inc()
		} else {
			e.metricEventDispatcherCount.Inc()
		}

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

// collectErrors collect the errors from the error channel and report to the maintainer.
func (e *EventDispatcherManager) collectErrors(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case err := <-e.errCh:
		if errors.Cause(err) != context.Canceled {
			log.Error("Event Dispatcher Manager Meets Error",
				zap.String("changefeedID", e.changefeedID.String()),
				zap.Error(err))

			// report error to maintainer
			var message heartbeatpb.HeartBeatRequest
			message.ChangefeedID = e.changefeedID.ToPB()
			message.Err = &heartbeatpb.RunningError{
				Time:    time.Now().String(),
				Node:    appcontext.GetID(),
				Code:    string(apperror.ErrorCode(err)),
				Message: err.Error(),
			}
			e.heartbeatRequestQueue.Enqueue(&HeartBeatRequestWithTargetID{TargetID: e.GetMaintainerID(), Request: &message})

			// resend message until the event dispatcher manager is closed
			// the first error is matter most, so we just need to resend it continuely and ignore the other errors.
			ticker := time.NewTicker(time.Second * 5)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					e.heartbeatRequestQueue.Enqueue(&HeartBeatRequestWithTargetID{TargetID: e.GetMaintainerID(), Request: &message})
				}
			}
		}
	}
}

// collectBlockStatusRequest collect the block status from the block status channel and report to the maintainer.
func (e *EventDispatcherManager) collectBlockStatusRequest(ctx context.Context) {
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

// collectComponentStatusWhenStatesChanged collect the component status info when the dispatchers states changed,
// such as --> working; --> stopped; --> stopping
// we will do a batch for the status, then send to heartbeatRequestQueue
func (e *EventDispatcherManager) collectComponentStatusWhenChanged(ctx context.Context) {
	for {
		statusMessage := make([]*heartbeatpb.TableSpanStatus, 0)
		watermark := e.latestWatermark.Get()
		select {
		case <-ctx.Done():
			return
		case tableSpanStatus := <-e.statusesChan:
			statusMessage = append(statusMessage, tableSpanStatus.TableSpanStatus)
			watermark.Seq = tableSpanStatus.Seq
			if tableSpanStatus.StartTs != 0 && tableSpanStatus.StartTs < watermark.CheckpointTs {
				watermark.CheckpointTs = tableSpanStatus.StartTs
			}
			if tableSpanStatus.StartTs != 0 && tableSpanStatus.StartTs < watermark.ResolvedTs {
				watermark.ResolvedTs = tableSpanStatus.StartTs
			}
			delay := time.NewTimer(10 * time.Millisecond)
		loop:
			for {
				select {
				case tableSpanStatus := <-e.statusesChan:
					statusMessage = append(statusMessage, tableSpanStatus.TableSpanStatus)
					if watermark.Seq < tableSpanStatus.Seq {
						watermark.Seq = tableSpanStatus.Seq
					}
					if tableSpanStatus.StartTs != 0 && tableSpanStatus.StartTs < watermark.CheckpointTs {
						watermark.CheckpointTs = tableSpanStatus.StartTs
					}
					if tableSpanStatus.StartTs != 0 && tableSpanStatus.StartTs < watermark.ResolvedTs {
						watermark.ResolvedTs = tableSpanStatus.StartTs
					}
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
			message.Watermark = watermark
			e.heartbeatRequestQueue.Enqueue(&HeartBeatRequestWithTargetID{TargetID: e.GetMaintainerID(), Request: &message})
		}
	}
}

// aggregateDispatcherHeartbeats aggregates heartbeat information from all dispatchers and generates a HeartBeatRequest.
// The function performs the following tasks:
// 1. Aggregates status and watermark information from all dispatchers
// 2. Handles removal of stopped dispatchers
// 3. Updates metrics for checkpoint and resolved timestamps
//
// Parameters:
//   - needCompleteStatus: when true, includes detailed status for all dispatchers in the response.
//     When false, only includes minimal information and watermarks to reduce message size.
//
// Returns a HeartBeatRequest containing the aggregated information.
func (e *EventDispatcherManager) aggregateDispatcherHeartbeats(needCompleteStatus bool) *heartbeatpb.HeartBeatRequest {
	message := heartbeatpb.HeartBeatRequest{
		ChangefeedID:    e.changefeedID.ToPB(),
		CompeleteStatus: needCompleteStatus,
		Watermark:       heartbeatpb.NewMaxWatermark(),
	}

	toRemoveDispatcherIDs := make([]common.DispatcherID, 0)
	removedDispatcherSchemaIDs := make([]int64, 0)
	heartBeatInfo := &dispatcher.HeartBeatInfo{}

	seq := e.dispatcherMap.ForEach(func(id common.DispatcherID, dispatcherItem *dispatcher.Dispatcher) {
		dispatcherItem.GetHeartBeatInfo(heartBeatInfo)
		// If the dispatcher is in removing state, we need to check if it's closed successfully.
		// If it's closed successfully, we could clean it up.
		// TODO: we need to consider how to deal with the checkpointTs of the removed dispatcher if the message will be discarded.
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
				removedDispatcherSchemaIDs = append(removedDispatcherSchemaIDs, dispatcherItem.GetSchemaID())
			}
		}

		message.Watermark.UpdateMin(heartBeatInfo.Watermark)
		if needCompleteStatus {
			message.Statuses = append(message.Statuses, &heartbeatpb.TableSpanStatus{
				ID:                 id.ToPB(),
				ComponentStatus:    heartBeatInfo.ComponentStatus,
				CheckpointTs:       heartBeatInfo.Watermark.CheckpointTs,
				EventSizePerSecond: dispatcherItem.GetEventSizePerSecond(),
			})
		}
	})
	message.Watermark.Seq = seq
	e.latestWatermark.Set(message.Watermark)

	// if the event dispatcher manager is closing, we don't to remove the stopped dispatchers.
	if !e.closing.Load() {
		for idx, id := range toRemoveDispatcherIDs {
			e.cleanDispatcher(id, removedDispatcherSchemaIDs[idx])
		}
	}

	e.metricCheckpointTs.Set(float64(message.Watermark.CheckpointTs))
	e.metricResolvedTs.Set(float64(message.Watermark.ResolvedTs))

	phyCheckpointTs := oracle.ExtractPhysical(message.Watermark.CheckpointTs)
	phyResolvedTs := oracle.ExtractPhysical(message.Watermark.ResolvedTs)

	e.metricCheckpointTsLag.Set(float64(oracle.GetPhysical(time.Now())-phyCheckpointTs) / 1e3)
	e.metricResolvedTsLag.Set(float64(oracle.GetPhysical(time.Now())-phyResolvedTs) / 1e3)
	return &message
}

func (e *EventDispatcherManager) removeDispatcher(id common.DispatcherID) {
	dispatcher, ok := e.dispatcherMap.Get(id)
	if ok {
		if dispatcher.GetRemovingStatus() {
			return
		}
		appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RemoveDispatcher(dispatcher)

		// for non-mysql class sink, only the event dispatcher manager with table trigger event dispatcher need to receive the checkpointTs message.
		if dispatcher.IsTableTriggerEventDispatcher() && e.sink.SinkType() != common.MysqlSinkType {
			err := appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RemoveCheckpointTsMessage(e.changefeedID)
			log.Error("remove checkpointTs message ds failed", zap.Error(err))
		}

		dispatcher.Remove()
	} else {
		e.statusesChan <- TableSpanStatusWithSeq{
			TableSpanStatus: &heartbeatpb.TableSpanStatus{
				ID:              id.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Stopped,
			},
			Seq: e.dispatcherMap.GetSeq(),
		}
	}
}

// cleanDispatcher is called when the dispatcher is removed successfully.
func (e *EventDispatcherManager) cleanDispatcher(id common.DispatcherID, schemaID int64) {
	e.dispatcherMap.Delete(id)
	e.schemaIDToDispatchers.Delete(schemaID, id)
	if e.tableTriggerEventDispatcher != nil && e.tableTriggerEventDispatcher.GetId() == id {
		e.tableTriggerEventDispatcher = nil
		e.metricTableTriggerEventDispatcherCount.Dec()
	} else {
		e.metricEventDispatcherCount.Dec()
	}
	log.Info("table event dispatcher completely stopped, and delete it from event dispatcher manager", zap.Any("dispatcher id", id))
}

func (e *EventDispatcherManager) GetDispatcherMap() *DispatcherMap {
	return e.dispatcherMap
}

func (e *EventDispatcherManager) GetMaintainerID() node.ID {
	return e.maintainerID
}

func (e *EventDispatcherManager) SetMaintainerID(maintainerID node.ID) {
	e.maintainerID = maintainerID
}

func (e *EventDispatcherManager) GetTableTriggerEventDispatcher() *dispatcher.Dispatcher {
	return e.tableTriggerEventDispatcher
}

func (e *EventDispatcherManager) SetHeartbeatRequestQueue(heartbeatRequestQueue *HeartbeatRequestQueue) {
	e.heartbeatRequestQueue = heartbeatRequestQueue
}

func (e *EventDispatcherManager) SetBlockStatusRequestQueue(blockStatusRequestQueue *BlockStatusRequestQueue) {
	e.blockStatusRequestQueue = blockStatusRequestQueue
}

// Get all dispatchers id of the specified schemaID. Including the tableTriggerEventDispatcherID if exists.
func (e *EventDispatcherManager) GetAllDispatchers(schemaID int64) []common.DispatcherID {
	dispatcherIDs := e.schemaIDToDispatchers.GetDispatcherIDs(schemaID)
	if e.tableTriggerEventDispatcher != nil {
		dispatcherIDs = append(dispatcherIDs, e.tableTriggerEventDispatcher.GetId())
	}
	return dispatcherIDs
}
