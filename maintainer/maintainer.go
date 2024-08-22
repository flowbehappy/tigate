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

package maintainer

import (
	"encoding/json"
	"fmt"
	"github.com/flowbehappy/tigate/utils/dynstream"
	"math"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/logservice/schemastore"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	configNew "github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Maintainer is response for handle changefeed replication tasks, Maintainer should:
// 1. schedules tables to ticdc watcher
// 2. calculate changefeed checkpoint ts
// 3. send changefeed status to coordinator
// 4. handle heartbeat reported by dispatcher
type Maintainer struct {
	id       model.ChangeFeedID
	config   *model.ChangeFeedInfo
	selfNode *common.NodeInfo

	stream        dynstream.DynamicStream[string, *Event, *Maintainer]
	taskScheduler *threadpool.TaskScheduler
	mc            messaging.MessageCenter

	watermark             *heartbeatpb.Watermark
	checkpointTsByCapture map[model.CaptureID]heartbeatpb.Watermark

	state        heartbeatpb.ComponentState
	bootstrapper *Bootstrapper

	changefeedSate model.FeedState

	removed *atomic.Bool

	initialized bool

	pdEndpoints []string
	nodeManager *watcher.NodeManager
	nodesClosed map[string]struct{}

	statusChanged  *atomic.Bool
	nodeChanged    *atomic.Bool
	lastReportTime time.Time

	scheduler *Scheduler

	removing        bool
	cascadeRemoving bool

	lastPrintStatusTime  time.Time
	lastCheckpointTsTime time.Time

	errLock         sync.Mutex
	runningErrors   map[messaging.ServerId]*heartbeatpb.RunningError
	runningWarnings map[messaging.ServerId]*heartbeatpb.RunningError

	changefeedCheckpointTsGauge    prometheus.Gauge
	changefeedCheckpointTsLagGauge prometheus.Gauge
	changefeedResolvedTsGauge      prometheus.Gauge
	changefeedResolvedTsLagGauge   prometheus.Gauge
	changefeedStatusGauge          prometheus.Gauge
	scheduledTaskGauge             prometheus.Gauge
	runningTaskGauge               prometheus.Gauge
	tableCountGauge                prometheus.Gauge
}

// NewMaintainer create the maintainer for the changefeed
func NewMaintainer(cfID model.ChangeFeedID,
	cfg *model.ChangeFeedInfo,
	selfNode *common.NodeInfo,
	stream dynstream.DynamicStream[string, *Event, *Maintainer],
	taskScheduler *threadpool.TaskScheduler,
	checkpointTs uint64,
	pdEndpoints []string,
) *Maintainer {
	m := &Maintainer{
		id:              cfID,
		selfNode:        selfNode,
		stream:          stream,
		taskScheduler:   taskScheduler,
		scheduler:       NewScheduler(1000, time.Minute),
		mc:              appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
		state:           heartbeatpb.ComponentState_Working,
		removed:         atomic.NewBool(false),
		nodeManager:     appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		nodesClosed:     make(map[string]struct{}),
		statusChanged:   atomic.NewBool(true),
		nodeChanged:     atomic.NewBool(false),
		cascadeRemoving: false,
		config:          cfg,
		watermark: &heartbeatpb.Watermark{
			CheckpointTs: checkpointTs,
			ResolvedTs:   checkpointTs,
		},
		checkpointTsByCapture: make(map[model.CaptureID]heartbeatpb.Watermark),
		pdEndpoints:           pdEndpoints,
		runningErrors:         map[messaging.ServerId]*heartbeatpb.RunningError{},
		runningWarnings:       map[messaging.ServerId]*heartbeatpb.RunningError{},

		changefeedCheckpointTsGauge:    metrics.ChangefeedCheckpointTsGauge.WithLabelValues(cfID.Namespace, cfID.ID),
		changefeedCheckpointTsLagGauge: metrics.ChangefeedCheckpointTsLagGauge.WithLabelValues(cfID.Namespace, cfID.ID),
		changefeedResolvedTsGauge:      metrics.ChangefeedResolvedTsGauge.WithLabelValues(cfID.Namespace, cfID.ID),
		changefeedResolvedTsLagGauge:   metrics.ChangefeedResolvedTsLagGauge.WithLabelValues(cfID.Namespace, cfID.ID),
		changefeedStatusGauge:          metrics.ChangefeedStatusGauge.WithLabelValues(cfID.Namespace, cfID.ID),
		scheduledTaskGauge:             metrics.ScheduleTaskGuage.WithLabelValues(cfID.Namespace, cfID.ID),
		runningTaskGauge:               metrics.RunningScheduleTaskGauge.WithLabelValues(cfID.Namespace, cfID.ID),
		tableCountGauge:                metrics.TableGauge.WithLabelValues(cfID.Namespace, cfID.ID),
	}
	m.bootstrapper = NewBootstrapper(m.getNewBootstrapFn())
	log.Info("maintainer is created", zap.String("id", cfID.String()))
	metrics.MaintainerGauge.WithLabelValues(cfID.Namespace, cfID.ID).Inc()
	return m
}

func (m *Maintainer) cleanupMetrics() {
	metrics.ChangefeedCheckpointTsGauge.DeleteLabelValues(m.id.Namespace, m.id.ID)
	metrics.ChangefeedCheckpointTsLagGauge.DeleteLabelValues(m.id.Namespace, m.id.ID)
	metrics.ChangefeedStatusGauge.DeleteLabelValues(m.id.Namespace, m.id.ID)
	metrics.ScheduleTaskGuage.DeleteLabelValues(m.id.Namespace, m.id.ID)
	metrics.RunningScheduleTaskGauge.DeleteLabelValues(m.id.Namespace, m.id.ID)
	metrics.TableGauge.DeleteLabelValues(m.id.Namespace, m.id.ID)
}

func (m *Maintainer) onMessage(msg *messaging.TargetMessage) error {
	switch msg.Type {
	case messaging.TypeHeartBeatRequest:
		return m.onHeartBeatRequest(msg)
	case messaging.TypeMaintainerBootstrapResponse:
		return m.onMaintainerBootstrapResponse(msg)
	case messaging.TypeMaintainerCloseResponse:
		m.onNodeClosed(string(msg.From), msg.Message[0].(*heartbeatpb.MaintainerCloseResponse))
	case messaging.TypeRemoveMaintainerRequest:
		m.onRemoveMaintainer(msg.Message[0].(*heartbeatpb.RemoveMaintainerRequest).Cascade)
	default:
		log.Panic("unexpected message type",
			zap.String("changefeed", m.id.ID),
			zap.String("type", msg.Type.String()))
	}
	return nil
}

func (m *Maintainer) onRemoveMaintainer(cascade bool) {
	m.removing = true
	m.cascadeRemoving = cascade
	closed := m.tryCloseChangefeed()
	if closed {
		m.removed.Store(true)
		m.state = heartbeatpb.ComponentState_Stopped
		metrics.MaintainerGauge.WithLabelValues(m.id.Namespace, m.id.ID).Dec()
		return
	}
}

func (m *Maintainer) onNodeChanged() error {
	activeNodes := m.nodeManager.GetAliveNodes()
	var newNodes = make(map[string]*common.NodeInfo)
	for id, node := range activeNodes {
		if _, ok := m.bootstrapper.nodes[id]; !ok {
			newNodes[id] = node
			m.scheduler.AddNewNode(id)
		}
	}
	var removedNodes []string
	for id, _ := range m.bootstrapper.nodes {
		if _, ok := activeNodes[id]; !ok {
			removedNodes = append(removedNodes, id)
			m.sendMessages(m.scheduler.RemoveNode(id))
		}
	}
	log.Info("maintainer node changed",
		zap.String("id", m.id.String()),
		zap.Int("new", len(newNodes)),
		zap.Int("removed", len(removedNodes)))
	m.sendMessages(m.bootstrapper.HandleNewNodes(newNodes))
	cachedResponse := m.bootstrapper.HandleRemoveNodes(removedNodes)
	if cachedResponse != nil {
		log.Info("bootstrap done after removed some nodes",
			zap.String("id", m.id.String()))
		if err := m.onBootstrapDone(cachedResponse); err != nil {
			return errors.Trace(err)
		}
	}
	if m.bootstrapper.CheckAllNodeInitialized() {
		return errors.Trace(m.scheduleTableSpan())
	}
	return nil
}

func (m *Maintainer) calCheckpointTs() {
	m.updateMetrics()
	if time.Since(m.lastCheckpointTsTime) < 2*time.Second ||
		m.scheduler.schedulingTask.Len() != 0 ||
		m.scheduler.absent.Len() != 0 {
		return
	}
	m.lastCheckpointTsTime = time.Now()

	newWatermark := heartbeatpb.NewMaxWatermark()
	for id, tasks := range m.scheduler.nodeTasks {
		if tasks.Len() > 0 {
			if _, ok := m.checkpointTsByCapture[id]; !ok {
				log.Debug("checkpointTs can not be advanced, since missing capture heartbeat",
					zap.String("changefeed", m.id.ID),
					zap.String("capture", id))
				return
			}
			newWatermark.UpdateMin(m.checkpointTsByCapture[id])
		}
	}
	if newWatermark.CheckpointTs != math.MaxUint64 {
		m.watermark.CheckpointTs = newWatermark.CheckpointTs
	}
	if newWatermark.ResolvedTs != math.MaxUint64 {
		m.watermark.ResolvedTs = newWatermark.ResolvedTs
	}
}

func (m *Maintainer) updateMetrics() {
	phyCkpTs := oracle.ExtractPhysical(m.watermark.CheckpointTs)
	m.changefeedCheckpointTsGauge.Set(float64(phyCkpTs))
	lag := (oracle.GetPhysical(time.Now()) - phyCkpTs) / 1e3
	m.changefeedCheckpointTsLagGauge.Set(float64(lag))

	phyResolvedTs := oracle.ExtractPhysical(m.watermark.ResolvedTs)
	m.changefeedResolvedTsGauge.Set(float64(phyResolvedTs))
	lag = (oracle.GetPhysical(time.Now()) - phyResolvedTs) / 1e3
	m.changefeedResolvedTsLagGauge.Set(float64(lag))

	m.changefeedStatusGauge.Set(float64(m.state))
}

// send message to remote, todo: use a io thread pool
func (m *Maintainer) sendMessages(msgs []*messaging.TargetMessage) {
	for _, msg := range msgs {
		err := m.mc.SendCommand(msg)
		if err != nil {
			log.Debug("failed to send maintainer request",
				zap.String("changefeed", m.id.ID),
				zap.Any("msg", msg), zap.Error(err))
			continue
		}
	}
}

func (m *Maintainer) scheduleTableSpan() error {
	if !m.bootstrapper.CheckAllNodeInitialized() {
		log.Info("not all node initialized, skip schedule",
			zap.String("changefeed", m.id.ID))
		return nil
	}
	msg, err := m.scheduler.Schedule()
	if err != nil {
		return errors.Trace(err)
	}
	m.sendMessages(msg)

	if m.scheduler.NeedSchedule() {
		// some table span is not scheduled, schedule it later
		submitScheduledEvent(m.taskScheduler, m.stream, &Event{
			cfID:      m.id.ID,
			eventType: EventSchedule,
		}, time.Now().Add(100*time.Millisecond))
	}
	return nil
}

// Close cleanup resources
func (m *Maintainer) Close() {
	m.cleanupMetrics()
	log.Info("changefeed maintainer closed",
		zap.String("id", m.id.String()),
		zap.Bool("removed", m.removed.Load()),
		zap.Uint64("checkpointTs", m.watermark.CheckpointTs))
}

func (m *Maintainer) initialize() error {
	var err error
	tableIDs, err := m.initTableIDs()
	for _, id := range tableIDs {
		span := spanz.TableIDToComparableSpan(id)
		tableSpan := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{
			TableID:  uint64(id),
			StartKey: span.StartKey,
			EndKey:   span.EndKey,
		}}
		replicaSet := NewReplicaSet(m.id, tableSpan, m.watermark.CheckpointTs).(*ReplicaSet)
		stm, err := scheduler.NewStateMachine(tableSpan, nil, replicaSet)
		if err != nil {
			return errors.Trace(err)
		}
		m.scheduler.absent.ReplaceOrInsert(tableSpan, stm)
	}
	log.Info("changefeed maintainer initialized",
		zap.String("id", m.id.String()))
	m.initialized = true
	m.state = heartbeatpb.ComponentState_Working
	m.statusChanged.Store(true)

	// detect the capture changes
	m.nodeManager.RegisterNodeChangeHandler("maintainer-"+m.id.ID, func(newNodes []*common.NodeInfo, removedNodes []*common.NodeInfo) {
		m.nodeChanged.Store(true)
		m.stream.In() <- &Event{cfID: m.id.ID, eventType: EventPeriod}
	})
	// send bootstrap message
	m.sendMessages(m.bootstrapper.HandleNewNodes(m.nodeManager.GetAliveNodes()))
	// setup period event
	submitScheduledEvent(m.taskScheduler, m.stream, &Event{
		cfID:      m.id.ID,
		eventType: EventPeriod,
	}, time.Now().Add(time.Millisecond*500))
	return err
}

func (m *Maintainer) onHeartBeatRequest(msg *messaging.TargetMessage) error {
	req := msg.Message[0].(*heartbeatpb.HeartBeatRequest)
	if req.Watermark != nil {
		m.checkpointTsByCapture[model.CaptureID(msg.From)] = *req.Watermark
	}

	var status []scheduler.InferiorStatus
	for _, info := range req.Statuses {
		status = append(status, &ReplicaSetStatus{
			ID: &common.TableSpan{
				TableSpan: info.Span,
			},
			State: info.ComponentStatus,
		})
	}
	msgs, err := m.scheduler.HandleStatus(msg.From.String(), req.Statuses)
	if err != nil {
		log.Error("handle status failed, ignore",
			zap.String("changefeed", m.id.ID),
			zap.Error(err))
		return errors.Trace(err)
	}
	m.sendMessages(msgs)
	// set error if not nil, todo: only save one
	m.errLock.Lock()
	if req.Warning != nil {
		m.runningWarnings[msg.From] = req.Warning
	}
	if req.Err != nil {
		m.runningErrors[msg.From] = req.Err
	}
	m.errLock.Unlock()
	return nil
}

func (m *Maintainer) onMaintainerBootstrapResponse(msg *messaging.TargetMessage) error {
	log.Info("received maintainer bootstrap response",
		zap.String("changefeed", m.id.ID),
		zap.String("server", msg.From.String()))
	m.scheduler.AddNewNode(msg.From.String())
	cachedResp := m.bootstrapper.HandleBootstrapResponse(msg.From, msg.Message[0].(*heartbeatpb.MaintainerBootstrapResponse))
	return m.onBootstrapDone(cachedResp)
}

func (m *Maintainer) onBootstrapDone(cachedResp map[common.NodeID]*heartbeatpb.MaintainerBootstrapResponse) error {
	if cachedResp == nil {
		return nil
	}
	log.Info("all nodes have sent bootstrap response",
		zap.String("changefeed", m.id.ID),
		zap.Int("size", len(cachedResp)))
	var status scheduler.InferiorStatus
	for server, bootstrapMsg := range cachedResp {
		for _, info := range bootstrapMsg.Statuses {
			status = &ReplicaSetStatus{
				ID: &common.TableSpan{
					TableSpan: info.Span,
				},
				State:        info.ComponentStatus,
				CheckpointTs: info.CheckpointTs,
			}
			stm, err := scheduler.NewStateMachine(status.GetInferiorID(),
				map[model.CaptureID]scheduler.InferiorStatus{server: status},
				NewReplicaSet(m.id, status.GetInferiorID(), info.CheckpointTs))
			if err != nil {
				return errors.Trace(err)
			}

			//working on remote
			if stm.State != scheduler.SchedulerStatusAbsent {
				span := stm.ID.(*common.TableSpan)
				m.scheduler.working.ReplaceOrInsert(span, stm)
				m.scheduler.absent.Delete(stm.ID.(*common.TableSpan))
				m.scheduler.nodeTasks[stm.Primary].ReplaceOrInsert(span, stm)
				if m.scheduler.schedulingTask.Has(span) {
					log.Warn("span state not expected, remove from commiting",
						zap.String("changefeed", m.id.ID),
						zap.String("span", span.String()))
					m.scheduler.schedulingTask.Delete(stm.ID.(*common.TableSpan))
				}
			}
		}
	}
	return errors.Trace(m.scheduleTableSpan())
}

// initTableIDs get tables ids base on the filter and checkpoint ts
func (m *Maintainer) initTableIDs() ([]common.TableID, error) {
	startTs := m.watermark.CheckpointTs
	f, err := filter.NewFilter(m.config.Config, "")
	if err != nil {
		return nil, errors.Cause(err)
	}

	schemaStore := appcontext.GetService[schemastore.SchemaStore](appcontext.SchemaStore)
	tableIDs, err := schemaStore.GetAllPhysicalTables(startTs, f)
	log.Info("get table ids", zap.Int("count", len(tableIDs)), zap.String("changefeed", m.id.String()))
	return tableIDs, nil
}

func (m *Maintainer) onNodeClosed(from string, response *heartbeatpb.MaintainerCloseResponse) {
	if response.Success {
		m.nodesClosed[from] = struct{}{}
	}
	// check if all nodes have sent response
	m.onRemoveMaintainer(m.cascadeRemoving)
}

func (m *Maintainer) onBalance() {
	msgs, err := m.scheduler.TryBalance()
	if err != nil {
		m.handleError(err)
	}
	m.sendMessages(msgs)
}

func (m *Maintainer) handleResendMessage() {
	// resend scheduling message
	m.sendMessages(m.scheduler.ResendMessage())
	// resend bootstrap message
	m.sendMessages(m.bootstrapper.ResendBootstrapMessage())
	// resend closing message
	if m.removing {
		m.sendMaintainerCloseRequestToAllNode()
	}
}

func (m *Maintainer) tryCloseChangefeed() bool {
	if m.state != heartbeatpb.ComponentState_Stopped {
		m.statusChanged.Store(true)
	}
	if !m.cascadeRemoving {
		return true
	}
	return m.sendMaintainerCloseRequestToAllNode()
}

func (m *Maintainer) sendMaintainerCloseRequestToAllNode() bool {
	msgs := make([]*messaging.TargetMessage, 0)
	for node := range m.nodeManager.GetAliveNodes() {
		if _, ok := m.nodesClosed[node]; !ok {
			msgs = append(msgs, messaging.NewSingleTargetMessage(
				messaging.ServerId(node),
				messaging.DispatcherManagerManagerTopic,
				&heartbeatpb.MaintainerCloseRequest{
					ChangefeedID: m.id.ID,
				}))
		}
	}
	m.sendMessages(msgs)
	return len(msgs) == 0
}

func (m *Maintainer) GetMaintainerStatus() *heartbeatpb.MaintainerStatus {
	// todo: fix data race here
	m.errLock.Lock()
	defer m.errLock.Unlock()
	var runningErrors []*heartbeatpb.RunningError
	if len(m.runningErrors) > 0 {
		runningErrors = make([]*heartbeatpb.RunningError, 0, len(m.runningErrors))
		for _, e := range m.runningErrors {
			runningErrors = append(runningErrors, e)
		}
	}
	var runningWarnings []*heartbeatpb.RunningError
	if len(m.runningWarnings) > 0 {
		runningWarnings = make([]*heartbeatpb.RunningError, 0, len(m.runningWarnings))
		for _, e := range m.runningWarnings {
			runningWarnings = append(runningWarnings, e)
		}
	}

	status := &heartbeatpb.MaintainerStatus{
		ChangefeedID: m.id.ID,
		FeedState:    string(m.changefeedSate),
		State:        m.state,
		CheckpointTs: m.watermark.CheckpointTs,
		Warning:      runningWarnings,
		Err:          runningErrors,
	}
	m.runningWarnings = make(map[messaging.ServerId]*heartbeatpb.RunningError)
	m.runningErrors = make(map[messaging.ServerId]*heartbeatpb.RunningError)
	return status
}

func (m *Maintainer) Handle(event *Event) (await bool) {
	if m.state == heartbeatpb.ComponentState_Stopped {
		log.Warn("maintainer is not stopped, ignore",
			zap.String("changefeed", m.id.String()))
		return false
	}
	// first check the online/offline nodes
	if m.nodeChanged.Load() {
		if err := m.onNodeChanged(); err != nil {
			m.handleError(err)
			return false
		}
		m.nodeChanged.Store(false)
	}
	switch event.eventType {
	case EventInit:
		// already initialized
		if m.initialized {
			return false
		}
		// async initialize the changefeed
		//go func() {
		err := m.initialize()
		if err != nil {
			m.handleError(err)
		}
		//m.stream.Wake() <- event.cfID
		//log.Info("stream waked", zap.String("changefeed", m.id.String()))
		//}()
		return false
	case EventMessage:
		if err := m.onMessage(event.message); err != nil {
			m.handleError(err)
		}
	case EventSchedule:
		if err := m.scheduleTableSpan(); err != nil {
			m.handleError(err)
		}
	case EventRemove:
		m.onRemoveMaintainer(m.cascadeRemoving)
	case EventPeriod:
		m.handlePeriodTask()
	}
	return false
}

func (m *Maintainer) handleError(err error) {
	log.Error("an error occurred in Owner",
		zap.String("changefeed", m.id.ID), zap.Error(err))
	var code string
	if rfcCode, ok := errors.RFCCode(err); ok {
		code = string(rfcCode)
	} else {
		code = string(errors.ErrOwnerUnknown.RFCCode())
	}
	m.runningErrors = map[messaging.ServerId]*heartbeatpb.RunningError{
		messaging.ServerId(m.selfNode.ID): {
			Time:    time.Now().String(),
			Node:    m.selfNode.AdvertiseAddr,
			Code:    code,
			Message: err.Error(),
		},
	}
	m.statusChanged.Store(true)
}

// getNewBootstrapFn returns a function that creates a new bootstrap message to initialize
// a changefeed dispatcher manager.
func (m *Maintainer) getNewBootstrapFn() scheduler.NewBootstrapFn {
	cfg := m.config
	changefeedConfig := configNew.ChangefeedConfig{
		Namespace:      cfg.Namespace,
		ID:             cfg.ID,
		StartTS:        cfg.StartTs,
		TargetTS:       cfg.TargetTs,
		SinkURI:        cfg.SinkURI,
		ForceReplicate: cfg.Config.ForceReplicate,
		SinkConfig:     cfg.Config.Sink,
		Filter:         cfg.Config.Filter,
		// other fileds are not necessary for maintainer
	}
	// cfgBytes only holds necessary fields to initialize a changefeed dispatcher.
	cfgBytes, err := json.Marshal(changefeedConfig)
	if err != nil {
		log.Panic("marshal changefeed config failed",
			zap.String("changefeed", m.id.ID),
			zap.Error(err))
	}
	return func(captureID model.CaptureID) *messaging.TargetMessage {
		log.Info("send maintainer bootstrap message",
			zap.String("changefeed", m.id.String()),
			zap.String("server", captureID))
		return messaging.NewSingleTargetMessage(
			messaging.ServerId(captureID),
			messaging.DispatcherManagerManagerTopic,
			&heartbeatpb.MaintainerBootstrapRequest{
				ChangefeedID: m.id.ID,
				Config:       cfgBytes,
			})
	}

}

func (m *Maintainer) handlePeriodTask() {
	m.printStatus()
	m.handleResendMessage()
	m.calCheckpointTs()
	m.onBalance()
	submitScheduledEvent(m.taskScheduler, m.stream, &Event{
		cfID:      m.id.ID,
		eventType: EventPeriod,
	}, time.Now().Add(time.Millisecond*500))
}

func (m *Maintainer) printStatus() {
	if time.Since(m.lastPrintStatusTime) > time.Second*60 {
		tableStates := make(map[scheduler.SchedulerStatus]int)
		for i := 0; i <= 3; i++ {
			tableStates[scheduler.SchedulerStatus(i)] = 0
		}
		total := m.scheduler.absent.Len() + m.scheduler.schedulingTask.Len() + m.scheduler.working.Len()
		tableStates[scheduler.SchedulerStatusWorking] = m.scheduler.working.Len()
		tableStates[scheduler.SchedulerStatusAbsent] = m.scheduler.absent.Len()
		m.scheduler.schedulingTask.Ascend(func(key *common.TableSpan, value *scheduler.StateMachine) bool {
			if _, ok := tableStates[value.State]; !ok {
				tableStates[value.State] = 0
			}
			tableStates[value.State]++
			return true
		})

		m.tableCountGauge.Set(float64(total))
		m.scheduledTaskGauge.Set(float64(total - m.scheduler.absent.Len()))
		for state, count := range tableStates {
			metrics.TableStateGauge.WithLabelValues(m.id.Namespace, m.id.ID, state.String()).Set(float64(count))
		}

		var taskDistribution string
		for nodeID, sm := range m.scheduler.nodeTasks {
			taskDistribution = fmt.Sprintf("%s, %s=%d", taskDistribution, nodeID, sm.Len())
		}
		log.Info("table span status",
			zap.String("distribution", taskDistribution),
			zap.String("changefeed", m.id.ID),
			zap.Int("total", total),
			zap.Int("absent", tableStates[scheduler.SchedulerStatusAbsent]),
			zap.Int("commit", tableStates[scheduler.SchedulerStatusCommiting]),
			zap.Int("working", tableStates[scheduler.SchedulerStatusWorking]),
			zap.Int("removing", tableStates[scheduler.SchedulerStatusRemoving]),
			zap.Int("schedulingTask", m.scheduler.schedulingTask.Len()))
		m.lastPrintStatusTime = time.Now()
	}
}
