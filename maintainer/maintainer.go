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
	"math"
	"strings"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	appctx "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/flowbehappy/tigate/rpc"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/flowbehappy/tigate/utils"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry/schema"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
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
	id     model.ChangeFeedID
	config *model.ChangeFeedInfo

	checkpointTs          *atomic.Uint64
	checkpointTsByCapture map[model.CaptureID]uint64

	state      heartbeatpb.ComponentState
	supervisor *scheduler.Supervisor

	changefeedSate model.FeedState

	taskCh  chan Task
	removed *atomic.Bool

	initialized bool
	// tableSpans track all table spans that need to be scheduled
	// when dispatcher reported a new table or remove a table, this field should be updated
	tableSpans utils.Map[scheduler.InferiorID, scheduler.Inferior]

	pdEndpoints []string
	nodeManager *watcher.NodeManager

	statusChanged  *atomic.Bool
	lastReportTime time.Time

	removing        *atomic.Bool
	cascadeRemoving *atomic.Bool
	isSecondary     *atomic.Bool

	msgQueue *MessageQueue
	msgBuf   []*messaging.TargetMessage

	lastResendTime       time.Time
	lastPrintStatusTime  time.Time
	lastCheckpointTsTime time.Time

	errLock         sync.Mutex
	runningErrors   map[messaging.ServerId]*heartbeatpb.RunningError
	runningWarnings map[messaging.ServerId]*heartbeatpb.RunningError

	changefeedCheckpointTsGauge    prometheus.Gauge
	changefeedCheckpointTsLagGauge prometheus.Gauge
	changefeedStatusGauge          prometheus.Gauge
	scheduleredTaskGuage           prometheus.Gauge
	runningTaskGuage               prometheus.Gauge
	tableCountGauge                prometheus.Gauge
}

// NewMaintainer create the maintainer for the changefeed
func NewMaintainer(cfID model.ChangeFeedID,
	isSecondary bool,
	cfg *model.ChangeFeedInfo,
	checkpointTs uint64,
	pdEndpoints []string,
) *Maintainer {
	m := &Maintainer{
		id:                    cfID,
		state:                 heartbeatpb.ComponentState_Prepared,
		removed:               atomic.NewBool(false),
		taskCh:                make(chan Task, 1024),
		nodeManager:           appctx.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		statusChanged:         atomic.NewBool(true),
		isSecondary:           atomic.NewBool(isSecondary),
		removing:              atomic.NewBool(false),
		cascadeRemoving:       atomic.NewBool(false),
		config:                cfg,
		checkpointTs:          atomic.NewUint64(checkpointTs),
		checkpointTsByCapture: make(map[model.CaptureID]uint64),
		pdEndpoints:           pdEndpoints,
		tableSpans:            utils.NewBtreeMap[scheduler.InferiorID, scheduler.Inferior](),
		msgQueue:              NewMessageQueue(1024),
		msgBuf:                make([]*messaging.TargetMessage, 1024),
		runningErrors:         map[messaging.ServerId]*heartbeatpb.RunningError{},
		runningWarnings:       map[messaging.ServerId]*heartbeatpb.RunningError{},

		changefeedCheckpointTsGauge:    metrics.ChangefeedCheckpointTsGauge.WithLabelValues(cfID.Namespace, cfID.ID),
		changefeedCheckpointTsLagGauge: metrics.ChangefeedCheckpointTsLagGauge.WithLabelValues(cfID.Namespace, cfID.ID),
		changefeedStatusGauge:          metrics.ChangefeedStatusGauge.WithLabelValues(cfID.Namespace, cfID.ID),
		scheduleredTaskGuage:           metrics.ScheduleTaskGuage.WithLabelValues(cfID.Namespace, cfID.ID),
		runningTaskGuage:               metrics.RunningScheduleTaskGauge.WithLabelValues(cfID.Namespace, cfID.ID),
		tableCountGauge:                metrics.TableGauge.WithLabelValues(cfID.Namespace, cfID.ID),
	}
	if !isSecondary {
		m.state = heartbeatpb.ComponentState_Working
	}
	m.supervisor = scheduler.NewSupervisor(scheduler.ChangefeedID(cfID),
		m.getReplicaSet, m.getNewBootstrapFn(),
		scheduler.NewBasicScheduler(1000),
		scheduler.NewBalanceScheduler(time.Minute, 1000),
	)
	log.Info("create maintainer", zap.String("id", cfID.String()))
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

func (m *Maintainer) Execute() (taskStatus threadpool.TaskStatus, tick time.Time) {
	log.Info("maintainer execute", zap.String("id", m.id.String()))
	m.updateMetrics()
	if m.removed.Load() {
		// removed, cancel the task
		return threadpool.Done, time.Time{}
	}

	taskStatus = threadpool.CPUTask
	tick = time.Now().Add(50 * time.Millisecond)
	if m.removing.Load() {
		//todo: send message to dispatcher manager if changefeed is removed
		m.closeChangefeed()
		if m.supervisor.GetInferiors().Len() == 0 {
			m.removed.Store(true)
			m.state = heartbeatpb.ComponentState_Stopped
			return threadpool.Done, time.Time{}
		} else {
			log.Error("maintainer is removing, but still has tasks",
				zap.Any("tasks", m.supervisor.GetInferiors().Len()))
		}
	}

	// init the maintainer, todo: return error and cancel the maintainer
	if !m.initialized {
		if err := m.initChangefeed(); err != nil {
			log.Warn("init changefeed failed",
				zap.String("id", m.id.String()),
				zap.Error(err))
			m.initialized = false
			m.removing.Store(true)
			return
		}
		m.initialized = true
	}

	// not on the primary status, skip running
	if m.isSecondary.Load() {
		return
	}
	m.state = heartbeatpb.ComponentState_Working

	// handle messages
	if err := m.handleMessages(); err != nil {
		return
	}

	nodes := m.nodeManager.GetAliveCaptures()
	//check capture changes
	msgs, err := m.supervisor.HandleAliveCaptureUpdate(nodes)
	if err != nil {
		log.Warn("handle capture failed", zap.Error(err))
		return
	}
	m.sendMessages(msgs)

	// calc checkpointTs before generate new tasks
	m.calCheckpointTs()

	// resend dispatcher message
	m.resendSchedulerMessage()

	// try to schedule table spans
	err = m.scheduleTableSpan()
	if err != nil {
		log.Warn("handle capture failed", zap.Error(err))
		return
	}
	m.printStatus()
	return
}

func (m *Maintainer) getReplicaSet(id scheduler.InferiorID) scheduler.Inferior {
	tableSpan, _ := m.tableSpans.Get(id.(*common.TableSpan))
	return tableSpan
}

func (m *Maintainer) handleMessages() error {
	size := m.msgQueue.PopMessages(m.msgBuf, len(m.msgBuf))
	for idx := 0; idx < size; idx++ {
		msg := m.msgBuf[idx]
		if err := m.onMessage(msg); err != nil {
			return errors.Trace(err)
		}

		m.msgBuf[idx] = nil
	}
	return nil
}

func (m *Maintainer) onMessage(msg *messaging.TargetMessage) error {
	switch msg.Type {
	case messaging.TypeHeartBeatRequest:
		if err := m.onHeartBeatRequest(msg); err != nil {
			return errors.Trace(err)
		}
	case messaging.TypeMaintainerBootstrapResponse:
		m.onMaintainerBootstrapResponse(msg)
	default:
		log.Panic("unexpected message type", zap.String("type", msg.Type.String()))
	}
	return nil
}

func (m *Maintainer) calCheckpointTs() {
	if m.tableSpans.Len() != m.supervisor.GetInferiors().Len() || time.Since(m.lastCheckpointTsTime) < 2*time.Second {
		return
	}
	m.lastCheckpointTsTime = time.Now()

	allCaptures := m.supervisor.GetAllCaptures()
	var checkpointTs uint64 = math.MaxUint64
	for c := range allCaptures {
		if _, ok := m.checkpointTsByCapture[c]; !ok {
			log.Debug("checkpointTs can not be advanced, since missing capture heartbeat",
				zap.String("capture", c))
			return
		}
		if m.checkpointTsByCapture[c] < checkpointTs {
			checkpointTs = m.checkpointTsByCapture[c]
		}
	}

	if checkpointTs != math.MaxUint64 {
		m.checkpointTs.Store(checkpointTs)
	}
}

func (m *Maintainer) updateMetrics() {
	phyCkpTs := oracle.ExtractPhysical(m.checkpointTs.Load())
	m.changefeedCheckpointTsGauge.Set(float64(phyCkpTs))

	lag := (oracle.GetPhysical(time.Now()) - phyCkpTs) / 1e3
	m.changefeedCheckpointTsLagGauge.Set(float64(lag))

	m.changefeedStatusGauge.Set(float64(m.state))
}

// send message to remote, todo: use a io thread pool
func (m *Maintainer) sendMessages(msgs []rpc.Message) {
	for _, msg := range msgs {
		err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendCommand(msg.(*messaging.TargetMessage))
		if err != nil {
			log.Debug("failed to send maintainer request", zap.Any("msg", msg), zap.Error(err))
			continue
		}
	}
}

func (m *Maintainer) scheduleTableSpan() error {
	if !m.supervisor.CheckAllCaptureInitialized() {
		return nil
	}

	tasks := m.supervisor.Schedule(
		m.tableSpans,
		m.supervisor.GetAllCaptures(),
		m.supervisor.GetInferiors(),
	)
	msg, err := m.supervisor.HandleScheduleTasks(tasks)
	if err != nil {
		return errors.Trace(err)
	}
	m.sendMessages(msg)
	return nil
}

// Close cleanup resources
func (m *Maintainer) Close() {
	m.cleanupMetrics()
	log.Info("changefeed maintainer closed", zap.String("id", m.id.String()),
		zap.Bool("removed", m.removed.Load()),
		zap.Uint64("checkpointTs", m.checkpointTs.Load()),
		zap.Bool("secondary", m.isSecondary.Load()))
}

func (m *Maintainer) initChangefeed() error {
	m.state = heartbeatpb.ComponentState_Prepared
	m.statusChanged.Store(true)
	var err error
	tableIDs, err := m.initTableIDs()
	for id := range tableIDs {
		span := spanz.TableIDToComparableSpan(id)
		tableSpan := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{
			TableID:  uint64(id),
			StartKey: span.StartKey,
			EndKey:   span.EndKey,
		}}
		replicaSet := NewReplicaSet(m.id, tableSpan, m.checkpointTs.Load()).(*ReplicaSet)
		m.tableSpans.ReplaceOrInsert(tableSpan, replicaSet)
	}
	return err
}

func (m *Maintainer) onHeartBeatRequest(msg *messaging.TargetMessage) error {
	m.checkpointTsByCapture[model.CaptureID(msg.From)] = msg.Message.(*heartbeatpb.HeartBeatRequest).CheckpointTs

	req := msg.Message.(*heartbeatpb.HeartBeatRequest)
	var status []scheduler.InferiorStatus
	for _, info := range req.Statuses {
		status = append(status, &ReplicaSetStatus{
			ID: &common.TableSpan{
				TableSpan: info.Span,
			},
			State: info.ComponentStatus,
		})
	}
	// set error if not nil, todo: only save one
	m.errLock.Lock()
	if req.Warning != nil {
		m.runningWarnings[msg.From] = req.Warning
	}
	if req.Err != nil {
		m.runningErrors[msg.From] = req.Err
	}
	m.errLock.Unlock()

	msgs, err := m.supervisor.HandleStatus(msg.From.String(), status)
	if err != nil {
		log.Error("handle status failed, ignore", zap.Error(err))
		return errors.Trace(err)
	}
	m.sendMessages(msgs)
	return nil
}

func (m *Maintainer) onMaintainerBootstrapResponse(msg *messaging.TargetMessage) {
	req := msg.Message.(*heartbeatpb.MaintainerBootstrapResponse)
	var status []scheduler.InferiorStatus
	for _, info := range req.Statuses {
		status = append(status, &ReplicaSetStatus{
			ID: &common.TableSpan{
				TableSpan: info.Span,
			},
			State:        info.ComponentStatus,
			CheckpointTs: info.CheckpointTs,
		})
	}
	m.supervisor.UpdateCaptureStatus(msg.From.String(), status)
}

func (m *Maintainer) resendSchedulerMessage() {
	if time.Since(m.lastResendTime) < time.Second*20 {
		return
	}
	var msgs []rpc.Message
	m.supervisor.RunningTasks.Ascend(func(key scheduler.InferiorID, _ *scheduler.ScheduleTask) bool {
		value, ok := m.supervisor.StateMachines.Get(key)
		if !ok {
			log.Panic("table span not found", zap.String("changefeedID", m.id.String()), zap.Any("key", key))
		}
		if time.Since(value.LastMsgTime) < time.Millisecond*200 {
			return true
		}
		if value.State == scheduler.SchedulerStatusPrepare {
			server, _ := value.GetRole(scheduler.RoleSecondary)
			status := value.Inferior.NewInferiorStatus(heartbeatpb.ComponentState_Absent)
			msg, err := value.HandleInferiorStatus(status, server)
			if err != nil {
				log.Error("poll failed", zap.Error(err))
			}
			msgs = append(msgs, msg...)
			value.LastMsgTime = time.Now()
		} else if value.State == scheduler.SchedulerStatusCommit {
			server, _ := value.GetRole(scheduler.RolePrimary)
			status := value.Inferior.NewInferiorStatus(heartbeatpb.ComponentState_Prepared)
			msg, err := value.HandleInferiorStatus(status, server)
			if err != nil {
				log.Error("poll failed", zap.Error(err))
			}
			msgs = append(msgs, msg...)
			value.LastMsgTime = time.Now()
		}
		return true
	})
	m.sendMessages(msgs)
}

// initTableIDs get tables ids base on the filter and checkpoint ts
func (m *Maintainer) initTableIDs() (map[int64]struct{}, error) {
	startTs := m.checkpointTs
	f, err := filter.NewFilter(m.config.Config, "")
	if err != nil {
		return nil, errors.Cause(err)
	}

	cfg := config.GetGlobalServerConfig()
	kvStore, err := kv.CreateTiStore(strings.Join(m.pdEndpoints, ","), cfg.Security)
	if err != nil {
		return nil, errors.Trace(err)
	}

	meta := kv.GetSnapshotMeta(kvStore, startTs.Load())
	snap, err := schema.NewSnapshotFromMeta(
		model.ChangeFeedID4Test("api", "verifyTable"),
		meta, startTs.Load(), false /* explicitTables */, f)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableIDs := make(map[int64]struct{})
	snap.IterTables(true, func(tableInfo *model.TableInfo) {
		if f.ShouldIgnoreTable(tableInfo.TableName.Schema, tableInfo.TableName.Table) {
			return
		}
		// TODO: remove this line when the filter can filter out system table
		if tableInfo.Name.O == "mysql" || tableInfo.Name.O == "sys" {
			return
		}
		if tableInfo.IsSequence() {
			return
		}
		tableIDs[tableInfo.ID] = struct{}{}
	})
	log.Info("get table ids", zap.Int("count", len(tableIDs)), zap.String("changefeed", m.id.String()))
	return tableIDs, nil
}

func (m *Maintainer) finishAddChangefeed() {
	m.state = heartbeatpb.ComponentState_Working
	m.statusChanged.Store(true)
}

func (m *Maintainer) closeChangefeed() {
	if m.state != heartbeatpb.ComponentState_Stopping && m.state != heartbeatpb.ComponentState_Stopped {
		m.state = heartbeatpb.ComponentState_Stopping
		m.statusChanged.Store(true)
		m.tableSpans = utils.NewBtreeMap[scheduler.InferiorID, scheduler.Inferior]()
	}
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
		CheckpointTs: m.checkpointTs.Load(),
		Warning:      runningWarnings,
		Err:          runningErrors,
	}
	m.runningWarnings = make(map[messaging.ServerId]*heartbeatpb.RunningError)
	m.runningErrors = make(map[messaging.ServerId]*heartbeatpb.RunningError)
	return status
}

// getNewBootstrapFn returns a function that creates a new bootstrap message to initialize
// a changefeed dispatcher manager.
func (m *Maintainer) getNewBootstrapFn() scheduler.NewBootstrapFn {
	cfg := m.config
	changefeedConfig := model.ChangefeedConfig{
		Namespace:      cfg.Namespace,
		ID:             cfg.ID,
		StartTS:        cfg.StartTs,
		TargetTS:       cfg.TargetTs,
		SinkURI:        cfg.SinkURI,
		ForceReplicate: cfg.Config.ForceReplicate,
		SinkConfig:     cfg.Config.Sink,
		// other fileds are not necessary for maintainer
	}
	// cfgBytes only holds necessary fields to initialize a changefeed dispatcher.
	cfgBytes, err := json.Marshal(changefeedConfig)
	if err != nil {
		log.Panic("marshal changefeed config failed", zap.Error(err))
	}
	log.Info("create maintainer bootstrap message function", zap.String("changefeed", m.id.String()), zap.ByteString("config", cfgBytes))
	return func(captureID model.CaptureID) rpc.Message {
		return messaging.NewTargetMessage(
			messaging.ServerId(captureID),
			messaging.MaintainerBoostrapRequestTopic,
			&heartbeatpb.MaintainerBootstrapRequest{
				ChangefeedID: m.id.ID,
				Config:       cfgBytes,
			})
	}

}

func (m *Maintainer) getMessageQueue() *MessageQueue {
	return m.msgQueue
}

func (m *Maintainer) printStatus() {
	if time.Since(m.lastPrintStatusTime) > time.Second*120 {
		tableStates := make(map[scheduler.SchedulerStatus]int)
		for i := 0; i <= 5; i++ {
			tableStates[scheduler.SchedulerStatus(i)] = 0
		}
		// var taskDistribution string
		m.supervisor.StateMachines.Ascend(func(key scheduler.InferiorID, value *scheduler.StateMachine) bool {
			if _, ok := tableStates[value.State]; !ok {
				tableStates[value.State] = 0
			}
			tableStates[value.State]++
			// span := key.(*common.TableSpan)
			// taskDistribution = fmt.Sprintf("%s, %d==>%s", taskDistribution, span.TableID, value.Primary)
			return true
		})

		m.tableCountGauge.Set(float64(m.tableSpans.Len()))
		m.scheduleredTaskGuage.Set(float64(m.supervisor.GetInferiors().Len()))
		for state, count := range tableStates {
			metrics.TableStateGauge.WithLabelValues(m.id.Namespace, m.id.ID, state.String()).Set(float64(count))
		}

		log.Info("table span status",
			// zap.String("distribution", taskDistribution),
			zap.String("changefeed", m.id.ID),
			zap.Int("total", m.tableSpans.Len()),
			zap.Int("scheduled", m.supervisor.GetInferiors().Len()),
			zap.Int("absent", tableStates[scheduler.SchedulerStatusAbsent]),
			zap.Int("prepare", tableStates[scheduler.SchedulerStatusPrepare]),
			zap.Int("commit", tableStates[scheduler.SchedulerStatusCommit]),
			zap.Int("working", tableStates[scheduler.SchedulerStatusWorking]),
			zap.Int("removing", tableStates[scheduler.SchedulerStatusRemoving]),
			zap.Int("runningTasks", m.supervisor.RunningTasks.Len()))
		m.lastPrintStatusTime = time.Now()
	}
}
