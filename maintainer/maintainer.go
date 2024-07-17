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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	appctx "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/rpc"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry/schema"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/spanz"
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
	cfgBytes []byte

	checkpointTs uint64

	state      heartbeatpb.ComponentState
	supervisor *scheduler.Supervisor
	scheduler  scheduler.Scheduler

	changefeedSate model.FeedState

	taskCh  chan Task
	removed *atomic.Bool

	initialized  bool
	tableIDs     map[int64]struct{}
	allInferiors []scheduler.InferiorID

	pdEndpoints []string
	nodeManager *watcher.NodeManager

	statusChanged  *atomic.Bool
	lastReportTime time.Time

	removing    *atomic.Bool
	isSecondary *atomic.Bool

	msgLock sync.RWMutex
	msgBuf  []*messaging.TargetMessage

	lastCheckTime time.Time
}

// NewMaintainer create the maintainer for the changefeed
func NewMaintainer(cfID model.ChangeFeedID,
	isSecondary bool,
	cfg *model.ChangeFeedInfo,
	cfgBytes []byte,
	checkpointTs uint64,
) *Maintainer {
	m := &Maintainer{
		id: cfID,
		scheduler: scheduler.NewCombineScheduler(
			scheduler.NewBasicScheduler(1000),
			scheduler.NewBalanceScheduler(time.Minute, 1000)),
		state:         heartbeatpb.ComponentState_Prepared,
		removed:       atomic.NewBool(false),
		taskCh:        make(chan Task, 1024),
		nodeManager:   appctx.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		statusChanged: atomic.NewBool(true),
		isSecondary:   atomic.NewBool(isSecondary),
		removing:      atomic.NewBool(false),
		config:        cfg,
		cfgBytes:      cfgBytes,
		checkpointTs:  checkpointTs,
	}
	if !isSecondary {
		m.state = heartbeatpb.ComponentState_Working
	}
	m.supervisor = scheduler.NewSupervisor(scheduler.ChangefeedID(cfID),
		func(id scheduler.InferiorID) scheduler.Inferior {
			return NewReplicaSet(m.id, id)
		}, m.bootstrapMessage)
	return m
}

func (m *Maintainer) Execute() (threadpool.TaskStatus, time.Time) {
	// removing, cancel the task
	if m.removing.Load() {
		m.closeChangefeed()
		return threadpool.Done, time.Time{}
	}
	// init the maintainer, todo: return error and cancel the maintainer
	if !m.initialized {
		if err := m.initChangefeed(); err != nil {
			log.Warn("init changefeed failed", zap.Error(err))
			m.initialized = false
			m.removed.Store(true)
			m.state = heartbeatpb.ComponentState_Stopped
			return threadpool.Done, time.Time{}
		}
		m.initialized = true
	}

	// not on the primary status, skip running
	if m.isSecondary.Load() {
		return threadpool.CPUTask, time.Now().Add(50 * time.Millisecond)
	}
	m.state = heartbeatpb.ComponentState_Working

	// handle messages
	if err := m.handleMessages(); err != nil {
		return threadpool.CPUTask, time.Now().Add(50 * time.Millisecond)
	}

	nodes := m.nodeManager.GetAliveCaptures()
	//check capture changes
	msgs, err := m.supervisor.HandleAliveCaptureUpdate(nodes)
	if err != nil {
		log.Warn("handle capture failed", zap.Error(err))
		return threadpool.CPUTask, time.Now().Add(50 * time.Millisecond)
	}
	m.sendMessages(msgs)

	// resend dispatcher message
	m.resendSchedulerMessage()

	// try to schedule table spans
	err = m.scheduleTableSpan()
	if err != nil {
		log.Warn("handle capture failed", zap.Error(err))
		return threadpool.CPUTask, time.Now().Add(50 * time.Millisecond)
	}
	m.printStatus()
	return threadpool.CPUTask, time.Now().Add(50 * time.Millisecond)
}

func (m *Maintainer) handleMessages() error {
	m.msgLock.Lock()
	buf := m.msgBuf
	m.msgBuf = nil
	m.msgLock.Unlock()
	for _, msg := range buf {
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
	}
	return nil
}

// send message to remote, todo: use a io thread pool
func (m *Maintainer) sendMessages(msgs []rpc.Message) {
	for _, msg := range msgs {
		err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendCommand(msg.(*messaging.TargetMessage))
		if err != nil {
			log.Error("failed to send coordinator request", zap.Any("msg", msg), zap.Error(err))
			continue
		}
	}
}

func (m *Maintainer) scheduleTableSpan() error {
	if !m.supervisor.CheckAllCaptureInitialized() {
		return nil
	}

	tasks := m.scheduler.Schedule(
		m.allInferiors,
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
}

func (m *Maintainer) initChangefeed() error {
	m.state = heartbeatpb.ComponentState_Prepared
	m.statusChanged.Store(true)
	var err error
	m.tableIDs = map[int64]struct{}{
		int64(1): {},
		int64(2): {},
		int64(3): {},
	}
	for id, _ := range m.tableIDs {
		start, end := spanz.GetTableRange(id)
		m.allInferiors = append(m.allInferiors, &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{
			TableID:  uint64(id),
			StartKey: start,
			EndKey:   end,
		}})
	}
	return err
}

func (m *Maintainer) onHeartBeatRequest(msg *messaging.TargetMessage) error {
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
			State: info.ComponentStatus,
		})
	}
	m.supervisor.UpdateCaptureStatus(msg.From.String(), status)
}

func (m *Maintainer) resendSchedulerMessage() {
	var msgs []rpc.Message
	m.supervisor.StateMachines.Ascend(func(key scheduler.InferiorID, value *scheduler.StateMachine) bool {
		if value.State == scheduler.SchedulerStatusPrepare &&
			time.Since(value.LastMsgTime) > time.Millisecond*200 {
			server, _ := value.GetRole(scheduler.RoleSecondary)
			msg, err := value.HandleInferiorStatus(&ReplicaSetStatus{State: heartbeatpb.ComponentState_Absent}, server)
			if err != nil {
				log.Error("poll failed", zap.Error(err))
			}
			msgs = append(msgs, msg)
			value.LastMsgTime = time.Now()
		}
		return true
	})
	m.sendMessages(msgs)
}

// GetTableIDs get tables ids base on the filter and checkpoint ts
func (m *Maintainer) GetTableIDs() (map[int64]struct{}, error) {
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

	meta := kv.GetSnapshotMeta(kvStore, startTs)
	snap, err := schema.NewSnapshotFromMeta(
		model.ChangeFeedID4Test("api", "verifyTable"),
		meta, startTs, false /* explicitTables */, f)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableIDs := make(map[int64]struct{})
	snap.IterTables(true, func(tableInfo *model.TableInfo) {
		if f.ShouldIgnoreTable(tableInfo.TableName.Schema, tableInfo.TableName.Table) {
			return
		}
		if tableInfo.IsSequence() {
			return
		}
		tableIDs[tableInfo.ID] = struct{}{}
	})
	return tableIDs, nil
}

func (m *Maintainer) finishAddChangefeed() {
	m.state = heartbeatpb.ComponentState_Working
	m.statusChanged.Store(true)
}

func (m *Maintainer) closeChangefeed() {
	if m.state != heartbeatpb.ComponentState_Stopping &&
		m.state != heartbeatpb.ComponentState_Stopped {
		m.state = heartbeatpb.ComponentState_Stopping
		m.statusChanged.Store(true)
		//todo: real async close
		go func() {
			// send message to dispatcher manager
			m.state = heartbeatpb.ComponentState_Stopped
			m.statusChanged.Store(true)
			m.removed.Store(true)
		}()
	}
}

func (m *Maintainer) GetMaintainerStatus() *heartbeatpb.MaintainerStatus {
	// todo: fix data race here
	return &heartbeatpb.MaintainerStatus{
		ChangefeedID: m.id.ID,
		FeedState:    string(m.changefeedSate),
		State:        m.state,
		CheckpointTs: m.checkpointTs,
	}
}

func (m *Maintainer) bootstrapMessage(captureID model.CaptureID) rpc.Message {
	return messaging.NewTargetMessage(
		messaging.ServerId(captureID),
		messaging.MaintainerBoostrapRequestTopic,
		&heartbeatpb.MaintainerBootstrapRequest{
			ChangefeedID: m.id.ID,
			Config:       m.cfgBytes,
		})
}

func (m *Maintainer) printStatus() {
	if time.Since(m.lastCheckTime) > time.Second*10 {
		workingTask := 0
		prepareTask := 0
		absentTask := 0
		commitTask := 0
		removingTask := 0
		var taskDistribution string
		m.supervisor.StateMachines.Ascend(func(key scheduler.InferiorID, value *scheduler.StateMachine) bool {
			switch value.State {
			case scheduler.SchedulerStatusAbsent:
				absentTask++
			case scheduler.SchedulerStatusPrepare:
				prepareTask++
			case scheduler.SchedulerStatusCommit:
				commitTask++
			case scheduler.SchedulerStatusWorking:
				workingTask++
			case scheduler.SchedulerStatusRemoving:
				removingTask++
			}
			span := key.(*common.TableSpan)
			taskDistribution = fmt.Sprintf("%s, %d==>%s", taskDistribution, span.TableID, value.Primary)
			return true
		})

		log.Info("table span status",
			zap.String("distribution", taskDistribution),
			zap.String("changefeed", m.id.ID),
			zap.Int("absent", absentTask),
			zap.Int("prepare", prepareTask),
			zap.Int("commit", commitTask),
			zap.Int("working", workingTask),
			zap.Int("removing", removingTask))
		m.lastCheckTime = time.Now()
	}
}
