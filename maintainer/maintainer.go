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
	"strings"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
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
	id     model.ChangeFeedID
	config *model.ChangeFeedInfo
	status *model.ChangeFeedStatus
	taskID threadpool.TaskId

	messageCenter messaging.MessageCenter

	state      scheduler.ComponentStatus
	supervisor *Supervisor
	scheduler  Scheduler

	changefeedSate model.FeedState

	taskCh  chan Task
	removed *atomic.Bool

	tableIDs map[int64]struct{}

	pdEndpoints []string
	nodeManager *watcher.NodeManager

	statusChanged  *atomic.Bool
	lastReportTime time.Time

	removing    *atomic.Bool
	isSecondary *atomic.Bool

	msgLock  sync.RWMutex
	msgBuf   []*messaging.TargetMessage
	msgTopic string

	lastCheckTime time.Time
}

// NewMaintainer create the maintainer for the changefeed
func NewMaintainer(cfID model.ChangeFeedID,
	center messaging.MessageCenter,
	isSecondary bool,
) *Maintainer {
	m := &Maintainer{
		id: cfID,
		scheduler: NewCombineScheduler(
			NewBasicScheduler(1000),
			NewBalanceScheduler(time.Minute, 1000)),
		taskID:        threadpool.NewGlobalTaskId(),
		messageCenter: center,
		state:         scheduler.ComponentStatusPrepared,
		removed:       atomic.NewBool(false),
		taskCh:        make(chan Task, 1024),
		nodeManager:   appctx.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		statusChanged: atomic.NewBool(true),
		isSecondary:   atomic.NewBool(isSecondary),
		removing:      atomic.NewBool(false),
		supervisor:    NewSupervisor(cfID),
		msgTopic:      "maintainer/" + cfID.ID,
	}
	if !isSecondary {
		m.state = scheduler.ComponentStatusWorking
	}
	// receive messages
	center.RegisterHandler(m.msgTopic, func(msg *messaging.TargetMessage) error {
		if m.isSecondary.Load() {
			return nil
		}
		m.msgLock.Lock()
		m.msgBuf = append(m.msgBuf, msg)
		m.msgLock.Unlock()
		return nil
	})
	return m
}

func (m *Maintainer) newBootstrapMessage(id model.CaptureID) rpc.Message {
	return &heartbeatpb.MaintainerBootstrapRequest{
		Id: id,
	}
}

func (m *Maintainer) Execute(status threadpool.TaskStatus) (threadpool.TaskStatus, time.Time) {
	// removing, cancel the task
	if m.removing.Load() {
		m.closeChangefeed()
		return threadpool.Done, time.Time{}
	}
	// not on the primary status, skip running
	if m.isSecondary.Load() {
		return status, time.Now().Add(50 * time.Millisecond)
	}
	m.state = scheduler.ComponentStatusWorking

	// handle messages
	if err := m.handleMessages(); err != nil {
		return status, time.Now().Add(50 * time.Millisecond)
	}

	nodes := m.nodeManager.GetAliveCaptures()
	//check capture changes
	msgs, err := m.supervisor.HandleAliveCaptureUpdate(nodes)
	if err != nil {
		log.Warn("handle capture failed", zap.Error(err))
		return status, time.Now().Add(50 * time.Millisecond)
	}
	if len(msgs) > 0 {
		m.sendMessages(msgs)
	}

	// try to schedule table spans
	msgs, err = m.scheduleTableSpan()
	if err != nil {
		log.Warn("handle capture failed", zap.Error(err))
		return status, time.Now().Add(50 * time.Millisecond)
	}
	m.sendMessages(msgs)
	m.printStatus()
	return status, time.Now().Add(50 * time.Millisecond)
}

func (m *Maintainer) TaskId() threadpool.TaskId {
	return m.taskID
}

func (m *Maintainer) handleMessages() error {
	m.msgLock.Lock()
	buf := m.msgBuf
	m.msgBuf = nil
	m.msgLock.Unlock()
	for _, msg := range buf {
		switch msg.Type {
		case messaging.TypeHeartBeatResponse:
			req := msg.Message.(*messaging.HeartBeatResponse)
			var status []scheduler.InferiorStatus
			for _, info := range req.Info {
				status = append(status, &ReplicaSetStatus{
					ID: &common.TableSpan{
						TableSpan: info.Span,
					},
					Status: scheduler.ComponentStatus(info.SchedulerStatus),
				})
			}
			msgs, err := m.supervisor.HandleStatus(msg.From.String(), status)
			if err != nil {
				log.Error("handle status failed, ignore", zap.Error(err))
				return errors.Trace(err)
			}
			m.sendMessages(msgs)
		case messaging.TypeMaintainerBootstrapResponse:
			req := msg.Message.(*messaging.MaintainerBootstrapResponse)
			var status []scheduler.InferiorStatus
			for _, info := range req.Statuses {
				status = append(status, &ReplicaSetStatus{
					ID: &common.TableSpan{
						TableSpan: info.Span,
					},
					Status: scheduler.ComponentStatus(info.SchedulerStatus),
				})
			}
			m.supervisor.UpdateCaptureStatus(msg.From.String(), status)
		default:
			log.Panic("unexpected message type", zap.String("type", msg.Type.String()))
		}
	}
	return nil
}

func (m *Maintainer) sendMessages(msgs []rpc.Message) {
	for _, msg := range msgs {
		err := m.messageCenter.SendCommand(msg.(*messaging.TargetMessage))
		if err != nil {
			log.Error("failed to send coordinator request", zap.Any("msg", msg), zap.Error(err))
			continue
		}
	}
}

var allInferiors []scheduler.InferiorID

func init() {
	for i := 0; i < 3; i++ {
		tblID := int64(100 + i)
		start, end := spanz.GetTableRange(tblID)
		allInferiors = append(allInferiors, &common.TableSpan{
			TableSpan: &heartbeatpb.TableSpan{
				TableID:  uint64(tblID),
				StartKey: start,
				EndKey:   end,
			}})
	}
}

func (m *Maintainer) scheduleTableSpan() ([]rpc.Message, error) {
	if !m.supervisor.CheckAllCaptureInitialized() {
		return nil, nil
	}
	tasks := m.scheduler.Schedule(
		allInferiors,
		m.supervisor.GetAllCaptures(),
		m.supervisor.GetInferiors(),
	)
	return m.supervisor.HandleScheduleTasks(tasks)
}

// Close cleanup resources
func (m *Maintainer) Close() {
	m.messageCenter.DeRegisterHandler(m.msgTopic)
}

func (m *Maintainer) initChangefeed() error {
	m.state = scheduler.ComponentStatusPrepared
	m.statusChanged.Store(true)
	return nil
}

// GetTableIDs get tables ids base on the filter and checkpoint ts
func (m *Maintainer) GetTableIDs() (map[int64]struct{}, error) {
	startTs := m.status.CheckpointTs
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
	m.state = scheduler.ComponentStatusWorking
	m.statusChanged.Store(true)
}

func (m *Maintainer) closeChangefeed() {
	if m.state != scheduler.ComponentStatusStopping &&
		m.state != scheduler.ComponentStatusStopped {
		m.state = scheduler.ComponentStatusStopping
		m.statusChanged.Store(true)
		//todo: real async close
		go func() {
			// send message to dispatcher manager
			m.state = scheduler.ComponentStatusStopped
			m.statusChanged.Store(true)
			m.removed.Store(true)
		}()
	}
}

func (m *Maintainer) GetMaintainerStatus() *heartbeatpb.MaintainerStatus {
	// todo: fix data race here
	return &heartbeatpb.MaintainerStatus{
		ChangefeedID:    m.id.ID,
		FeedState:       string(m.changefeedSate),
		SchedulerStatus: int32(m.state),
		CheckpointTs:    0,
	}
}

func (m *Maintainer) printStatus() {
	if time.Since(m.lastCheckTime) > time.Second*10 {
		workingTask := 0
		prepareTask := 0
		absentTask := 0
		commitTask := 0
		removingTask := 0
		m.supervisor.stateMachines.Ascend(func(key scheduler.InferiorID, value *StateMachine) bool {
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
			return true
		})

		log.Info("table span status",
			zap.String("changefeed", m.id.ID),
			zap.Int("absent", absentTask),
			zap.Int("prepare", prepareTask),
			zap.Int("commit", commitTask),
			zap.Int("working", workingTask),
			zap.Int("removing", removingTask))
		m.lastCheckTime = time.Now()
	}
}
