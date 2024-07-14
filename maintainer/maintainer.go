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
	appctx "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/rpc"
	"github.com/flowbehappy/tigate/scheduler"
	watcher "github.com/flowbehappy/tigate/server/wacher"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/tiflow/cdc/entry/schema"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"go.uber.org/atomic"
)

// Maintainer is response for handle changefeed replication tasks, Maintainer should:
// 1. schedules tables to ticdc wacher
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

	msgLock sync.RWMutex
	msgBuf  []*messaging.TargetMessage
}

// NewMaintainer create the maintainer for the changefeed
func NewMaintainer(cfID model.ChangeFeedID,
	center messaging.MessageCenter,
	isSecondary bool,
) *Maintainer {
	m := &Maintainer{
		id:            cfID,
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
	}
	if !isSecondary {
		m.state = scheduler.ComponentStatusWorking
	}
	// receive messages
	center.RegisterHandler("maintainer/"+m.id.ID, func(msg *messaging.TargetMessage) error {
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
	//todo: handle messages

	//m.msgLock.Lock()
	//buf := m.msgBuf
	//m.msgBuf = nil
	//m.msgLock.Unlock()

	//for _, msg := range buf {
	//	switch msg.Type {
	//	case messaging.TypeHeartBeatResponse:
	//		req := msg.Message.(*messaging.HeartBeatResponse)
	//		m.supervisor.han(msg.From.String(), req.Statuses)
	//	case messaging.TypeMaintainerBootstrapResponse:
	//		req := msg.Message.(*messaging.MaintainerBootstrapResponse)
	//	}
	//}

	return status, time.Now().Add(50 * time.Millisecond)
}

func (m *Maintainer) TaskId() threadpool.TaskId {
	return m.taskID
}

func (m *Maintainer) sendMessages(msgs []rpc.Message) {

}

func (m *Maintainer) scheduleMaintainer(allInferiors []scheduler.InferiorID) ([]rpc.Message, error) {
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

func (m *Maintainer) Release() {
}

func (m *Maintainer) Cancel() {

}

func (m *Maintainer) initChangefeed() error {
	m.state = scheduler.ComponentStatusPrepared
	m.statusChanged.Store(true)
	//tableIds, err := m.GetTableIDs()
	//if err != nil {
	//	return err
	//}
	//m.tableIDs = tableIds
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
