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
	"context"
	"strings"
	"sync/atomic"
	"time"

	appctx "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/rpc"
	"github.com/flowbehappy/tigate/scheduler"
	watcher "github.com/flowbehappy/tigate/server/wacher"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry/schema"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"go.uber.org/zap"
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

	messageCenter messaging.MessageCenter

	task *dispatchMaintainerTask

	state      scheduler.ComponentStatus
	supervisor *scheduler.Supervisor
	scheduler  scheduler.Scheduler

	changefeedSate model.FeedState

	taskCh  chan Task
	removed *atomic.Bool

	tick *time.Ticker

	tableIDs map[int64]struct{}

	pdEndpoints []string
	nodeManager *watcher.NodeManager
}

// NewMaintainer create the maintainer for the changefeed
func NewMaintainer(cfID model.ChangeFeedID, center messaging.MessageCenter) *Maintainer {
	var remove = &atomic.Bool{}
	remove.Store(false)
	m := &Maintainer{
		id:            cfID,
		messageCenter: center,
		state:         scheduler.ComponentStatusAbsent,
		removed:       remove,
		tick:          time.NewTicker(time.Millisecond * 50),
		taskCh:        make(chan Task, 1024),
		nodeManager:   appctx.GetService[*watcher.NodeManager](watcher.NodeManagerName),
	}
	m.supervisor = scheduler.NewSupervisor(
		MaintainerID(cfID),
		NewReplicaSet,
		m.newBootstrapMessage,
	)
	return m
}

func (m *Maintainer) newBootstrapMessage(id model.CaptureID) rpc.Message {
	return &rpc.MaintainerBootstrapRequest{
		To:     uuid.MustParse(id),
		ID:     m.id,
		Config: m.config,
	}
}

func (m *Maintainer) Execute(timeout time.Duration) threadpool.TaskStatus {
	timer := time.NewTimer(timeout)
	for {
		select {
		case task := <-m.taskCh:
			if err := task.Execute(context.Background()); err != nil {
				log.Error("Execute task failed", zap.Error(err))
				return threadpool.Failed
			}
		case <-timer.C:
			return threadpool.Running
		default:
			select {
			case <-m.tick.C:
				// tick
				// check changes
				msgs, removed := m.supervisor.HandleAliveCaptureUpdate(m.nodeManager.GetAliveCaptures())
				if len(msgs) > 0 {
					m.sendMessages(msgs)
				}
				if len(removed) > 0 {
					msgs, err := m.supervisor.HandleCaptureChanges(removed)
					if err != nil {
						log.Warn("handle changes failed", zap.Error(err))
						break
					}
					m.sendMessages(msgs)
				}
			default:
				if !timer.Stop() {
					<-timer.C
				}
			}
			return threadpool.Running
		}
	}
}

func (m *Maintainer) sendMessages(msgs []rpc.Message) {
	for _, msg := range msgs {
		//if dispatchMsg, ok := msg.(*heartbeatpb.ScheduleDispatcherRequest); ok {
		//	log.Info("dispatchMsg", zap.Any("dispatchMsg", dispatchMsg))
		//}

		creq := msg.(*rpc.MaintainerBootstrapRequest)
		buf, err := creq.Encode()
		if err != nil {
			log.Error("failed to encode coordinator request", zap.Any("msg", msg), zap.Error(err))
			continue
		}
		targetMsg := messaging.NewTargetMessage(messaging.ServerId(creq.To),
			"HeartBeatResponse",
			messaging.TypeBytes, buf)
		targetMsg.Topic = "xxxx"
		err = m.messageCenter.SendCommand()
		if err != nil {
			log.Error("failed to send coordinator request", zap.Any("msg", msg), zap.Error(err))
			continue
		}
	}
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

func (m *Maintainer) Await() threadpool.TaskStatus {
	return threadpool.Running
}

func (m *Maintainer) GetStatus() threadpool.TaskStatus {
	return threadpool.Running
}

func (m *Maintainer) SetStatus(status threadpool.TaskStatus) {

}

func (m *Maintainer) Cancel() {

}

type dispatchTaskStatus int32

const (
	dispatchTaskReceived = dispatchTaskStatus(iota + 1)
	dispatchTaskProcessed
)

type dispatchMaintainerTask struct {
	Maintainer *Maintainer
	ID         model.ChangeFeedID
	IsRemove   bool
	IsPrepare  bool
	status     dispatchTaskStatus
}

func (d *dispatchMaintainerTask) Execute(ctx context.Context) error {
	d.Maintainer.injectDispatchTableTask(d)
	if d.IsRemove {
		return d.Maintainer.handleRemoveMaintainerTask()
	}
	return d.Maintainer.handleAddMaintainerTask()
}

func (m *Maintainer) initChangefeed() error {
	m.state = scheduler.ComponentStatusPrepared
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
}

func (m *Maintainer) closeChangefeed() {
	if m.state != scheduler.ComponentStatusStopping &&
		m.state != scheduler.ComponentStatusStopped {
		m.state = scheduler.ComponentStatusStopping
		//todo: real async close
		go func() {
			// send message to dispatcher manager
			m.state = scheduler.ComponentStatusStopped
			m.removed.Store(true)
		}()
	}
}

func (m *Maintainer) handleAddMaintainerTask() error {
	state, _ := m.getAndUpdateMaintainerState(m.state)
	changed := true
	for changed {
		switch state {
		case scheduler.ComponentStatusAbsent:
			err := m.initChangefeed()
			if err != nil {
				log.Warn("add maintainer failed",
					zap.Any("changefeed", m.id),
					zap.Error(err))
				return errors.Trace(err)
			}
			state, changed = m.getAndUpdateMaintainerState(m.state)
		case scheduler.ComponentStatusWorking:
			log.Info("maintainer is working")
			m.task = nil
			return nil
		case scheduler.ComponentStatusPrepared:
			if m.task.IsPrepare {
				// `prepared` is a stable state, if the task was to prepare the table.
				log.Info("maintainer is prepared",
					zap.String("changefeed", m.id.String()))
				m.task = nil
				return nil
			}

			if m.task.status == dispatchTaskReceived {
				m.task.status = dispatchTaskProcessed
			}
			// coordinator send start changefeed command
			m.finishAddChangefeed()
			return nil
		case scheduler.ComponentStatusPreparing:
			return nil
		case scheduler.ComponentStatusStopping,
			scheduler.ComponentStatusStopped:
			log.Warn("ignore add maintainer")
			m.task = nil
			return nil
		default:
			log.Panic("unknown maintainer state")
		}
	}
	return nil
}

func (m *Maintainer) handleRemoveMaintainerTask() error {
	state, _ := m.getAndUpdateMaintainerState(m.state)
	changed := true
	for changed {
		switch state {
		case scheduler.ComponentStatusAbsent:
			log.Warn("remove maintainer, but maintainer is absent, ignore")
			m.task = nil
			return nil
		case scheduler.ComponentStatusStopping,
			scheduler.ComponentStatusStopped:
			log.Warn("remove maintainer, but maintainer is stopping")
			m.task = nil
			return nil
		case scheduler.ComponentStatusPreparing,
			scheduler.ComponentStatusPrepared,
			scheduler.ComponentStatusWorking:
			m.closeChangefeed()
			state, changed = m.getAndUpdateMaintainerState(m.state)
		default:
			log.Panic("unknown maintainer state")
		}
	}
	return nil
}

func (m *Maintainer) injectDispatchTableTask(task *dispatchMaintainerTask) {
	if m.task == nil {
		log.Info("add new task",
			zap.Any("task", task))
		m.task = task
		return
	}
	log.Warn("maintainer inject dispatch task ignored,"+
		"since there is one not finished yet",
		zap.Any("nowTask", m.task),
		zap.Any("ignoredTask", task))
}

func (m *Maintainer) getAndUpdateMaintainerState(old scheduler.ComponentStatus) (scheduler.ComponentStatus, bool) {
	return m.state, m.state != old
}

func (m *Maintainer) GetMaintainerStatus() *rpc.MaintainerStatus {
	// todo: fix data race here
	return &rpc.MaintainerStatus{
		ID:              m.id,
		ChangefeedState: m.changefeedSate,
		SchedulerState:  int(m.state),
		CheckpointTs:    0,
	}
}

// MaintainerID implement the InferiorID interface
type MaintainerID model.ChangeFeedID

func (m MaintainerID) String() string {
	return model.ChangeFeedID(m).String()
}

func (m MaintainerID) Equal(id scheduler.InferiorID) bool {
	return model.ChangeFeedID(m).String() == id.String()
}

func (m MaintainerID) Less(id scheduler.InferiorID) bool {
	return model.ChangeFeedID(m).String() < id.String()
}
