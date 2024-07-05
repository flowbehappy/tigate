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
	"github.com/flowbehappy/tigate/rpc"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"time"
)

// Maintainer is response for handle changefeed replication tasks, Maintainer should:
// 1. schedules tables to ticdc node
// 2. calculate changefeed checkpoint ts
// 3. send changefeed status to coordinator
// 4. handle heartbeat reported by dispatcher
type Maintainer struct {
	id     model.ChangeFeedID
	config *model.ChangeFeedInfo
	status *model.ChangeFeedStatus

	task *dispatchMaintainerTask

	state      scheduler.ComponentStatus
	supervisor *scheduler.Supervisor
	scheduler  scheduler.Scheduler

	taskCh chan Task
}

// NewMaintainer create the maintainer for the changefeed
func NewMaintainer(cfID model.ChangeFeedID) *Maintainer {
	m := &Maintainer{}
	m.supervisor = scheduler.NewSupervisor(
		MaintainerID(cfID),
		NewReplicaSet,
		m.newBootstrapMessage,
	)
	return m
}

func (m *Maintainer) newBootstrapMessage(id model.CaptureID) rpc.Message {
	return nil
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
			if !timer.Stop() {
				<-timer.C
			}
			return threadpool.Running
		}
	}
}

func (m *Maintainer) Release() {
}

func (m *Maintainer) Await() threadpool.TaskStatus {
	return threadpool.Running
}

func (m *Maintainer) GetStatus() threadpool.TaskStatus {
	return threadpool.Running
}

type dispatchTaskStatus int32

const (
	dispatchTaskReceived = dispatchTaskStatus(iota + 1)
	dispatchTaskProcessed
)

type dispatchMaintainerTask struct {
	ID        model.ChangeFeedID
	IsRemove  bool
	IsPrepare bool
	status    dispatchTaskStatus
}

func (m *Maintainer) initChangefeed() error {
	m.state = scheduler.ComponentStatusPrepared
	return nil
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
			m.state = scheduler.ComponentStatusStopped
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
