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

package coordinator

import (
	"context"
	"time"

	"github.com/flowbehappy/tigate/rpc"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"go.uber.org/zap"
)

// Coordinator is the master of the ticdc cluster,
// 1. schedules changefeed maintainer to ticdc node
// 2. save changefeed checkpoint ts to etcd
// 3. send checkpoint to downstream
// 4. manager gc safe point
// 5. response for open API call
type Coordinator interface {
	AsyncStop()
	GetNodeInfo() *model.CaptureInfo
}

type coordinator struct {
	supervisor *scheduler.Supervisor
	scheduler  scheduler.Scheduler
	nodeInfo   *model.CaptureInfo
	tick       *time.Ticker
	taskCh     chan Task

	version int64
}

func NewCoordinator(capture *model.CaptureInfo,
	version int64) Coordinator {
	c := &coordinator{
		tick:      time.NewTicker(time.Second),
		scheduler: scheduler.NewCombineScheduler(scheduler.NewBasicScheduler(1000)),
		version:   version,
		nodeInfo:  capture,
	}
	c.supervisor = scheduler.NewSupervisor(
		CoordinatorID(capture.ID),
		NewChangefeed,
		c.newBootstrapMessage,
	)
	return c
}

func (c *coordinator) Tick(ctx context.Context,
	state orchestrator.ReactorState) (orchestrator.ReactorState, error) {
	return state, nil
}

func (c *coordinator) GetNodeInfo() *model.CaptureInfo {
	return c.nodeInfo
}

func (c *coordinator) Execute(timeout time.Duration) threadpool.TaskStatus {
	timer := time.NewTimer(timeout)
	for {
		select {
		case task := <-c.taskCh:
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

func (c *coordinator) Release() {
}

func (c *coordinator) Await() threadpool.TaskStatus {
	return threadpool.Running
}

func (c *coordinator) GetStatus() threadpool.TaskStatus {
	return threadpool.Running
}

func (c *coordinator) AsyncStop() {
}

func (c *coordinator) checkLiveness() ([]rpc.Message, error) {
	var msgs []rpc.Message
	c.supervisor.GetInferiors().Ascend(
		func(key scheduler.InferiorID, value *scheduler.StateMachine) bool {
			if !value.Inferior.IsAlive() {
				log.Info("found inactive inferior", zap.Any("ID", key))
				c.supervisor.GetInferiors().Delete(key)
				// clean messages
				// trigger schedule task
			}
			return true
		})
	return msgs, nil
}

func (c *coordinator) newBootstrapMessage(model.CaptureID) rpc.Message {
	return nil
}

func (c *coordinator) scheduleMaintainer() ([]rpc.Message, error) {
	if c.supervisor.CheckAllCaptureInitialized() {
		return nil, nil
	}
	tasks := c.scheduler.Schedule(
		nil,
		nil,
		c.supervisor.GetInferiors(),
	)
	return c.supervisor.HandleScheduleTasks(tasks)
}

func (c *coordinator) handleMessages() ([]rpc.Message, error) {
	var status []scheduler.InferiorStatus
	c.supervisor.UpdateCaptureStatus("", status)
	return c.supervisor.HandleCaptureChanges(nil)
}

type CoordinatorID string

func (c CoordinatorID) String() string {
	return string(c)
}
func (c CoordinatorID) Equal(id scheduler.InferiorID) bool {
	return c.String() == id.String()
}
func (c CoordinatorID) Less(id scheduler.InferiorID) bool {
	return c.String() < id.String()
}
