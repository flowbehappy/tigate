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
	"github.com/flowbehappy/tigate/rpc"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
	"time"
)

// Coordinator is the master of the ticdc cluster,
// 1. schedules changefeed maintainer to ticdc node
// 2. save changefeed checkpoint ts to etcd
// 3. send checkpoint to downstream
// 4. manager gc safe point
// 5. response for open API call
type Coordinator struct {
	rpcClient  rpc.RpcClient
	supervisor *scheduler.Supervisor
	scheduler  scheduler.Scheduler
	tick       *time.Ticker
	taskCh     chan Task
}

func NewCoordinator(rpcClient rpc.RpcClient,
	capture *model.CaptureInfo) *Coordinator {
	c := &Coordinator{
		rpcClient: rpcClient,
		tick:      time.NewTicker(time.Second),
		scheduler: scheduler.NewCombineScheduler(scheduler.NewBasicScheduler(1000)),
	}
	c.supervisor = scheduler.NewSupervisor(
		CoordinatorID(capture.ID),
		NewChangefeed,
		c.newBootstrapMessage,
	)
	return c
}

func (c *Coordinator) Execute(timeout time.Duration) threadpool.TaskStatus {
	timer := time.NewTimer(timeout)
	for {
		select {
		case task := <-c.taskCh:
			if err := task.Execute(); err != nil {
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

func (c *Coordinator) Release() {
}

func (c *Coordinator) Await() threadpool.TaskStatus {
	return threadpool.Running
}

func (c *Coordinator) GetStatus() threadpool.TaskStatus {
	return threadpool.Running
}

func (c *Coordinator) checkLiveness() ([]rpc.Message, error) {
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

func (c *Coordinator) newBootstrapMessage(model.CaptureID) rpc.Message {
	return nil
}

func (c *Coordinator) scheduleMaintainer() ([]rpc.Message, error) {
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

func (c *Coordinator) handleMessages() ([]rpc.Message, error) {
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

type changefeed struct {
	ID    model.ChangeFeedID
	State *ChangefeedStatus

	Info   *model.ChangeFeedInfo
	Status *model.ChangeFeedStatus

	lastHeartBeat time.Time
}

func NewChangefeed(ID scheduler.InferiorID) scheduler.Inferior {
	return &changefeed{}
}

func (c *changefeed) UpdateStatus(status scheduler.InferiorStatus) {
	c.State = status.(*ChangefeedStatus)
	c.lastHeartBeat = time.Now()
}

func (c *changefeed) GetID() scheduler.InferiorID {
	return MaintainerID(c.ID)
}

func (c *changefeed) NewInferiorStatus(status scheduler.ComponentStatus) scheduler.InferiorStatus {
	return &ChangefeedStatus{
		ID:     MaintainerID(c.ID),
		Status: status,
	}
}

func (c *changefeed) IsAlive() bool {
	return time.Now().Sub(c.lastHeartBeat) < 10*time.Second
}

func (c *changefeed) NewAddInferiorMessage(capture model.CaptureID, secondary bool) rpc.Message {
	//return &Message{
	//	To: capture,
	//	DispatchMaintainerRequest: &DispatchMaintainerRequest{
	//		AddMaintainerRequest: &AddMaintainerRequest{
	//			ID:          c.ID,
	//			Config:      c.Info,
	//			Status:      c.Status,
	//			IsSecondary: secondary,
	//		}},
	//}
	return nil
}

func (c *changefeed) NewRemoveInferiorMessage(capture model.CaptureID) rpc.Message {
	//return &Message{
	//	To: capture,
	//	DispatchMaintainerRequest: &DispatchMaintainerRequest{
	//		RemoveMaintainerRequest: &RemoveMaintainerRequest{
	//			ID: c.Info.ID,
	//		},
	//	},
	//}
	return nil
}

type ChangefeedStatus struct {
	ID     MaintainerID
	Status scheduler.ComponentStatus
}

func (c *ChangefeedStatus) GetInferiorID() scheduler.InferiorID {
	return scheduler.InferiorID(c.ID)
}

func (c *ChangefeedStatus) GetInferiorState() scheduler.ComponentStatus {
	return c.Status
}
