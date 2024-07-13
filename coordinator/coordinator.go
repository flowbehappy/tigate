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
	"fmt"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/rpc"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"go.uber.org/zap"
)

const maintainerMangerTopic = "maintainer-manager"

// Coordinator is the master of the ticdc cluster,
// 1. schedules changefeed maintainer to ticdc wacher
// 2. save changefeed checkpoint ts to etcd
// 3. send checkpoint to downstream
// 4. manager gc safe point
// 5. response for open API call
type Coordinator interface {
	AsyncStop()
	GetNodeInfo() *model.CaptureInfo
}

type coordinator struct {
	supervisor    *Supervisor
	scheduler     Scheduler
	messageCenter messaging.MessageCenter
	nodeInfo      *model.CaptureInfo

	changefeeds map[model.ChangeFeedID]changefeed

	captures map[model.CaptureID]struct{}

	version int64

	msgLock sync.RWMutex
	msgBuf  []*messaging.TargetMessage

	lastCheckTime time.Time

	dispatchMsgs map[model.CaptureID]*messaging.TargetMessage
}

func NewCoordinator(capture *model.CaptureInfo,
	messageCenter messaging.MessageCenter,
	version int64) Coordinator {
	c := &coordinator{
		scheduler: NewCombineScheduler(
			NewBasicScheduler(1000),
			NewBalanceScheduler(time.Minute, 1000)),
		messageCenter: messageCenter,
		version:       version,
		nodeInfo:      capture,
		dispatchMsgs:  make(map[model.CaptureID]*messaging.TargetMessage),
	}
	c.supervisor = NewSupervisor(
		CoordinatorID(capture.ID),
		c.NewChangefeed,
		c.newBootstrapMessage,
	)
	// receive messages
	messageCenter.RegisterHandler("coordinator", func(msg *messaging.TargetMessage) error {
		c.msgLock.Lock()
		c.msgBuf = append(c.msgBuf, msg)
		c.msgLock.Unlock()
		return nil
	})
	return c
}

var allChangefeeds = make(map[model.ChangeFeedID]*model.ChangeFeedInfo)

func init() {
	for i := 0; i < 500000; i++ {
		id := fmt.Sprintf("%d", i)
		allChangefeeds[model.DefaultChangeFeedID(id)] = &model.ChangeFeedInfo{
			ID: id,
			//Config:  config.GetDefaultReplicaConfig(),
			SinkURI: "blackhole://",
		}
	}
}

func (c *coordinator) Tick(ctx context.Context,
	rawState orchestrator.ReactorState) (orchestrator.ReactorState, error) {
	state := rawState.(*orchestrator.GlobalReactorState)

	if time.Since(c.lastCheckTime) > time.Second*20 {
		workingTask := 0
		prepareTask := 0
		absentTask := 0
		commitTask := 0
		removingTask := 0
		for _, value := range c.supervisor.GetInferiors() {
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
		}

		log.Info("changefeed status",
			zap.Int("absent", absentTask),
			zap.Int("prepare", prepareTask),
			zap.Int("commit", commitTask),
			zap.Int("working", workingTask),
			zap.Int("removing", removingTask))
		c.lastCheckTime = time.Now()
	}

	c.msgLock.Lock()
	buf := c.msgBuf
	c.msgBuf = nil
	c.msgLock.Unlock()
	msgs, err := c.HandleMessage(buf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	msgs, removed := c.supervisor.HandleAliveCaptureUpdate(state.Captures)
	if len(msgs) > 0 {
		c.sendMessages(msgs)
	}
	if len(removed) > 0 {
		msgs, err := c.supervisor.HandleCaptureChanges(removed)
		if err != nil {
			return nil, errors.Trace(err)
		}
		c.sendMessages(msgs)
	}

	changed := false
	allChangefeedID := make([]model.ChangeFeedID, 0)
	// check all changefeeds.
	for _, reactor := range allChangefeeds {
		changefeedID := model.DefaultChangeFeedID(reactor.ID)
		_, exist := c.supervisor.GetInferior(ChangefeedID(changefeedID))
		if !exist {
			// check if changefeed should be running
			if !shouldRunChangefeed(reactor.State) {
				continue
			}
			changed = true
			allChangefeedID = append(allChangefeedID, changefeedID)
		} else {
			if !shouldRunChangefeed(reactor.State) {
				changed = true
			} else {
				allChangefeedID = append(allChangefeedID, changefeedID)
			}
		}
	}

	if changed {
		msgs, err := c.scheduleMaintainer(allChangefeedID)
		if err != nil {
			return state, err
		}
		c.sendMessages(msgs)
	}
	return state, nil
}

func (c *coordinator) HandleMessage(msgs []*messaging.TargetMessage) ([]rpc.Message, error) {
	var rsp []rpc.Message
	for _, msg := range msgs {
		req := msg.Message.(*heartbeatpb.MaintainerHeartbeat)
		serverID := msg.From
		statuses := make([]scheduler.InferiorStatus, 0)
		for _, status := range req.Statuses {
			statuses = append(statuses, &ChangefeedStatus{
				ID:              ChangefeedID(model.DefaultChangeFeedID(status.ChangefeedID)),
				Status:          scheduler.ComponentStatus(status.SchedulerStatus),
				ChangefeedState: model.FeedState(status.FeedState),
				CheckpointTs:    status.CheckpointTs,
			})
		}
		c.supervisor.UpdateCaptureStatus(serverID.String(), statuses)
		if c.supervisor.CheckAllCaptureInitialized() {
			msgs, err := c.supervisor.HandleStatus(serverID.String(), statuses)
			if err != nil {
				log.Error("handle status failed", zap.Error(err))
				return nil, errors.Trace(err)
			}
			c.sendMessages(msgs)
		}
	}
	return rsp, nil
}

func shouldRunChangefeed(state model.FeedState) bool {
	// check if changefeed should be running
	if state == model.StateStopped ||
		state == model.StateFailed ||
		state == model.StateFinished {
		return false
	}
	return true
}

func (c *coordinator) GetNodeInfo() *model.CaptureInfo {
	return c.nodeInfo
}

func (c *coordinator) AsyncStop() {
}

func (c *coordinator) checkLiveness() ([]rpc.Message, error) {
	var msgs []rpc.Message
	for key, value := range c.supervisor.GetInferiors() {
		if !value.Inferior.IsAlive() {
			log.Info("found inactive inferior", zap.Any("ID", key))
			//c.supervisor.GetInferiors().Delete(key)
			// clean messages
			// trigger schedule task
		}
	}
	return msgs, nil
}

func (c *coordinator) newBootstrapMessage(captureID model.CaptureID) rpc.Message {
	return messaging.NewTargetMessage(
		messaging.ServerId(uuid.MustParse(captureID)),
		maintainerMangerTopic,
		messaging.TypeCoordinatorBootstrapRequest,
		&heartbeatpb.CoordinatorBootstrapRequest{Version: c.version})
}

func (c *coordinator) sendMessages(msgs []rpc.Message) {
	for _, msg := range msgs {
		err := c.messageCenter.SendCommand(msg.(*messaging.TargetMessage))
		if err != nil {
			log.Error("failed to send coordinator request", zap.Any("msg", msg), zap.Error(err))
			continue
		}
	}
}

func (c *coordinator) scheduleMaintainer(allInferiors []model.ChangeFeedID) ([]rpc.Message, error) {
	if !c.supervisor.CheckAllCaptureInitialized() {
		return nil, nil
	}
	tasks := c.scheduler.Schedule(
		allInferiors,
		c.supervisor.GetAllCaptures(),
		c.supervisor.GetInferiors(),
	)
	return c.supervisor.HandleScheduleTasks(tasks)
	//rpc, err := c.supervisor.HandleScheduleTasks(tasks)
	//if err != nil {
	//	return nil, err
	//}
	//var msgs []rpc.Message
	//for _, task := range c.dispatchMsgs {
	//	msgs = append(msgs, task)
	//}
	//c.dispatchMsgs = make(map[model.CaptureID]*rpc.CoordinatorRequest)
	//return msgs, nil
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
