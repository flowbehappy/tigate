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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"go.uber.org/zap"
)

const maintainerMangerTopic = "maintainer-manager"

// Coordinator is the master of the ticdc cluster,
// 1. schedules changefeed maintainer to ticdc watcher
// 2. save changefeed checkpoint ts to etcd
// 3. send checkpoint to downstream
// 4. manager gc safe point
// 5. response for open API call
type Coordinator interface {
	AsyncStop()
	GetNodeInfo() *model.CaptureInfo
}

type coordinator struct {
	messageCenter messaging.MessageCenter

	nodeInfo    *model.CaptureInfo
	ID          model.CaptureID
	initialized bool
	version     int64

	// message buf from remote
	msgLock sync.RWMutex
	msgBuf  []*messaging.TargetMessage

	lastCheckTime time.Time

	// scheduling fields
	scheduler          Scheduler
	stateMachines      map[model.ChangeFeedID]*StateMachine
	runningTasks       map[model.ChangeFeedID]*ScheduleTask
	maxTaskConcurrency int

	captures map[model.CaptureID]*ServerStatus
	// track all status reported by remote when bootstrap
	initStatus map[model.CaptureID][]*heartbeatpb.MaintainerStatus
}

func NewCoordinator(capture *model.CaptureInfo,
	messageCenter messaging.MessageCenter,
	version int64) Coordinator {
	c := &coordinator{
		scheduler: NewCombineScheduler(
			NewBasicScheduler(1000),
			NewBalanceScheduler(time.Minute, 1000)),
		messageCenter:      messageCenter,
		version:            version,
		nodeInfo:           capture,
		stateMachines:      make(map[model.ChangeFeedID]*StateMachine),
		runningTasks:       map[model.ChangeFeedID]*ScheduleTask{},
		initialized:        false,
		captures:           make(map[model.CaptureID]*ServerStatus),
		initStatus:         make(map[model.CaptureID][]*heartbeatpb.MaintainerStatus),
		maxTaskConcurrency: 10000,
	}
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
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("%d", i)
		allChangefeeds[model.DefaultChangeFeedID(id)] = &model.ChangeFeedInfo{
			ID:      id,
			Config:  config.GetDefaultReplicaConfig(),
			SinkURI: "blackhole://",
		}
	}
}

func (c *coordinator) Tick(ctx context.Context,
	rawState orchestrator.ReactorState) (orchestrator.ReactorState, error) {
	state := rawState.(*orchestrator.GlobalReactorState)
	if time.Since(c.lastCheckTime) > time.Second*10 {
		workingTask := 0
		prepareTask := 0
		absentTask := 0
		commitTask := 0
		removingTask := 0
		for _, value := range c.stateMachines {
			switch value.State {
			case SchedulerStatusAbsent:
				absentTask++
			case SchedulerStatusPrepare:
				prepareTask++
			case SchedulerStatusCommit:
				commitTask++
			case SchedulerStatusWorking:
				workingTask++
			case SchedulerStatusRemoving:
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

	err := c.handleMessages()
	if err != nil {
		return nil, errors.Trace(err)
	}

	msgs, err := c.HandleAliveCaptureUpdate(state.Captures)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(msgs) > 0 {
		c.sendMessages(msgs)
	}

	allChangefeedID := make([]model.ChangeFeedID, 0)
	// check all changefeeds.
	for _, reactor := range allChangefeeds {
		changefeedID := model.DefaultChangeFeedID(reactor.ID)
		_, exist := c.GetChangefeedStateMachine(changefeedID)
		if !exist {
			// check if changefeed should be running
			if !shouldRunChangefeed(reactor.State) {
				continue
			}
			allChangefeedID = append(allChangefeedID, changefeedID)
		} else {
			if shouldRunChangefeed(reactor.State) {
				allChangefeedID = append(allChangefeedID, changefeedID)
			}
		}
	}

	msgs, err = c.scheduleMaintainer(allChangefeedID)
	if err != nil {
		return state, err
	}
	c.sendMessages(msgs)
	return state, nil
}

func (c *coordinator) handleMessages() error {
	c.msgLock.Lock()
	buf := c.msgBuf
	c.msgBuf = nil
	c.msgLock.Unlock()
	for _, msg := range buf {
		switch msg.Type {
		case messaging.TypeCoordinatorBootstrapResponse:
			req := msg.Message.(*messaging.CoordinatorBootstrapResponse)
			c.cacheBootstrapResponse(msg.From.String(), req.Statuses)
		case messaging.TypeMaintainerHeartbeatRequest:
			if c.CheckAllCaptureInitialized() {
				req := msg.Message.(*messaging.MaintainerHeartbeat)
				serverID := msg.From
				msgs, err := c.HandleStatus(serverID.String(), req.Statuses)
				if err != nil {
					log.Error("handle status failed", zap.Error(err))
					return errors.Trace(err)
				}
				c.sendMessages(msgs)
			}
		default:
			log.Panic("unexpected message", zap.Any("message", msg))
		}
	}
	return nil
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
	if !c.CheckAllCaptureInitialized() {
		return nil, nil
	}
	tasks := c.scheduler.Schedule(
		allInferiors,
		c.captures,
		c.stateMachines,
	)
	return c.HandleScheduleTasks(tasks)
}
