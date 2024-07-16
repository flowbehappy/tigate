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
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/common/server"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/rpc"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"go.uber.org/zap"
)

const maintainerMangerTopic = "maintainer-manager"

// coordinator implements the Coordinator interface
type coordinator struct {
	nodeInfo    *model.CaptureInfo
	ID          model.CaptureID
	initialized bool
	version     int64

	// message buf from remote
	msgLock sync.RWMutex
	msgBuf  []*messaging.TargetMessage

	// for log print
	lastCheckTime time.Time

	// scheduling fields
	scheduler  scheduler.Scheduler
	supervisor *scheduler.Supervisor
}

func NewCoordinator(capture *model.CaptureInfo,
	version int64) server.Coordinator {
	c := &coordinator{
		scheduler: scheduler.NewCombineScheduler(
			scheduler.NewBasicScheduler(1000),
			scheduler.NewBalanceScheduler(time.Minute, 1000)),
		version:  version,
		nodeInfo: capture,
	}

	c.supervisor = scheduler.NewSupervisor(
		scheduler.ChangefeedID(model.DefaultChangeFeedID("coordinator")),
		newChangefeed, c.newBootstrapMessage,
	)
	// receive messages
	appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).RegisterHandler("coordinator", func(msg *messaging.TargetMessage) error {
		c.msgLock.Lock()
		c.msgBuf = append(c.msgBuf, msg)
		c.msgLock.Unlock()
		return nil
	})
	return c
}

// Tick is the entrance of the coordinator, it will be called by the etcd watcher every 50ms.
//  1. Handle message reported by other modules
//  2. check if the node is changed, if a new node is added, send bootstrap message to that node ,
//     or if a node is removed, clean related state machine that binded to that node
//  3. schedule unscheduled changefeeds is all node is bootstrapped
func (c *coordinator) Tick(ctx context.Context,
	rawState orchestrator.ReactorState) (orchestrator.ReactorState, error) {
	state := rawState.(*orchestrator.GlobalReactorState)

	// 1. handle grpc  messages
	err := c.handleMessages()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// 2. check if nodes is changed
	msgs, err := c.supervisor.HandleAliveCaptureUpdate(state.Captures)
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.sendMessages(msgs)

	// 3. schedule changefeed maintainer
	msgs, err = c.scheduleMaintainer()
	if err != nil {
		return state, err
	}
	c.sendMessages(msgs)

	c.printStatus()
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
			req := msg.Message.(*heartbeatpb.CoordinatorBootstrapResponse)
			var statues = make([]scheduler.InferiorStatus, 0, len(req.Statuses))
			for _, status := range req.Statuses {
				statues = append(statues, &MaintainerStatus{status})
			}
			c.supervisor.UpdateCaptureStatus(msg.From.String(), statues)
		case messaging.TypeMaintainerHeartbeatRequest:
			if c.supervisor.CheckAllCaptureInitialized() {
				req := msg.Message.(*heartbeatpb.MaintainerHeartbeat)
				var statues = make([]scheduler.InferiorStatus, 0, len(req.Statuses))
				for _, status := range req.Statuses {
					statues = append(statues, &MaintainerStatus{status})
				}
				serverID := msg.From
				msgs, err := c.supervisor.HandleStatus(serverID.String(), statues)
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

func (c *coordinator) AsyncStop() {
}

func (c *coordinator) sendMessages(msgs []rpc.Message) {
	for _, msg := range msgs {
		err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendCommand(msg.(*messaging.TargetMessage))
		if err != nil {
			log.Error("failed to send coordinator request", zap.Any("msg", msg), zap.Error(err))
			continue
		}
	}
}

func (c *coordinator) scheduleMaintainer() ([]rpc.Message, error) {
	if !c.supervisor.CheckAllCaptureInitialized() {
		return nil, nil
	}
	allChangefeedID := make([]scheduler.InferiorID, 0)
	// check all changefeeds.
	for id, reactor := range allChangefeeds {
		if shouldRunChangefeed(reactor.State) {
			allChangefeedID = append(allChangefeedID, scheduler.ChangefeedID(id))
		}
	}
	tasks := c.scheduler.Schedule(
		allChangefeedID,
		c.supervisor.GetAllCaptures(),
		c.supervisor.StateMachines,
	)
	return c.supervisor.HandleScheduleTasks(tasks)
}

func (c *coordinator) newBootstrapMessage(captureID model.CaptureID) rpc.Message {
	return messaging.NewTargetMessage(
		messaging.ServerId(captureID),
		maintainerMangerTopic,
		&heartbeatpb.CoordinatorBootstrapRequest{Version: c.version})
}

func (c *coordinator) printStatus() {
	if time.Since(c.lastCheckTime) > time.Second*10 {
		workingTask := 0
		prepareTask := 0
		absentTask := 0
		commitTask := 0
		removingTask := 0
		var taskDistribution string
		c.supervisor.StateMachines.Ascend(func(key scheduler.InferiorID, value *scheduler.StateMachine) bool {
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
			taskDistribution = fmt.Sprintf("%s, %s==>%s", taskDistribution, value.ID.String(), value.Primary)
			return true
		})
		log.Info("changefeed status",
			zap.String("distribution", taskDistribution),
			zap.Int("absent", absentTask),
			zap.Int("prepare", prepareTask),
			zap.Int("commit", commitTask),
			zap.Int("working", workingTask),
			zap.Int("removing", removingTask))
		c.lastCheckTime = time.Now()
	}
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
