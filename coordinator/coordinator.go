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
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"go.uber.org/zap"
)

// coordinator implements the Coordinator interface
type coordinator struct {
	nodeInfo    *model.CaptureInfo
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

	lastState *orchestrator.GlobalReactorState

	lastSaveTime         time.Time
	scheduledChangefeeds map[model.ChangeFeedID]*changefeed
}

func NewCoordinator(capture *model.CaptureInfo,
	version int64) server.Coordinator {
	c := &coordinator{
		scheduler: scheduler.NewCombineScheduler(
			scheduler.NewBasicScheduler(1000),
			scheduler.NewBalanceScheduler(time.Minute, 1000)),
		version:              version,
		nodeInfo:             capture,
		scheduledChangefeeds: make(map[model.ChangeFeedID]*changefeed),
	}

	c.supervisor = scheduler.NewSupervisor(
		scheduler.ChangefeedID(model.DefaultChangeFeedID("coordinator")),
		c.newChangefeed, c.newBootstrapMessage,
	)
	// receive messages
	appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).
		RegisterHandler(messaging.CoordinatorTopic, func(msg *messaging.TargetMessage) error {
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
	c.lastState = state

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
	msgs, err = c.scheduleMaintainer(state)
	if err != nil {
		return state, err
	}
	c.sendMessages(msgs)

	//4. update checkpoint ts and changefeed states
	c.saveChangefeedStatus()

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
	switch state {
	case model.StateStopped, model.StateFailed, model.StateFinished:
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

func (c *coordinator) scheduleMaintainer(state *orchestrator.GlobalReactorState) ([]rpc.Message, error) {
	if !c.supervisor.CheckAllCaptureInitialized() {
		return nil, nil
	}
	allChangefeedID := make([]scheduler.InferiorID, 0)
	// check all changefeeds.
	for id, reactor := range state.Changefeeds {
		if reactor.Info == nil {
			continue
		}
		if !preflightCheck(reactor) {
			log.Info("precheck failed ignored",
				zap.String("id", id.String()))
			continue
		}
		if shouldRunChangefeed(reactor.Info.State) {
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
		"maintainer-manager",
		&heartbeatpb.CoordinatorBootstrapRequest{Version: c.version})
}

func (c *coordinator) newChangefeed(id scheduler.InferiorID) scheduler.Inferior {
	cfID := model.ChangeFeedID(id.(scheduler.ChangefeedID))
	cfInfo := c.lastState.Changefeeds[cfID]
	cf := newChangefeed(c, cfID, cfInfo.Info, cfInfo.Status.CheckpointTs)
	c.scheduledChangefeeds[cfInfo.ID] = cf
	return cf
}

func (c *coordinator) saveChangefeedStatus() {
	if time.Since(c.lastSaveTime) > time.Millisecond*500 {
		for id, cf := range c.scheduledChangefeeds {
			status := c.lastState.Changefeeds[id]
			if shouldRunChangefeed(model.FeedState(cf.State.FeedState)) {
				status.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
					info.State = model.FeedState(cf.State.FeedState)
					return info, true, nil
				})
			}
			updateStatus(status, cf.checkpointTs)
		}
		c.lastSaveTime = time.Now()
	}
}

// preflightCheck makes sure that the metadata in Etcd is complete enough to run the tick.
// If the metadata is not complete, such as when the ChangeFeedStatus is nil,
// this function will reconstruct the lost metadata and skip this tick.
func preflightCheck(changefeed *orchestrator.ChangefeedReactorState) (ok bool) {
	ok = true
	if changefeed.Status == nil {
		// complete the changefeed status when it is just created.
		changefeed.PatchStatus(
			func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
				if status == nil {
					status = &model.ChangeFeedStatus{
						// changefeed status is nil when the changefeed has just created.
						CheckpointTs:      changefeed.Info.StartTs,
						MinTableBarrierTs: changefeed.Info.StartTs,
						AdminJobType:      model.AdminNone,
					}
					return status, true, nil
				}
				return status, false, nil
			})
		ok = false
	} else if changefeed.Status.MinTableBarrierTs == 0 {
		// complete the changefeed status when the TiCDC cluster is
		// upgraded from an old version(less than v6.7.0).
		changefeed.PatchStatus(
			func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
				if status != nil {
					if status.MinTableBarrierTs == 0 {
						status.MinTableBarrierTs = status.CheckpointTs
					}
					return status, true, nil
				}
				return status, false, nil
			})
		ok = false
	}

	if !ok {
		log.Info("changefeed preflight check failed, will skip this tick",
			zap.String("namespace", changefeed.ID.Namespace),
			zap.String("changefeed", changefeed.ID.ID),
			zap.Any("status", changefeed.Status), zap.Bool("ok", ok),
		)
	}

	return
}

func updateStatus(changefeed *orchestrator.ChangefeedReactorState,
	checkpointTs uint64,
) {
	if checkpointTs == 0 {
		return
	}
	changefeed.PatchStatus(
		func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			changed := false
			if status == nil {
				return nil, false, nil
			}
			if status.CheckpointTs != checkpointTs {
				status.CheckpointTs = checkpointTs
				changed = true
			}
			return status, changed, nil
		})
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
