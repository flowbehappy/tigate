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
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/common/server"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/flowbehappy/tigate/pkg/rpc"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/utils"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"go.uber.org/zap"
)

// coordinator implements the Coordinator interface
type coordinator struct {
	nodeInfo    *common.NodeInfo
	initialized bool
	version     int64

	// message buf from remote
	msgLock sync.RWMutex
	msgBuf  []*messaging.TargetMessage

	// for log print
	lastCheckTime time.Time

	// scheduling fields
	supervisor *scheduler.Supervisor

	lastState *orchestrator.GlobalReactorState

	lastSaveTime         time.Time
	lastTickTime         time.Time
	scheduledChangefeeds map[model.ChangeFeedID]*changefeed
}

func NewCoordinator(capture *common.NodeInfo, version int64) server.Coordinator {
	c := &coordinator{
		version:              version,
		nodeInfo:             capture,
		scheduledChangefeeds: make(map[model.ChangeFeedID]*changefeed),
		lastTickTime:         time.Now(),
	}
	id := scheduler.ChangefeedID(model.DefaultChangeFeedID("coordinator"))
	c.supervisor = scheduler.NewSupervisor(
		id,
		c.newChangefeed, c.newBootstrapMessage,
		scheduler.NewBasicScheduler(id),
	)

	// receive messages
	appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).
		RegisterHandler(messaging.CoordinatorTopic, func(_ context.Context, msg *messaging.TargetMessage) error {
			c.msgLock.Lock()
			c.msgBuf = append(c.msgBuf, msg)
			c.msgLock.Unlock()
			return nil
		})
	return c
}

// Tick is the entrance of the coordinator, it will be called by the etcd watcher every 50ms.
//  1. Handle message reported by other modules.
//  2. Check if the node is changed:
//     - if a new node is added, send bootstrap message to that node ,
//     - if a node is removed, clean related state machine that binded to that node.
//  3. Schedule changefeeds if all node is bootstrapped.
func (c *coordinator) Tick(
	ctx context.Context, rawState orchestrator.ReactorState,
) (orchestrator.ReactorState, error) {
	state := rawState.(*orchestrator.GlobalReactorState)
	c.lastState = state

	now := time.Now()
	metrics.CoordinatorCounter.Add(float64(now.Sub(c.lastTickTime)) / float64(time.Second))
	c.lastTickTime = now

	// 1. handle grpc messages
	err := c.handleMessages()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// 2. check if nodes is changed
	msgs, err := c.supervisor.HandleAliveCaptureUpdate(common.CaptureInfosToNodeInfos(state.Captures))
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
				msgs, err := c.supervisor.HandleStatus(msg.From.String(), statues)
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
	allChangefeeds := utils.NewBtreeMap[scheduler.InferiorID, scheduler.Inferior]()
	// check all changefeeds.
	for id, cfState := range state.Changefeeds {
		if cfState.Info == nil {
			continue
		}
		if !preflightCheck(cfState) {
			log.Error("precheck failed ignored",
				zap.String("id", id.String()))
			continue
		}
		if shouldRunChangefeed(cfState.Info.State) {
			// todo use real changefeed instance here
			cf, ok := c.scheduledChangefeeds[id]
			if !ok {
				cf = &changefeed{}
			}
			allChangefeeds.ReplaceOrInsert(scheduler.ChangefeedID(id), cf)
		}
	}
	c.supervisor.MarkNeedAddInferior()
	c.supervisor.MarkNeedRemoveInferior()
	return c.supervisor.Schedule(allChangefeeds)
}

func (c *coordinator) newBootstrapMessage(captureID model.CaptureID) rpc.Message {
	return messaging.NewTargetMessage(
		messaging.ServerId(captureID),
		messaging.MaintainerManagerTopic,
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
			cfState, ok := c.lastState.Changefeeds[id]
			if !ok {
				continue
			}
			if !shouldRunChangefeed(model.FeedState(cf.State.FeedState)) {
				cfState.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
					info.State = model.FeedState(cf.State.FeedState)
					return info, true, nil
				})
			}
			updateStatus(cfState, cf.checkpointTs)
			saveErrorFn := func(err *heartbeatpb.RunningError) {
				node, ok := c.lastState.Captures[err.Node]
				addr := err.Node
				if ok {
					addr = node.AdvertiseAddr
				}
				cfState.PatchTaskPosition(err.Node,
					func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
						if position == nil {
							position = &model.TaskPosition{}
						}
						position.Error = &model.RunningError{
							//Time:    err.Time, //todo: save time
							Addr:    addr,
							Code:    err.Code,
							Message: err.Message,
						}
						return position, true, nil
					})
			}
			if len(cf.State.Err) > 0 {
				for _, err := range cf.State.Err {
					saveErrorFn(err)
				}
			}
			if len(cf.State.Warning) > 0 {
				for _, err := range cf.State.Warning {
					saveErrorFn(err)
				}
			}
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

func updateStatus(
	changefeed *orchestrator.ChangefeedReactorState,
	checkpointTs uint64,
) {
	if checkpointTs == 0 || changefeed == nil {
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
			zap.Int("removing", removingTask),
			zap.Any("runningTask", c.supervisor.RunningTasks.Len()),
		)
		c.lastCheckTime = time.Now()
	}
}
