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
	"math"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/node"

	"github.com/flowbehappy/tigate/heartbeatpb"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// coordinator implements the Coordinator interface
type coordinator struct {
	nodeInfo    *node.Info
	initialized bool
	version     int64

	// message buf from remote
	msgLock sync.RWMutex
	msgBuf  []*messaging.TargetMessage

	// for log print
	lastCheckTime time.Time

	// scheduling fields
	supervisor *Supervisor

	lastState *orchestrator.GlobalReactorState

	lastSaveTime         time.Time
	lastTickTime         time.Time
	scheduledChangefeeds map[common.MaintainerID]scheduler.Inferior

	gcManager gc.Manager
	pdClient  pd.Client
	pdClock   pdutil.Clock

	mc messaging.MessageCenter
}

func New(node *node.Info,
	pdClient pd.Client,
	pdClock pdutil.Clock,
	serviceID string,
	version int64) node.Coordinator {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	c := &coordinator{
		version:              version,
		nodeInfo:             node,
		scheduledChangefeeds: make(map[common.MaintainerID]scheduler.Inferior),
		lastTickTime:         time.Now(),
		gcManager:            gc.NewManager(serviceID, pdClient, pdClock),
		pdClient:             pdClient,
		pdClock:              pdClock,
		mc:                   mc,
	}
	id := common.CoordinatorID("coordinator")
	c.supervisor = NewSupervisor(
		id,
		c.newChangefeed, c.newBootstrapMessage,
		NewBasicScheduler(id),
		NewBalanceScheduler(id, time.Minute, 1000),
	)

	// receive messages
	mc.RegisterHandler(messaging.CoordinatorTopic, c.recvMessages)
	return c
}

func (c *coordinator) recvMessages(_ context.Context, msg *messaging.TargetMessage) error {
	c.msgLock.Lock()
	c.msgBuf = append(c.msgBuf, msg)
	c.msgLock.Unlock()
	return nil
}

// Tick is the entrance of the coordinator, it will be called by the etcd watcher every 50ms.
//  1. Handle message reported by other modules.
//  2. Check if the node is changed:
//     - if a new node is added, send bootstrap message to that node ,
//     - if a node is removed, clean related state machine that bind to that node.
//  3. Schedule changefeeds if all node is bootstrapped.
func (c *coordinator) Tick(
	ctx context.Context, rawState orchestrator.ReactorState,
) (orchestrator.ReactorState, error) {
	state := rawState.(*orchestrator.GlobalReactorState)
	c.lastState = state

	now := time.Now()
	metrics.CoordinatorCounter.Add(float64(now.Sub(c.lastTickTime)) / float64(time.Second))
	c.lastTickTime = now

	if err := c.updateGCSafepoint(ctx, state); err != nil {
		return nil, errors.Trace(err)
	}

	// 1. handle grpc messages
	err := c.handleMessages()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// 2. check if nodes is changed
	msgs, err := c.supervisor.HandleAliveCaptureUpdate(node.CaptureInfosToNodeInfos(state.Captures))
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.sendMessages(msgs)

	// 3. schedule changefeed maintainer
	msgs = c.scheduleMaintainer(state.Changefeeds)
	c.sendMessages(msgs)

	//4. update checkpoint ts and changefeed states
	c.saveChangefeedStatus()
	// 5. send saved checkpoint ts to maintainer
	c.sendSavedCheckpointTsToMaintainer()

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
			req := msg.Message[0].(*heartbeatpb.CoordinatorBootstrapResponse)
			c.supervisor.UpdateCaptureStatus(msg.From, req.Statuses)
		case messaging.TypeMaintainerHeartbeatRequest:
			if c.supervisor.CheckAllCaptureInitialized() {
				req := msg.Message[0].(*heartbeatpb.MaintainerHeartbeat)
				msgs, err := c.supervisor.HandleStatus(msg.From, req.Statuses)
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

func (c *coordinator) sendMessages(msgs []*messaging.TargetMessage) {
	for _, msg := range msgs {
		err := c.mc.SendCommand(msg)
		if err != nil {
			log.Error("failed to send coordinator request", zap.Any("msg", msg), zap.Error(err))
			continue
		}
	}
}

func (c *coordinator) scheduleMaintainer(
	changefeeds map[model.ChangeFeedID]*orchestrator.ChangefeedReactorState,
) []*messaging.TargetMessage {
	if !c.supervisor.CheckAllCaptureInitialized() {
		return nil
	}
	// check all changefeeds.
	for id, cfState := range changefeeds {
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
			_, ok := c.scheduledChangefeeds[common.MaintainerID(id.ID)]
			if !ok {
				c.scheduledChangefeeds[common.MaintainerID(id.ID)] = &changefeed{}
			}
		} else {
			// changefeed is stopped
			delete(c.scheduledChangefeeds, common.MaintainerID(id.ID))
		}
	}
	c.supervisor.MarkNeedAddInferior()
	c.supervisor.MarkNeedRemoveInferior()
	return c.supervisor.Schedule(c.scheduledChangefeeds)
}

func (c *coordinator) newBootstrapMessage(id node.ID) *messaging.TargetMessage {
	log.Info("send coordinator bootstrap request", zap.Any("to", id))
	return messaging.NewSingleTargetMessage(
		id,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.CoordinatorBootstrapRequest{Version: c.version})
}

func (c *coordinator) newChangefeed(id common.MaintainerID) scheduler.Inferior {
	cfID := model.DefaultChangeFeedID(id.String())
	cfInfo := c.lastState.Changefeeds[cfID]
	cf := newChangefeed(c, cfID, cfInfo.Info, cfInfo.Status.CheckpointTs)
	c.scheduledChangefeeds[id] = cf
	return cf
}

func (c *coordinator) saveChangefeedStatus() {
	if time.Since(c.lastSaveTime) > time.Millisecond*500 {
		for key, value := range c.scheduledChangefeeds {
			id := model.DefaultChangeFeedID(key.String())
			cfState, ok := c.lastState.Changefeeds[id]
			if !ok {
				continue
			}
			cf := value.(*changefeed)
			if cf.Status == nil {
				continue
			}
			if !shouldRunChangefeed(model.FeedState(cf.Status.FeedState)) {
				cfState.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
					info.State = model.FeedState(cf.Status.FeedState)
					return info, true, nil
				})
			}
			updateStatus(cfState, cf.Status.CheckpointTs)
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
			if len(cf.Status.Err) > 0 {
				for _, err := range cf.Status.Err {
					saveErrorFn(err)
				}
			}
			if len(cf.Status.Warning) > 0 {
				for _, err := range cf.Status.Warning {
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

func (c *coordinator) updateGCSafepoint(
	ctx context.Context, state *orchestrator.GlobalReactorState,
) error {
	minCheckpointTs, forceUpdate := c.calculateGCSafepoint(state)
	// When the changefeed starts up, CDC will do a snapshot read at
	// (checkpointTs - 1) from TiKV, so (checkpointTs - 1) should be an upper
	// bound for the GC safepoint.
	gcSafepointUpperBound := minCheckpointTs - 1
	err := c.gcManager.TryUpdateGCSafePoint(ctx, gcSafepointUpperBound, forceUpdate)
	return errors.Trace(err)
}

// calculateGCSafepoint calculates GCSafepoint for different upstream.
// Note: we need to maintain a TiCDC service GC safepoint for each upstream TiDB cluster
// to prevent upstream TiDB GC from removing data that is still needed by TiCDC.
// GcSafepoint is the minimum checkpointTs of all changefeeds that replicating a same upstream TiDB cluster.
func (c *coordinator) calculateGCSafepoint(state *orchestrator.GlobalReactorState) (
	uint64, bool,
) {
	var minCpts uint64 = math.MaxUint64
	var forceUpdate = false

	for changefeedID, changefeedState := range state.Changefeeds {
		if changefeedState.Info == nil || !changefeedState.Info.NeedBlockGC() {
			continue
		}
		checkpointTs := changefeedState.Info.GetCheckpointTs(changefeedState.Status)
		if minCpts > checkpointTs {
			minCpts = checkpointTs
		}
		// Force update when adding a new changefeed.
		_, exist := c.scheduledChangefeeds[common.MaintainerID(changefeedID.ID)]
		if !exist {
			forceUpdate = true
		}
	}
	// check if the upstream has a changefeed, if not we should update the gc safepoint
	if minCpts == math.MaxUint64 {
		ts := c.pdClock.CurrentTime()
		minCpts = oracle.GoTimeToTS(ts)
	}
	return minCpts, forceUpdate
}

func (c *coordinator) sendSavedCheckpointTsToMaintainer() {
	for key, value := range c.scheduledChangefeeds {
		cf := value.(*changefeed)
		if !cf.isMQSink || cf.stateMachine == nil || cf.stateMachine.Primary == "" {
			continue
		}
		id := model.DefaultChangeFeedID(key.String())
		cfState, ok := c.lastState.Changefeeds[id]
		if !ok || cfState.Status == nil {
			continue
		}
		if cf.lastSavedCheckpointTs < cfState.Status.CheckpointTs {
			msg := cf.NewCheckpointTsMessage(cfState.Status.CheckpointTs)
			c.sendMessages([]*messaging.TargetMessage{msg})
			cf.lastSavedCheckpointTs = cfState.Status.CheckpointTs
		}
	}
}

func (c *coordinator) printStatus() {
	if time.Since(c.lastCheckTime) > time.Second*10 {
		workingTask := 0
		absentTask := 0
		commitTask := 0
		removingTask := 0
		for _, value := range c.supervisor.StateMachines {
			switch value.State {
			case scheduler.SchedulerStatusAbsent:
				absentTask++
			case scheduler.SchedulerStatusCommiting:
				commitTask++
			case scheduler.SchedulerStatusWorking:
				workingTask++
			case scheduler.SchedulerStatusRemoving:
				removingTask++
			}
		}
		log.Info("changefeed status",
			zap.Int("absent", absentTask),
			zap.Int("commit", commitTask),
			zap.Int("working", workingTask),
			zap.Int("removing", removingTask),
			zap.Any("runningTask", len(c.supervisor.RunningTasks)),
		)
		c.lastCheckTime = time.Now()
	}
}
