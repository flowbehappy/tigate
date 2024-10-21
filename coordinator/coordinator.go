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

	"github.com/flowbehappy/tigate/coordinator/changefeed"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/utils/dynstream"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
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

	stream        dynstream.DynamicStream[string, *Event, *Controller]
	taskScheduler threadpool.ThreadPool
	controller    *Controller
}

func (c *coordinator) RemoveChangefeed(ctx context.Context, id model.ChangeFeedID) (uint64, error) {
	return c.controller.RemoveChangefeed(ctx, id)
}

func (c *coordinator) PauseChangefeed(ctx context.Context, id model.ChangeFeedID) error {
	return c.controller.PauseChangefeed(ctx, id)
}

func (c *coordinator) ResumeChangefeed(ctx context.Context, id model.ChangeFeedID, newCheckpointTs uint64) error {
	return c.controller.ResumeChangefeed(ctx, id, newCheckpointTs)
}

func (c *coordinator) UpdateChangefeed(ctx context.Context, change *model.ChangeFeedInfo) error {
	return c.controller.UpdateChangefeed(ctx, change)
}

func New(node *node.Info,
	pdClient pd.Client,
	pdClock pdutil.Clock,
	etcdClient etcd.CDCEtcdClient,
	version int64) node.Coordinator {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	c := &coordinator{
		version:              version,
		nodeInfo:             node,
		scheduledChangefeeds: make(map[common.MaintainerID]scheduler.Inferior),
		lastTickTime:         time.Now(),
		gcManager:            gc.NewManager(etcdClient.GetClusterID(), pdClient, pdClock),
		pdClient:             pdClient,
		pdClock:              pdClock,
		mc:                   mc,
	}
	c.stream = dynstream.NewDynamicStream[string, *Event, *Controller](NewStreamHandler())
	c.stream.Start()
	c.taskScheduler = threadpool.NewThreadPoolDefault()

	backend := changefeed.NewEtcdBackend(etcdClient)
	ctl := NewController(c.version, backend, c.taskScheduler, 1000, time.Minute)
	c.controller = ctl
	if err := c.stream.AddPath("coordinator", ctl); err != nil {
		log.Panic("failed to add path",
			zap.Error(err))
	}
	// receive messages
	mc.RegisterHandler(messaging.CoordinatorTopic, c.recvMessages)
	return c
}

func (c *coordinator) recvMessages(_ context.Context, msg *messaging.TargetMessage) error {
	c.stream.In() <- &Event{message: msg}
	return nil
}

// Run is the entrance of the coordinator, it will be called by the etcd watcher every 50ms.
//  1. Handle message reported by other modules.
//  2. Check if the node is changed:
//     - if a new node is added, send bootstrap message to that node ,
//     - if a node is removed, clean related state machine that bind to that node.
//  3. Schedule changefeeds if all node is bootstrapped.
func (c *coordinator) Run(ctx context.Context) error {
	gcTick := time.NewTicker(time.Minute)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gcTick.C:
			//if err := c.updateGCSafepoint(ctx); err != nil {
			//	log.Warn("update gc safepoint failed",
			//		zap.Error(err))
			//}
			now := time.Now()
			metrics.CoordinatorCounter.Add(float64(now.Sub(c.lastTickTime)) / float64(time.Second))
			c.lastTickTime = now
		}
	}
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

func (c *coordinator) CreateChangefeed(ctx context.Context, info *model.ChangeFeedInfo) error {
	return c.controller.CreateChangefeed(ctx, info)
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

//func (c *coordinator) scheduleMaintainer(
//	changefeeds map[model.ChangeFeedID]*orchestrator.ChangefeedReactorState,
//) []*messaging.TargetMessage {
//	if !c.supervisor.CheckAllCaptureInitialized() {
//		return nil
//	}
//	// check all changefeeds.
//	for id, cfState := range changefeeds {
//		if cfState.Info == nil {
//			continue
//		}
//		if !preflightCheck(cfState) {
//			log.Error("precheck failed ignored",
//				zap.String("id", id.String()))
//			continue
//		}
//		if shouldRunChangefeed(cfState.Info.State) {
//			// todo use real changefeed instance here
//			_, ok := c.scheduledChangefeeds[common.MaintainerID(id.ID)]
//			if !ok {
//				c.scheduledChangefeeds[common.MaintainerID(id.ID)] = &changefeed{}
//			}
//		} else {
//			// changefeed is stopped
//			delete(c.scheduledChangefeeds, common.MaintainerID(id.ID))
//		}
//	}
//	c.supervisor.MarkNeedAddInferior()
//	c.supervisor.MarkNeedRemoveInferior()
//	return c.supervisor.Schedule(c.scheduledChangefeeds)
//}

func (c *coordinator) newBootstrapMessage(id node.ID) *messaging.TargetMessage {
	log.Info("send coordinator bootstrap request", zap.Any("to", id))
	return messaging.NewSingleTargetMessage(
		id,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.CoordinatorBootstrapRequest{Version: c.version})
}

//func (c *coordinator) saveChangefeedStatus() {
//	if time.Since(c.lastSaveTime) > time.Millisecond*500 {
//		for key, value := range c.scheduledChangefeeds {
//			id := model.DefaultChangeFeedID(key.String())
//			cfState, ok := c.lastState.Changefeeds[id]
//			if !ok {
//				continue
//			}
//			cf := value.(*changefeed)
//			if cf.Status == nil {
//				continue
//			}
//			if !shouldRunChangefeed(model.FeedState(cf.Status.FeedState)) {
//				cfState.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
//					info.State = model.FeedState(cf.Status.FeedState)
//					return info, true, nil
//				})
//			}
//			updateStatus(cfState, cf.Status.CheckpointTs)
//			saveErrorFn := func(err *heartbeatpb.RunningError) {
//				node, ok := c.lastState.Captures[err.Node]
//				addr := err.Node
//				if ok {
//					addr = node.AdvertiseAddr
//				}
//				cfState.PatchTaskPosition(err.Node,
//					func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
//						if position == nil {
//							position = &model.TaskPosition{}
//						}
//						position.Error = &model.RunningError{
//							//Time:    err.Time, //todo: save time
//							Addr:    addr,
//							Code:    err.Code,
//							Message: err.Message,
//						}
//						return position, true, nil
//					})
//			}
//			if len(cf.Status.Err) > 0 {
//				for _, err := range cf.Status.Err {
//					saveErrorFn(err)
//				}
//			}
//			if len(cf.Status.Warning) > 0 {
//				for _, err := range cf.Status.Warning {
//					saveErrorFn(err)
//				}
//			}
//		}
//		c.lastSaveTime = time.Now()
//	}
//}

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
	ctx context.Context,
) error {
	minCheckpointTs, forceUpdate := c.calculateGCSafepoint(nil)
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

//func (c *coordinator) sendSavedCheckpointTsToMaintainer() {
//	for key, value := range c.scheduledChangefeeds {
//		cf := value.(*changefeed)
//		if !cf.isMQSink || cf.stateMachine == nil || cf.stateMachine.Primary == "" {
//			continue
//		}
//		id := model.DefaultChangeFeedID(key.String())
//		cfState, ok := c.lastState.Changefeeds[id]
//		if !ok || cfState.Status == nil {
//			continue
//		}
//		if cf.lastSavedCheckpointTs < cfState.Status.CheckpointTs {
//			msg := cf.NewCheckpointTsMessage(cfState.Status.CheckpointTs)
//			c.sendMessages([]*messaging.TargetMessage{msg})
//			cf.lastSavedCheckpointTs = cfState.Status.CheckpointTs
//		}
//	}
//}
