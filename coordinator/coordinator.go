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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/pingcap/ticdc/utils/threadpool"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// coordinator implements the Coordinator interface
type coordinator struct {
	nodeInfo     *node.Info
	initialized  bool
	version      int64
	lastTickTime time.Time

	mc            messaging.MessageCenter
	stream        dynstream.DynamicStream[int, string, *Event, *Controller, *StreamHandler]
	taskScheduler threadpool.ThreadPool
	controller    *Controller

	gcManager gc.Manager
	pdClient  pd.Client
	pdClock   pdutil.Clock

	updatedChangefeedCh chan map[common.ChangeFeedID]*changefeed.Changefeed
}

func New(node *node.Info,
	pdClient pd.Client,
	pdClock pdutil.Clock,
	backend changefeed.Backend,
	clusterID string,
	version int64,
	batchSize int,
	balanceCheckInterval time.Duration) node.Coordinator {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	c := &coordinator{
		version:             version,
		nodeInfo:            node,
		lastTickTime:        time.Now(),
		gcManager:           gc.NewManager(clusterID, pdClient, pdClock),
		pdClient:            pdClient,
		pdClock:             pdClock,
		mc:                  mc,
		updatedChangefeedCh: make(chan map[common.ChangeFeedID]*changefeed.Changefeed, 1024),
	}
	c.stream = dynstream.NewDynamicStream[int, string, *Event, *Controller, *StreamHandler](NewStreamHandler())
	c.stream.Start()
	c.taskScheduler = threadpool.NewThreadPoolDefault()

	ctl := NewController(c.version, c.nodeInfo, c.updatedChangefeedCh, backend, c.stream, c.taskScheduler, batchSize, balanceCheckInterval)
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
	defer gcTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gcTick.C:
			if err := c.updateGCSafepoint(ctx); err != nil {
				log.Warn("update gc safepoint failed",
					zap.Error(err))
			}
			now := time.Now()
			metrics.CoordinatorCounter.Add(float64(now.Sub(c.lastTickTime)) / float64(time.Second))
			c.lastTickTime = now
		case cfs := <-c.updatedChangefeedCh:
			if err := c.saveCheckpointTs(ctx, cfs); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (c *coordinator) saveCheckpointTs(ctx context.Context, cfs map[common.ChangeFeedID]*changefeed.Changefeed) error {
	statusMap := make(map[common.ChangeFeedID]uint64)
	for _, upCf := range cfs {
		reportedCheckpointTs := upCf.GetStatus().CheckpointTs
		if upCf.GetLastSavedCheckPointTs() < reportedCheckpointTs {
			statusMap[upCf.ID] = reportedCheckpointTs
		}
	}
	if len(statusMap) == 0 {
		return nil
	}
	err := c.controller.backend.UpdateChangefeedCheckpointTs(ctx, statusMap)
	if err != nil {
		log.Error("failed to update checkpointTs", zap.Error(err))
		return errors.Trace(err)
	}
	// update the last saved checkpoint ts and send checkpointTs to maintainer
	for id, cp := range statusMap {
		cf, ok := cfs[id]
		if !ok {
			continue
		}
		cf.SetLastSavedCheckPointTs(cp)
		if cf.IsMQSink() {
			msg := cf.NewCheckpointTsMessage(cf.GetLastSavedCheckPointTs())
			c.sendMessages([]*messaging.TargetMessage{msg})
		}
	}
	return nil
}

func (c *coordinator) CreateChangefeed(ctx context.Context, info *config.ChangeFeedInfo) error {
	return c.controller.CreateChangefeed(ctx, info)
}

func (c *coordinator) RemoveChangefeed(ctx context.Context, id common.ChangeFeedID) (uint64, error) {
	return c.controller.RemoveChangefeed(ctx, id)
}

func (c *coordinator) PauseChangefeed(ctx context.Context, id common.ChangeFeedID) error {
	return c.controller.PauseChangefeed(ctx, id)
}

func (c *coordinator) ResumeChangefeed(ctx context.Context, id common.ChangeFeedID, newCheckpointTs uint64) error {
	return c.controller.ResumeChangefeed(ctx, id, newCheckpointTs)
}

func (c *coordinator) UpdateChangefeed(ctx context.Context, change *config.ChangeFeedInfo) error {
	return c.controller.UpdateChangefeed(ctx, change)
}

func (c *coordinator) ListChangefeeds(ctx context.Context) ([]*config.ChangeFeedInfo, []*config.ChangeFeedStatus, error) {
	return c.controller.ListChangefeeds(ctx)
}

func (c *coordinator) GetChangefeed(ctx context.Context, changefeedDisplayName common.ChangeFeedDisplayName) (*config.ChangeFeedInfo, *config.ChangeFeedStatus, error) {
	return c.controller.GetChangefeed(ctx, changefeedDisplayName)
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

func (c *coordinator) newBootstrapMessage(id node.ID) *messaging.TargetMessage {
	log.Info("send coordinator bootstrap request", zap.Any("to", id))
	return messaging.NewSingleTargetMessage(
		id,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.CoordinatorBootstrapRequest{Version: c.version})
}

func (c *coordinator) updateGCSafepoint(
	ctx context.Context,
) error {
	minCheckpointTs := c.controller.changefeedDB.CalculateGCSafepoint()
	// check if the upstream has a changefeed, if not we should update the gc safepoint
	if minCheckpointTs == math.MaxUint64 {
		ts := c.pdClock.CurrentTime()
		minCheckpointTs = oracle.GoTimeToTS(ts)
	}
	// When the changefeed starts up, CDC will do a snapshot read at
	// (checkpointTs - 1) from TiKV, so (checkpointTs - 1) should be an upper
	// bound for the GC safepoint.
	gcSafepointUpperBound := minCheckpointTs - 1
	err := c.gcManager.TryUpdateGCSafePoint(ctx, gcSafepointUpperBound, false)
	return errors.Trace(err)
}
