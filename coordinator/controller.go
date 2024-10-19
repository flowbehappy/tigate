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

	"github.com/flowbehappy/tigate/coordinator/changefeed"
	"github.com/flowbehappy/tigate/coordinator/operator"
	"github.com/flowbehappy/tigate/coordinator/scheduler"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/bootstrap"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Controller schedules and balance changefeeds
// there are 3 main components in the controller, scheduler, ChangefeedDB and operator controller
type Controller struct {
	//  initailChangefeeds hold all tables that before controller bootstrapped
	initailChangefeeds map[string]*changefeed.Meta
	bootstrapped       bool
	version            int64

	nodeChanged *atomic.Bool

	cfScheduller       *scheduler.Scheduler
	operatorController *operator.Controller
	replicationDB      *changefeed.ChangefeedDB
	messageCenter      messaging.MessageCenter
	nodeManager        *watcher.NodeManager
	batchSize          int

	bootstrapper *bootstrap.Bootstrapper[heartbeatpb.CoordinatorBootstrapResponse]

	taskScheduler            threadpool.ThreadPool
	operatorControllerHandle *threadpool.TaskHandle
	schedulerHandle          *threadpool.TaskHandle
	backend                  *changefeed.EtcdBackend
}

func NewController(
	version int64,
	backend *changefeed.EtcdBackend,
	taskScheduler threadpool.ThreadPool,
	batchSize int, balanceInterval time.Duration) *Controller {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	changefeedDB := changefeed.NewChangefeedDB()

	oc := operator.NewOperatorController(mc, changefeedDB, batchSize)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	c := &Controller{
		version:            version,
		batchSize:          batchSize,
		bootstrapped:       false,
		cfScheduller:       scheduler.NewScheduler(batchSize, oc, changefeedDB, nodeManager, balanceInterval),
		operatorController: oc,
		messageCenter:      mc,
		replicationDB:      changefeedDB,
		nodeManager:        nodeManager,
		taskScheduler:      taskScheduler,
		backend:            backend,
		nodeChanged:        atomic.NewBool(false),
	}
	c.bootstrapper = bootstrap.NewBootstrapper[heartbeatpb.CoordinatorBootstrapResponse]("coordinator", c.newBootstrapMessage)
	cfs, err := c.backend.LoadAllChangefeeds(context.Background())
	if err != nil {
		log.Panic("load all changefeeds failed", zap.Error(err))
	}
	c.initailChangefeeds = cfs
	// init bootstrapper nodes
	nodes := c.nodeManager.GetAliveNodes()
	// detect the capture changes
	c.nodeManager.RegisterNodeChangeHandler("coordinator-controller", func(allNodes map[node.ID]*node.Info) {
		//c.nodeChanged.Store(true)
		c.onNodeChanged()
	})
	log.Info("changefeed bootstrap initial nodes",
		zap.Int("nodes", len(nodes)))
	var newNodes = make([]*node.Info, 0, len(nodes))
	for _, n := range nodes {
		newNodes = append(newNodes, n)
	}
	for _, msg := range c.bootstrapper.HandleNewNodes(newNodes) {
		_ = c.messageCenter.SendCommand(msg)
	}
	return c
}

// HandleEvent implements the event-driven process mode
func (c *Controller) HandleEvent(event *Event) bool {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		if duration > time.Second {
			log.Info("coordinator is too slow",
				zap.Int("type", event.eventType),
				zap.Duration("duration", duration))
		}
	}()
	// first check the online/offline nodes
	if c.nodeChanged.Load() {
		c.onNodeChanged()
		c.nodeChanged.Store(false)
	}
	switch event.eventType {
	case EventMessage:
		c.onMessage(event.message)
	}
	return false
}

func (c *Controller) CreateChangefeed(ctx context.Context, info *model.ChangeFeedInfo) error {
	if !c.bootstrapped {
		return errors.New("not initialized, wait a moment")
	}
	id := model.DefaultChangeFeedID(info.ID)
	old := c.replicationDB.GetByID(id)
	if old != nil {
		return errors.New("changefeed already exists")
	}
	op := c.operatorController.GetOperator(id)
	if op != nil {
		return errors.New("changefeed is removing")
	}
	_, err := c.backend.CreateChangefeed(ctx, info)
	if err != nil {
		return errors.Trace(err)
	}
	c.replicationDB.AddAbsentChangefeed(changefeed.NewChangefeed(id, info, info.StartTs))
	return nil
}

func (c *Controller) onMessage(msg *messaging.TargetMessage) {
	switch msg.Type {
	case messaging.TypeCoordinatorBootstrapResponse:
		c.onMaintainerBootstrapResponse(msg)
	case messaging.TypeMaintainerHeartbeatRequest:
		if c.bootstrapper.CheckAllNodeInitialized() {
			req := msg.Message[0].(*heartbeatpb.MaintainerHeartbeat)
			c.HandleStatus(msg.From, req.Statuses)
		}
	default:
		log.Panic("unexpected message type",
			zap.String("type", msg.Type.String()))
	}
}

func (c *Controller) onNodeChanged() {
	currentNodes := c.bootstrapper.GetAllNodes()

	activeNodes := c.nodeManager.GetAliveNodes()
	var newNodes = make([]*node.Info, 0, len(activeNodes))
	for id, n := range activeNodes {
		if _, ok := currentNodes[id]; !ok {
			newNodes = append(newNodes, n)
		}
	}
	var removedNodes []node.ID
	for id, _ := range currentNodes {
		if _, ok := activeNodes[id]; !ok {
			removedNodes = append(removedNodes, id)
			c.RemoveNode(id)
		}
	}
	log.Info("maintainer node changed",
		zap.Int("new", len(newNodes)),
		zap.Int("removed", len(removedNodes)))
	c.sendMessages(c.bootstrapper.HandleNewNodes(newNodes))
	cachedResponse := c.bootstrapper.HandleRemoveNodes(removedNodes)
	if cachedResponse != nil {
		log.Info("bootstrap done after removed some nodes")
		c.onBootstrapDone(cachedResponse)
	}
}

func (c *Controller) sendMessages(msgs []*messaging.TargetMessage) {
	for _, msg := range msgs {
		_ = c.messageCenter.SendCommand(msg)
	}
}

func (c *Controller) onMaintainerBootstrapResponse(msg *messaging.TargetMessage) {
	log.Info("received maintainer bootstrap response",
		zap.Any("server", msg.From))
	cachedResp := c.bootstrapper.HandleBootstrapResponse(msg.From, msg.Message[0].(*heartbeatpb.CoordinatorBootstrapResponse))
	c.onBootstrapDone(cachedResp)
}

type remoteMaintainer struct {
	nodeID node.ID
	status *heartbeatpb.MaintainerStatus
}

func (c *Controller) onBootstrapDone(cachedResp map[node.ID]*heartbeatpb.CoordinatorBootstrapResponse) {
	if cachedResp == nil {
		return
	}
	log.Info("all nodes have sent bootstrap response",
		zap.Int("size", len(cachedResp)))
	workingMap := make(map[model.ChangeFeedID]remoteMaintainer)
	for server, bootstrapMsg := range cachedResp {
		log.Info("received bootstrap response",
			zap.Any("server", server),
			zap.Int("size", len(bootstrapMsg.Statuses)))
		for _, info := range bootstrapMsg.Statuses {
			cfID := model.DefaultChangeFeedID(info.ChangefeedID)
			if _, ok := workingMap[cfID]; ok {
				log.Panic("maintainer runs on multiple node",
					zap.String("cf", info.ChangefeedID))
			}
			workingMap[cfID] = remoteMaintainer{
				nodeID: server,
				status: info,
			}
		}
	}
	c.FinishBootstrap(workingMap)
}

// HandleStatus handle the status report from the node
func (c *Controller) HandleStatus(from node.ID, statusList []*heartbeatpb.MaintainerStatus) {
	for _, status := range statusList {
		cfID := model.DefaultChangeFeedID(status.ChangefeedID)
		c.operatorController.UpdateOperatorStatus(cfID, from, status)
		cf := c.GetTask(cfID)
		if cf == nil {
			log.Warn("no span found, ignore",
				zap.String("changefeed", status.ChangefeedID),
				zap.String("from", from.String()),
				zap.Any("status", status))
			if status.State == heartbeatpb.ComponentState_Working {
				// if the span is not found, and the status is working, we need to remove it from dispatcher
				_ = c.messageCenter.SendCommand(changefeed.NewRemoveInferiorMessage(status.ChangefeedID, from, true))
			}
			continue
		}
		nodeID := cf.GetNodeID()
		if nodeID != from {
			// todo: handle the case that the node id is mismatch
			log.Warn("node id not match",
				zap.String("changefeed", status.ChangefeedID),
				zap.Stringer("from", from),
				zap.Stringer("node", nodeID))
			continue
		}
		cf.UpdateStatus(status)
	}
}

// FinishBootstrap adds working state tasks to this controller directly,
// it reported by the bootstrap response
func (c *Controller) FinishBootstrap(workingMap map[model.ChangeFeedID]remoteMaintainer) {
	if c.bootstrapped {
		log.Panic("already bootstrapped",
			zap.Any("workingMap", workingMap))
	}
	for id, cfMeta := range c.initailChangefeeds {
		cfID := model.DefaultChangeFeedID(id)
		rm, ok := workingMap[cfID]
		if !ok {
			cf := changefeed.NewChangefeed(cfID, cfMeta.Info, cfMeta.Status.CheckpointTs)
			c.replicationDB.AddAbsentChangefeed(cf)
		} else {
			log.Info("maintainer already working in other server",
				zap.String("changefeed", id))
			cf := changefeed.NewChangefeed(cfID, cfMeta.Info, rm.status.CheckpointTs)
			cf.SetNodeID(rm.nodeID)
			c.replicationDB.AddAbsentChangefeed(cf)
			// delete it
			delete(workingMap, cfID)
		}
	}
	for id, rm := range workingMap {
		log.Warn("maintainer not found in local, remove it",
			zap.String("changefeed", id.ID),
			zap.String("node", rm.nodeID.String()),
		)
		_ = c.messageCenter.SendCommand(changefeed.NewRemoveInferiorMessage(id.ID, rm.nodeID, true))
	}

	// start operator and scheduler
	c.operatorControllerHandle = c.taskScheduler.Submit(c.cfScheduller, time.Now())
	c.schedulerHandle = c.taskScheduler.Submit(c.operatorController, time.Now())
	c.bootstrapped = true
	c.initailChangefeeds = nil
}

func (c *Controller) Stop() {
	if c.operatorControllerHandle != nil {
		c.operatorControllerHandle.Cancel()
	}
	if c.schedulerHandle != nil {
		c.schedulerHandle.Cancel()
	}
}

// GetTask queries a task by dispatcherID, return nil if not found
func (c *Controller) GetTask(id model.ChangeFeedID) *changefeed.Changefeed {
	return c.replicationDB.GetByID(id)
}

// RemoveNode is called when a node is removed
func (c *Controller) RemoveNode(id node.ID) {
	c.operatorController.OnNodeRemoved(id)
}

// ScheduleFinished return false if not all task are running in working state
func (c *Controller) ScheduleFinished() bool {
	return c.replicationDB.GetAbsentSize() == 0 && c.operatorController.OperatorSize() == 0
}

func (c *Controller) newBootstrapMessage(id node.ID) *messaging.TargetMessage {
	log.Info("send coordinator bootstrap request", zap.Any("to", id))
	return messaging.NewSingleTargetMessage(
		id,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.CoordinatorBootstrapRequest{Version: c.version})
}
