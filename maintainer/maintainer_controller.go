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

package maintainer

import (
	"context"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/maintainer/checker"
	"github.com/flowbehappy/tigate/maintainer/operator"
	"github.com/flowbehappy/tigate/maintainer/replica"
	"github.com/flowbehappy/tigate/maintainer/scheduler"
	"github.com/flowbehappy/tigate/maintainer/split"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/flowbehappy/tigate/utils"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/spanz"
	"go.uber.org/zap"
)

// Controller schedules and balance tables
// there are 3 main components in the controller, scheduler, ReplicationDB and operator controller
type Controller struct {
	//  initialTables hold all tables that before controller bootstrapped
	initialTables []commonEvent.Table
	bootstrapped  bool

	spanScheduler      *scheduler.Scheduler
	operatorController *operator.Controller
	checkController    *checker.Controller
	replicationDB      *replica.ReplicationDB
	messageCenter      messaging.MessageCenter
	nodeManager        *watcher.NodeManager

	splitter               *split.Splitter
	spanReplicationEnabled bool
	startCheckpointTs      uint64
	ddlDispatcherID        common.DispatcherID

	changefeedID string
	batchSize    int

	taskScheduler            threadpool.ThreadPool
	operatorControllerHandle *threadpool.TaskHandle
	schedulerHandle          *threadpool.TaskHandle
	checkerHandle            *threadpool.TaskHandle
}

func NewController(changefeedID string,
	checkpointTs uint64,
	pdapi pdutil.PDAPIClient,
	regionCache split.RegionCache,
	taskScheduler threadpool.ThreadPool,
	config *config.ChangefeedSchedulerConfig,
	batchSize int, balanceInterval time.Duration) *Controller {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	replicaSetDB := replica.NewReplicaSetDB(changefeedID)
	oc := operator.NewOperatorController(changefeedID, mc, replicaSetDB, batchSize)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	s := &Controller{
		startCheckpointTs:  checkpointTs,
		changefeedID:       changefeedID,
		batchSize:          batchSize,
		bootstrapped:       false,
		spanScheduler:      scheduler.NewScheduler(changefeedID, batchSize, oc, replicaSetDB, nodeManager, balanceInterval),
		operatorController: oc,
		messageCenter:      mc,
		replicationDB:      replicaSetDB,
		nodeManager:        nodeManager,
		taskScheduler:      taskScheduler,
	}
	if config != nil && config.EnableTableAcrossNodes {
		s.splitter = split.NewSplitter(changefeedID, pdapi, regionCache, config)
		s.spanReplicationEnabled = true
	}
	s.checkController = checker.NewController(changefeedID, s.splitter, oc, replicaSetDB, nodeManager)
	return s
}

// HandleStatus handle the status report from the node
func (c *Controller) HandleStatus(from node.ID, statusList []*heartbeatpb.TableSpanStatus) {
	for _, status := range statusList {
		dispatcherID := common.NewDispatcherIDFromPB(status.ID)
		c.operatorController.UpdateOperatorStatus(dispatcherID, from, status)
		stm := c.GetTask(dispatcherID)
		if stm == nil {
			log.Warn("no span found, ignore",
				zap.String("changefeed", c.changefeedID),
				zap.String("from", from.String()),
				zap.Any("status", status),
				zap.String("span", dispatcherID.String()))
			if status.ComponentStatus == heartbeatpb.ComponentState_Working {
				// if the span is not found, and the status is working, we need to remove it from dispatcher
				_ = c.messageCenter.SendCommand(replica.NewRemoveInferiorMessage(from, c.changefeedID, status.ID))
			}
			continue
		}
		nodeID := stm.GetNodeID()
		if nodeID != from {
			// todo: handle the case that the node id is mismatch
			log.Warn("node id not match",
				zap.String("changefeed", c.changefeedID),
				zap.Any("from", from),
				zap.Stringer("node", nodeID))
			continue
		}
		stm.UpdateStatus(status)
	}
}

func (c *Controller) GetTasksBySchemaID(schemaID int64) []*replica.SpanReplication {
	return c.replicationDB.GetTasksBySchemaID(schemaID)
}

func (c *Controller) GetTaskSizeBySchemaID(schemaID int64) int {
	return c.replicationDB.GetTaskSizeBySchemaID(schemaID)
}

func (c *Controller) GetAllNodes() []node.ID {
	aliveNodes := c.nodeManager.GetAliveNodes()
	var nodes = make([]node.ID, 0, len(aliveNodes))
	for id := range aliveNodes {
		nodes = append(nodes, id)
	}
	return nodes
}

func (c *Controller) AddNewTable(table commonEvent.Table, startTs uint64) {
	if c.replicationDB.IsTableExists(table.TableID) {
		log.Warn("table already add, ignore",
			zap.String("changefeed", c.changefeedID),
			zap.Int64("schema", table.SchemaID),
			zap.Int64("table", table.TableID))
		return
	}
	span := spanz.TableIDToComparableSpan(table.TableID)
	tableSpan := &heartbeatpb.TableSpan{
		TableID:  table.TableID,
		StartKey: span.StartKey,
		EndKey:   span.EndKey,
	}
	tableSpans := []*heartbeatpb.TableSpan{tableSpan}
	if c.spanReplicationEnabled {
		//split the whole table span base on the configuration, todo: background split table
		tableSpans = c.splitter.SplitSpans(context.Background(), tableSpan, len(c.nodeManager.GetAliveNodes()))
	}
	c.addNewSpans(table.SchemaID, tableSpans, startTs)
}

func (c *Controller) SetInitialTables(tables []commonEvent.Table) {
	c.initialTables = tables
}

// FinishBootstrap adds working state tasks to this controller directly,
// it reported by the bootstrap response
func (c *Controller) FinishBootstrap(workingMap map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication]) {
	if c.bootstrapped {
		log.Panic("already bootstrapped",
			zap.String("changefeed", c.changefeedID),
			zap.Any("workingMap", workingMap))
	}
	for _, table := range c.initialTables {
		tableMap, ok := workingMap[table.TableID]
		if !ok {
			c.AddNewTable(table, c.startCheckpointTs)
		} else {
			span := spanz.TableIDToComparableSpan(table.TableID)
			tableSpan := &heartbeatpb.TableSpan{
				TableID:  table.TableID,
				StartKey: span.StartKey,
				EndKey:   span.EndKey,
			}
			log.Info("table already working in other server",
				zap.String("changefeed", c.changefeedID),
				zap.Int64("tableID", table.TableID))
			c.addWorkingSpans(tableMap)
			if c.spanReplicationEnabled {
				holes := split.FindHoles(tableMap, tableSpan)
				// todo: split the hole
				c.addNewSpans(table.SchemaID, holes, c.startCheckpointTs)
			}
			// delete it
			delete(workingMap, table.TableID)
		}
	}
	ddlSpanFound := false
	// tables that not included in init table map we get from tikv at checkpoint ts
	// that can happen if a table is created after checkpoint ts
	// the initial table map only contains real physical tables,
	// ddl table is special table id (0), can be included in the bootstrap response message
	for tableID, tableMap := range workingMap {
		log.Info("found a tables not in initial table map",
			zap.String("changefeed", c.changefeedID),
			zap.Int64("id", tableID))
		if c.addWorkingSpans(tableMap) {
			ddlSpanFound = true
		}
	}

	// add a table_event_trigger dispatcher if not found
	if !ddlSpanFound {
		c.addDDLDispatcher()
	}
	// start operator and scheduler
	c.operatorControllerHandle = c.taskScheduler.Submit(c.spanScheduler, time.Now())
	c.schedulerHandle = c.taskScheduler.Submit(c.operatorController, time.Now())
	c.checkerHandle = c.taskScheduler.Submit(c.checkController, time.Now().Add(time.Second*120))
	c.bootstrapped = true
	c.initialTables = nil
}

func (c *Controller) Stop() {
	if c.operatorControllerHandle != nil {
		c.operatorControllerHandle.Cancel()
	}
	if c.schedulerHandle != nil {
		c.schedulerHandle.Cancel()
	}
	if c.checkerHandle != nil {
		c.checkerHandle.Cancel()
	}
}

// GetTask queries a task by dispatcherID, return nil if not found
func (c *Controller) GetTask(dispatcherID common.DispatcherID) *replica.SpanReplication {
	return c.replicationDB.GetTaskByID(dispatcherID)
}

// RemoveAllTasks remove all tasks
func (c *Controller) RemoveAllTasks() {
	c.operatorController.RemoveAllTasks()
}

// RemoveTasksBySchemaID remove all tasks by schema id
func (c *Controller) RemoveTasksBySchemaID(schemaID int64) {
	c.operatorController.RemoveTasksBySchemaID(schemaID)
}

// RemoveTasksByTableIDs remove all tasks by table id
func (c *Controller) RemoveTasksByTableIDs(tables ...int64) {
	c.operatorController.RemoveTasksByTableIDs(tables...)
}

// GetTasksByTableIDs get all tasks by table id
func (c *Controller) GetTasksByTableIDs(tableIDs ...int64) []*replica.SpanReplication {
	return c.replicationDB.GetTasksByTableIDs(tableIDs...)
}

// GetAllTasks get all tasks
func (c *Controller) GetAllTasks() []*replica.SpanReplication {
	return c.replicationDB.GetAllTasks()
}

// UpdateSchemaID will update the schema id of the table, and move the task to the new schema map
// it called when rename a table to another schema
func (c *Controller) UpdateSchemaID(tableID, newSchemaID int64) {
	c.replicationDB.UpdateSchemaID(tableID, newSchemaID)
}

// RemoveNode is called when a node is removed
func (c *Controller) RemoveNode(id node.ID) {
	c.operatorController.OnNodeRemoved(id)
}

// ScheduleFinished return false if not all task are running in working state
func (c *Controller) ScheduleFinished() bool {
	return c.replicationDB.GetAbsentSize() == 0 && c.operatorController.OperatorSize() == 0
}

func (c *Controller) TaskSize() int {
	return c.replicationDB.TaskSize()
}

func (c *Controller) GetSchedulingSize() int {
	return c.replicationDB.GetSchedulingSize()
}

func (c *Controller) GetTaskSizeByNodeID(id node.ID) int {
	return c.replicationDB.GetTaskSizeByNodeID(id)
}

func (c *Controller) addDDLDispatcher() {
	ddlTableSpan := heartbeatpb.DDLSpan
	c.ddlDispatcherID = common.NewDispatcherID()
	c.addNewSpan(c.ddlDispatcherID, heartbeatpb.DDLSpanSchemaID,
		ddlTableSpan, c.startCheckpointTs)
	log.Info("create table event trigger dispatcher",
		zap.String("changefeed", c.changefeedID),
		zap.String("dispatcher", c.ddlDispatcherID.String()))
}

func (c *Controller) addWorkingSpans(tableMap utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication]) bool {
	ddlSpanFound := false
	tableMap.Ascend(func(span *heartbeatpb.TableSpan, stm *replica.SpanReplication) bool {
		dispatcherID := stm.ID
		c.replicationDB.AddReplicatingSpan(stm)
		if span.TableID == 0 {
			ddlSpanFound = true
			c.ddlDispatcherID = dispatcherID
		}
		return true
	})
	return ddlSpanFound
}

func (c *Controller) addNewSpans(schemaID int64,
	tableSpans []*heartbeatpb.TableSpan, startTs uint64) {
	for _, newSpan := range tableSpans {
		dispatcherID := common.NewDispatcherID()
		c.addNewSpan(dispatcherID, schemaID, newSpan, startTs)
	}
}

func (c *Controller) addNewSpan(dispatcherID common.DispatcherID, schemaID int64,
	span *heartbeatpb.TableSpan, startTs uint64) {
	replicaSet := replica.NewReplicaSet(model.DefaultChangeFeedID(c.changefeedID),
		dispatcherID, schemaID, span, startTs)
	c.replicationDB.AddAbsentReplicaSet(replicaSet)
}
