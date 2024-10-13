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
	"github.com/flowbehappy/tigate/maintainer/operator"
	"github.com/flowbehappy/tigate/maintainer/replica"
	"github.com/flowbehappy/tigate/maintainer/split"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/scheduler"
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
type Controller struct {
	//  initialTables hold all tables that before controller bootstrapped
	initialTables []commonEvent.Table
	bootstrapped  bool

	splitter               *split.Splitter
	spanReplicationEnabled bool
	startCheckpointTs      uint64
	ddlDispatcherID        common.DispatcherID

	changefeedID string
	batchSize    int
	sche         *Scheduler
	oc           *operator.OperatorController
	db           *replica.ReplicaSetDB
	nodeManager  *watcher.NodeManager

	taskScheduler threadpool.ThreadPool
	ocHandle      *threadpool.TaskHandle
	scheHandle    *threadpool.TaskHandle
}

func NewController(changefeedID string,
	checkpointTs uint64,
	pdapi pdutil.PDAPIClient,
	regionCache split.RegionCache,
	taskScheduler threadpool.ThreadPool,
	config *config.ChangefeedSchedulerConfig,
	batchSize int, balanceInterval time.Duration) *Controller {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	oc := operator.NewOperatorController(mc, batchSize)
	replicaSetDB := replica.NewReplicaSetDB()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	s := &Controller{
		startCheckpointTs: checkpointTs,
		changefeedID:      changefeedID,
		batchSize:         batchSize,
		bootstrapped:      false,
		sche:              NewScheduler(batchSize, changefeedID, oc, replicaSetDB, nodeManager, balanceInterval),
		oc:                oc,
		db:                replicaSetDB,
		nodeManager:       nodeManager,
		taskScheduler:     taskScheduler,
	}
	if config != nil && config.EnableTableAcrossNodes {
		s.splitter = split.NewSplitter(changefeedID, pdapi, regionCache, config)
		s.spanReplicationEnabled = true
	}
	return s
}

func (c *Controller) GetTasksBySchemaID(schemaID int64) []*replica.ReplicaSet {
	return c.db.GetTasksBySchemaID(schemaID)
}

func (c *Controller) GetTaskSizeBySchemaID(schemaID int64) int {
	return c.db.GetTaskSizeBySchemaID(schemaID)
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
	if c.db.IsTableExists(table.TableID) {
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
func (c *Controller) FinishBootstrap(workingMap map[int64]utils.Map[*heartbeatpb.TableSpan, *scheduler.StateMachine[common.DispatcherID]]) {
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
	c.ocHandle = c.taskScheduler.Submit(c.sche, time.Now())
	c.scheHandle = c.taskScheduler.Submit(c.oc, time.Now())
	c.bootstrapped = true
	c.initialTables = nil
}

func (c *Controller) Stop() {
	if c.ocHandle != nil {
		c.ocHandle.Cancel()
	}
	if c.scheHandle != nil {
		c.scheHandle.Cancel()
	}
}

// GetTask queries a task by dispatcherID, return nil if not found
func (c *Controller) GetTask(dispatcherID common.DispatcherID) (*replica.ReplicaSet, bool) {
	return c.db.GetTaskByID(dispatcherID)
}

func (c *Controller) RemoveAllTasks() {
	for _, replicaSet := range c.db.TryRemoveAll() {
		c.oc.ReplaceOperator(operator.NewRemoveDispatcherOperator(replicaSet))
	}
}

func (c *Controller) RemoveTasksBySchemaID(schemaID int64) {
	for _, replicaSet := range c.db.TryRemoveBySchemaID(schemaID) {
		c.oc.ReplaceOperator(operator.NewRemoveDispatcherOperator(replicaSet))
	}
}

func (c *Controller) RemoveTasksByTableIDs(tables ...int64) {
	for _, replicaSet := range c.db.TryRemoveByTableIDs(tables...) {
		c.oc.ReplaceOperator(operator.NewRemoveDispatcherOperator(replicaSet))
	}
}

func (c *Controller) GetTasksByTableIDs(tableIDs ...int64) []*replica.ReplicaSet {
	return c.db.GetTasksByTableIDs(tableIDs...)
}

// UpdateSchemaID will update the schema id of the table, and move the task to the new schema map
// it called when rename a table to another schema
func (c *Controller) UpdateSchemaID(tableID, newSchemaID int64) {
	c.db.UpdateSchemaID(tableID, newSchemaID)
}

func (c *Controller) RemoveNode(id node.ID) {
	c.db.RemoveNode(id)
}

// ScheduleFinished return false if not all task are running in working state
func (c *Controller) ScheduleFinished() bool {
	return c.db.GetAbsentSize() == 0 && c.oc.OperatorSize() == 0
}

func (c *Controller) HandleStatus(from node.ID, statusList []*heartbeatpb.TableSpanStatus) {
	for _, status := range statusList {
		span := common.NewDispatcherIDFromPB(status.ID)
		stm, working := c.GetTask(span)
		if stm == nil {
			log.Warn("no span found, ignore",
				zap.String("changefeed", c.changefeedID),
				zap.Any("from", from),
				zap.String("span", span.String()))
			continue
		}
		op := c.oc.GetOperator(span)
		if op != nil {
			op.Check(from, status)
		}
		nodeID := stm.GetNodeID()
		if nodeID != from {
			log.Warn("node id not match",
				zap.String("changefeed", c.changefeedID),
				zap.Any("from", from),
				zap.Stringer("node", nodeID))
			continue
		}
		stm.UpdateStatus(status)
		if status.ComponentStatus == heartbeatpb.ComponentState_Working {
			if !working {
				c.db.MarkReplicaSetWorking(stm)
			}
		} else {
			c.db.RemoveReplicaSet(stm)
		}
	}
}

func (c *Controller) TaskSize() int {
	return c.db.TaskSize()
}

func (c *Controller) GetSchedulingSize() int {
	return c.oc.OperatorSize()
}

func (c *Controller) GetTaskSizeByNodeID(id node.ID) int {
	return c.db.GetTaskSizeByNodeID(id)
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

func (c *Controller) addWorkingSpans(tableMap utils.Map[*heartbeatpb.TableSpan, *scheduler.StateMachine[common.DispatcherID]]) bool {
	ddlSpanFound := false
	tableMap.Ascend(func(span *heartbeatpb.TableSpan, stm *scheduler.StateMachine[common.DispatcherID]) bool {
		if stm.State != scheduler.SchedulerStatusWorking {
			log.Panic("unexpected state",
				zap.String("changefeed", c.changefeedID),
				zap.Any("stm", stm))
		}
		dispatcherID := stm.ID
		c.db.AddWorkingSpan(stm.Inferior.(*replica.ReplicaSet))
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
		dispatcherID, schemaID, span, startTs).(*replica.ReplicaSet)
	c.db.AddAbsentReplicaSet(replicaSet)
}
