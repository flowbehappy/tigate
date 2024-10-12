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
	"sync"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/maintainer/split"
	"github.com/flowbehappy/tigate/pkg/common"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/utils"
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
	// group the tasks by nodes
	nodeTasks map[node.ID]map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]
	// group the tasks by schema id
	schemaTasks map[int64]map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]
	// tables
	tableTasks map[int64]map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]
	// totalMaps holds all state maps, absent, committing, working and removing
	totalMaps    []map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]
	bootstrapped bool

	splitter               *split.Splitter
	spanReplicationEnabled bool
	startCheckpointTs      uint64
	ddlDispatcherID        common.DispatcherID

	changefeedID string
	batchSize    int
	sche         *Scheduler[common.DispatcherID]
	oc           *OperatorController

	// the dispatcher is in replacing status, not removed, when the dispatcher is removed by the ddl,
	// it should be removed from this map
	pendingTaskLock sync.RWMutex
	replacing       map[common.DispatcherID]struct{}
	pendingTasks    []*InternalScheduleDispatcherEvent
}

func NewController(changefeedID string,
	checkpointTs uint64,
	pdapi pdutil.PDAPIClient,
	regionCache split.RegionCache,
	config *config.ChangefeedSchedulerConfig,
	batchSize int, balanceInterval time.Duration) *Controller {
	s := &Controller{
		nodeTasks:         make(map[node.ID]map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]),
		schemaTasks:       make(map[int64]map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]),
		tableTasks:        make(map[int64]map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]),
		startCheckpointTs: checkpointTs,
		changefeedID:      changefeedID,
		batchSize:         batchSize,
		bootstrapped:      false,
		replacing:         make(map[common.DispatcherID]struct{}),
		sche:              NewScheduler[common.DispatcherID](batchSize, changefeedID, balanceInterval),
		oc:                NewOperatorController(),
	}
	if config != nil && config.EnableTableAcrossNodes {
		s.splitter = split.NewSplitter(changefeedID, pdapi, regionCache, config)
		s.spanReplicationEnabled = true
	}
	// put all maps to totalMaps
	s.totalMaps = make([]map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID], 4)
	s.totalMaps[scheduler.SchedulerStatusAbsent] = make(map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID])
	s.totalMaps[scheduler.SchedulerStatusWorking] = make(map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID])
	// running tasks
	s.totalMaps[scheduler.SchedulerStatusRemoving] = make(map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID])
	s.totalMaps[scheduler.SchedulerStatusCommiting] = make(map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID])
	return s
}

func (c *Controller) MarkRePlacing(id common.DispatcherID) {
	c.pendingTaskLock.Lock()
	defer c.pendingTaskLock.Unlock()
	c.replacing[id] = struct{}{}
}

func (c *Controller) PreAddPendingTask(task *InternalScheduleDispatcherEvent) common.DispatcherID {
	return c.ddlDispatcherID
}

func (c *Controller) MarkPendingTask(task *InternalScheduleDispatcherEvent) common.DispatcherID {
	return c.ddlDispatcherID
}

// schedulePendingDispatchers handle the dispatcher scheduling trigger by other modules
func (c *Controller) schedulePendingDispatchers() {
	c.pendingTaskLock.Lock()
	defer c.pendingTaskLock.Unlock()
	for _, event := range c.pendingTasks {
		if event.RemovingDispatcher != nil {
			stm := c.GetTask(event.RemovingDispatcher.DispatcherID)
			if stm != nil {
				c.ReplaceTask(stm)
			} else {
				log.Warn("dispatcher is not found",
					zap.String("id", c.changefeedID),
					zap.Any("dispatcher", event.RemovingDispatcher))
			}
		}

		if event.ReplacingDispatcher != nil {
			allFound := true
			for _, disp := range event.ReplacingDispatcher.Removing {
				if _, ok := c.replacing[disp]; !ok {
					log.Warn("dispatcher is not found in replacing map",
						zap.String("id", c.changefeedID),
						zap.Any("dispatcher", disp))
					allFound = false
				}
				delete(c.replacing, disp)
			}
			if allFound {
				for _, disp := range event.ReplacingDispatcher.NewDispatcher {
					c.addNewSpans(disp.SchemaID, disp.Span.TableID,
						[]*heartbeatpb.TableSpan{disp.Span}, disp.CheckpointTs)
				}
			}
		}
	}
	c.pendingTasks = nil
}

func (c *Controller) GetTasksBySchemaID(schemaID int64) map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID] {
	return c.schemaTasks[schemaID]
}

func (c *Controller) GetAllNodes() []node.ID {
	var nodes = make([]node.ID, 0, len(c.nodeTasks))
	for id := range c.nodeTasks {
		nodes = append(nodes, id)
	}
	return nodes
}

func (c *Controller) AddNewTable(table commonEvent.Table, startTs uint64) {
	tables, ok := c.tableTasks[table.TableID]
	if ok && len(tables) > 0 {
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
		tableSpans = c.splitter.SplitSpans(context.Background(), tableSpan, len(c.nodeTasks))
	}
	c.addNewSpans(table.SchemaID, tableSpan.TableID, tableSpans, startTs)
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
				c.addNewSpans(table.SchemaID, table.TableID, holes, c.startCheckpointTs)
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
	c.bootstrapped = true
	c.initialTables = nil
}

// GetTask queries a task by dispatcherID, return nil if not found
func (c *Controller) GetTask(dispatcherID common.DispatcherID) *scheduler.StateMachine[common.DispatcherID] {
	var stm *scheduler.StateMachine[common.DispatcherID]
	var ok bool
	for _, m := range c.totalMaps {
		stm, ok = m[dispatcherID]
		if ok {
			break
		}
	}
	return stm
}

func (c *Controller) RemoveAllTasks() {
	for _, m := range c.totalMaps {
		for _, stm := range m {
			c.RemoveTask(stm)
		}
	}
}

func (c *Controller) RemoveTask(stm *scheduler.StateMachine[common.DispatcherID]) {
	oldState := stm.State
	oldPrimary := stm.Primary
	stm.HandleRemoveInferior()
	c.tryMoveTask(stm.ID, stm, oldState, oldPrimary, true)
}

func (c *Controller) ReplaceTask(stm *scheduler.StateMachine[common.DispatcherID]) {
	c.RemoveTask(stm)
	c.replacing[stm.ID] = struct{}{}
}

func (c *Controller) GetTasksByTableIDs(tableIDs ...int64) []*scheduler.StateMachine[common.DispatcherID] {
	var stms []*scheduler.StateMachine[common.DispatcherID]
	for _, tableID := range tableIDs {
		for _, stm := range c.tableTasks[tableID] {
			stms = append(stms, stm)
		}
	}
	return stms
}

// UpdateSchemaID will update the schema id of the table, and move the task to the new schema map
// it called when rename a table to another schema
func (c *Controller) UpdateSchemaID(tableID, newSchemaID int64) {
	for _, stm := range c.tableTasks[tableID] {
		replicaSet := stm.Inferior.(*ReplicaSet)
		oldSchemaID := replicaSet.SchemaID
		// update schemaID
		replicaSet.SchemaID = newSchemaID

		//update schema map
		schemaMap, ok := c.schemaTasks[oldSchemaID]
		if ok {
			delete(schemaMap, stm.ID)
			//clear the map if empty
			if len(schemaMap) == 0 {
				delete(c.schemaTasks, oldSchemaID)
			}
		}
		// add it to new schema map
		newMap, ok := c.schemaTasks[newSchemaID]
		if !ok {
			newMap = make(map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID])
			c.schemaTasks[newSchemaID] = newMap
		}
		newMap[stm.ID] = stm
	}
}

func (c *Controller) AddNewNode(id node.ID) {
	_, ok := c.nodeTasks[id]
	if ok {
		log.Info("node already exists",
			zap.String("changefeed", c.changefeedID),
			zap.Any("node", id))
		return
	}
	log.Info("add new node",
		zap.String("changefeed", c.changefeedID),
		zap.Any("node", id))
	c.nodeTasks[id] = make(map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID])
}

func (c *Controller) RemoveNode(id node.ID) {
	stmMap, ok := c.nodeTasks[id]
	if !ok {
		log.Info("node is not maintained by controller, ignore",
			zap.String("changefeed", c.changefeedID),
			zap.Any("node", id))
		return
	}
	for key, value := range stmMap {
		oldState := value.State
		value.HandleCaptureShutdown(id)
		if value.Primary != "" && value.Primary != id {
			delete(c.nodeTasks[value.Primary], key)
		}
		c.tryMoveTask(key, value, oldState, id, false)
	}
	// check removing map, maybe some task are moving to this node
	for key, value := range c.Removing() {
		if value.Secondary == id {
			oldState := value.State
			value.HandleCaptureShutdown(id)
			c.tryMoveTask(key, value, oldState, id, false)
		}
	}
	delete(c.nodeTasks, id)
}

func (c *Controller) Schedule() {
	if len(c.nodeTasks) == 0 {
		log.Warn("controller has no node tasks", zap.String("changeeed", c.changefeedID))
		return
	}
	absents := c.Absent()
	working := c.Working()
	commiting := c.Commiting()
	tasks := c.sche.Schedule(absents, commiting, c.Removing(), working, c.nodeTasks)
	for _, task := range tasks {
		if task.AddInferior != nil {
			value := absents[task.AddInferior.ID]
			value.HandleAddInferior(task.AddInferior.CaptureID)
			commiting[value.ID] = value
			c.nodeTasks[task.AddInferior.CaptureID][value.ID] = value
			delete(absents, value.ID)
		} else if task.MoveInferior != nil {
			op := c.oc.GetOperator(task.MoveInferior.ID)
			if op != nil {
				log.Info("operator is running, skip")
				continue
			}
			// move should from working state
			value := working[task.MoveInferior.ID]
			target := task.MoveInferior.DestCapture
			oldState := value.State
			oldPrimary := value.Primary
			value.HandleMoveInferior(target)
			c.tryMoveTask(value.ID, value, oldState, oldPrimary, false)
		}
	}
}

func (c *Controller) NeedSchedule() bool {
	return c.GetTaskSizeByState(scheduler.SchedulerStatusAbsent) > 0
}

// ScheduleFinished return false if not all task are running in working state
func (c *Controller) ScheduleFinished() bool {
	c.pendingTaskLock.Lock()
	defer c.pendingTaskLock.Unlock()
	return c.TaskSize() == c.GetTaskSizeByState(scheduler.SchedulerStatusWorking) &&
		c.oc.OperatorSize() == 0 &&
		len(c.pendingTasks) == 0
}

func (c *Controller) GetSchedulingMessages() []*messaging.TargetMessage {
	var msgs = make([]*messaging.TargetMessage, 0, c.batchSize)
	resend := func(m map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]) {
		for _, value := range m {
			if msg := value.GetSchedulingMessage(); msg != nil {
				msgs = append(msgs, msg)
			}
			if len(msgs) >= c.batchSize {
				return
			}
		}
	}
	resend(c.Commiting())
	resend(c.Removing())
	return msgs
}

func (c *Controller) HandleStatus(from node.ID, statusList []*heartbeatpb.TableSpanStatus) {
	stMap, ok := c.nodeTasks[from]
	if !ok {
		log.Warn("no server id found, ignore",
			zap.String("changefeed", c.changefeedID),
			zap.Any("from", from))
		return
	}
	for _, status := range statusList {
		span := common.NewDispatcherIDFromPB(status.ID)
		stm, ok := stMap[span]
		if !ok {
			log.Warn("no statemachine id found, ignore",
				zap.String("changefeed", c.changefeedID),
				zap.Any("from", from),
				zap.String("span", span.String()))
			continue
		}
		oldState := stm.State
		oldPrimary := stm.Primary
		stm.HandleInferiorStatus(status.ComponentStatus, status, from)
		c.tryMoveTask(span, stm, oldState, oldPrimary, true)
	}
}

func (c *Controller) TaskSize() int {
	size := 0
	for _, m := range c.totalMaps {
		size += len(m)
	}
	return size
}

func (c *Controller) Absent() map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID] {
	return c.getTaskByState(scheduler.SchedulerStatusAbsent)
}

func (c *Controller) Commiting() map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID] {
	return c.getTaskByState(scheduler.SchedulerStatusCommiting)
}

func (c *Controller) Working() map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID] {
	return c.getTaskByState(scheduler.SchedulerStatusWorking)
}

func (c *Controller) Removing() map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID] {
	return c.getTaskByState(scheduler.SchedulerStatusRemoving)
}

func (c *Controller) getTaskByState(state scheduler.SchedulerStatus) map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID] {
	return c.totalMaps[state]
}

func (c *Controller) GetTaskSizeByState(state scheduler.SchedulerStatus) int {
	return len(c.getTaskByState(state))
}

func (c *Controller) GetTaskSizeByNodeID(id node.ID) int {
	sm, ok := c.nodeTasks[id]
	if ok {
		return len(sm)
	}
	return 0
}

func (c *Controller) addDDLDispatcher() {
	ddlTableSpan := heartbeatpb.DDLSpan
	c.addNewSpans(heartbeatpb.DDLSpanSchemaID, ddlTableSpan.TableID,
		[]*heartbeatpb.TableSpan{ddlTableSpan}, c.startCheckpointTs)
	var dispatcherID common.DispatcherID
	for id := range c.schemaTasks[heartbeatpb.DDLSpanSchemaID] {
		dispatcherID = id
	}
	c.ddlDispatcherID = dispatcherID
	log.Info("create table event trigger dispatcher",
		zap.String("changefeed", c.changefeedID),
		zap.String("dispatcher", dispatcherID.String()))
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
		c.Working()[dispatcherID] = stm
		c.nodeTasks[stm.Primary][dispatcherID] = stm
		if span.TableID == 0 {
			ddlSpanFound = true
			c.ddlDispatcherID = dispatcherID
		}
		return true
	})
	return ddlSpanFound
}

func (c *Controller) addNewSpans(schemaID, tableID int64,
	tableSpans []*heartbeatpb.TableSpan, startTs uint64) {
	for _, newSpan := range tableSpans {
		dispatcherID := common.NewDispatcherID()
		replicaSet := NewReplicaSet(model.DefaultChangeFeedID(c.changefeedID),
			dispatcherID, schemaID, newSpan, startTs).(*ReplicaSet)
		stm := scheduler.NewStateMachine(dispatcherID, nil, replicaSet)
		c.Absent()[dispatcherID] = stm
		// modify the schema map
		schemaMap, ok := c.schemaTasks[schemaID]
		if !ok {
			schemaMap = make(map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID])
			c.schemaTasks[schemaID] = schemaMap
		}
		schemaMap[dispatcherID] = stm

		// modify the table map
		tableMap, ok := c.tableTasks[tableID]
		if !ok {
			tableMap = make(map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID])
			c.tableTasks[tableID] = tableMap
		}
		tableMap[dispatcherID] = stm
	}
}

// tryMoveTask moves the StateMachine to the right map and modified the node map if changed
func (c *Controller) tryMoveTask(dispatcherID common.DispatcherID,
	stm *scheduler.StateMachine[common.DispatcherID],
	oldSate scheduler.SchedulerStatus,
	oldPrimary node.ID,
	modifyNodeMap bool) {
	if oldSate != stm.State {
		delete(c.totalMaps[oldSate], dispatcherID)
		c.totalMaps[stm.State][dispatcherID] = stm
	}
	// state machine is remove after state changed, remove from all maps
	// if removed, new primary node must be empty so we also can
	// update nodesMap if modifyNodeMap is true
	if stm.HasRemoved() {
		for _, m := range c.totalMaps {
			delete(m, dispatcherID)
		}
		delete(c.schemaTasks[stm.Inferior.(*ReplicaSet).SchemaID], dispatcherID)
		if len(c.schemaTasks[stm.Inferior.(*ReplicaSet).SchemaID]) == 0 {
			delete(c.schemaTasks, stm.Inferior.(*ReplicaSet).SchemaID)
		}
		delete(c.tableTasks[stm.Inferior.(*ReplicaSet).Span.TableID], dispatcherID)
		if len(c.tableTasks[stm.Inferior.(*ReplicaSet).Span.TableID]) == 0 {
			delete(c.tableTasks, stm.Inferior.(*ReplicaSet).Span.TableID)
		}
		// really deleted, remove from replacing map and clear operator
		if _, ok := c.replacing[dispatcherID]; !ok {
			c.oc.RemoveOperator(dispatcherID)
		}
	}
	// keep node task map is updated
	if modifyNodeMap && oldPrimary != stm.Primary {
		taskMap, ok := c.nodeTasks[oldPrimary]
		if ok {
			delete(taskMap, dispatcherID)
		}
		taskMap, ok = c.nodeTasks[stm.Primary]
		if ok {
			taskMap[dispatcherID] = stm
		}
	}
}
