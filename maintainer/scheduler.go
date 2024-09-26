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
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/maintainer/split"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/utils"
	"github.com/flowbehappy/tigate/utils/heap"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/spanz"
	"go.uber.org/zap"
)

// Scheduler schedules and balance tables
type Scheduler struct {
	//  initialTables hold all tables that before scheduler bootstrapped
	initialTables []common.Table
	// group the tasks by nodes
	nodeTasks map[node.ID]map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]
	// group the tasks by schema id
	schemaTasks map[int64]map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]
	// tables
	tableTasks map[int64]struct{}
	// totalMaps holds all state maps, absent, committing, working and removing
	totalMaps    []map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]
	bootstrapped bool

	splitter               *split.Splitter
	spanReplicationEnabled bool
	startCheckpointTs      uint64
	ddlDispatcherID        common.DispatcherID

	changefeedID         string
	batchSize            int
	random               *rand.Rand
	lastRebalanceTime    time.Time
	checkBalanceInterval time.Duration
}

func NewScheduler(changefeedID string,
	checkpointTs uint64,
	pdapi pdutil.PDAPIClient,
	regionCache split.RegionCache,
	config *config.ChangefeedSchedulerConfig,
	batchSize int, balanceInterval time.Duration) *Scheduler {
	s := &Scheduler{
		nodeTasks:            make(map[node.ID]map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]),
		schemaTasks:          make(map[int64]map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]),
		tableTasks:           make(map[int64]struct{}),
		startCheckpointTs:    checkpointTs,
		changefeedID:         changefeedID,
		bootstrapped:         false,
		batchSize:            batchSize,
		random:               rand.New(rand.NewSource(time.Now().UnixNano())),
		checkBalanceInterval: balanceInterval,
		lastRebalanceTime:    time.Now(),
	}
	if config != nil && config.EnableTableAcrossNodes {
		s.splitter = split.NewSplitter(changefeedID, pdapi, regionCache, config)
		s.spanReplicationEnabled = true
	}
	// put all maps to totalMaps
	s.totalMaps = make([]map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID], 4)
	s.totalMaps[scheduler.SchedulerStatusAbsent] = make(map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID])
	s.totalMaps[scheduler.SchedulerStatusCommiting] = make(map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID])
	s.totalMaps[scheduler.SchedulerStatusWorking] = make(map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID])
	s.totalMaps[scheduler.SchedulerStatusRemoving] = make(map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID])
	return s
}

func (s *Scheduler) GetTasksBySchemaID(schemaID int64) map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID] {
	return s.schemaTasks[schemaID]
}

func (s *Scheduler) GetAllNodes() []node.ID {
	var nodes = make([]node.ID, 0, len(s.nodeTasks))
	for id := range s.nodeTasks {
		nodes = append(nodes, id)
	}
	return nodes
}

func (s *Scheduler) AddNewTable(table common.Table, startTs uint64) {
	_, ok := s.tableTasks[table.TableID]
	if ok {
		log.Warn("table already add, ignore",
			zap.String("changefeed", s.changefeedID),
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
	if s.spanReplicationEnabled {
		//split the whole table span base on the configuration, todo: background split table
		tableSpans = s.splitter.SplitSpans(context.Background(), tableSpan, len(s.nodeTasks))
	}
	s.addNewSpans(table.SchemaID, tableSpan.TableID, tableSpans, startTs)
}

func (s *Scheduler) SetInitialTables(tables []common.Table) {
	s.initialTables = tables
}

// FinishBootstrap adds working state tasks to this scheduler directly,
// it reported by the bootstrap response
func (s *Scheduler) FinishBootstrap(workingMap map[int64]utils.Map[*heartbeatpb.TableSpan, *scheduler.StateMachine[common.DispatcherID]]) {
	if s.bootstrapped {
		log.Panic("already bootstrapped",
			zap.String("changefeed", s.changefeedID),
			zap.Any("workingMap", workingMap))
	}
	for _, table := range s.initialTables {
		tableMap, ok := workingMap[table.TableID]
		if !ok {
			s.AddNewTable(table, s.startCheckpointTs)
		} else {
			span := spanz.TableIDToComparableSpan(table.TableID)
			tableSpan := &heartbeatpb.TableSpan{
				TableID:  table.TableID,
				StartKey: span.StartKey,
				EndKey:   span.EndKey,
			}
			log.Info("table already working in other server",
				zap.String("changefeed", s.changefeedID),
				zap.Int64("tableID", table.TableID))
			s.addWorkingSpans(tableMap)
			if s.spanReplicationEnabled {
				holes := split.FindHoles(tableMap, tableSpan)
				// todo: split the hole
				s.addNewSpans(table.SchemaID, table.TableID, holes, s.startCheckpointTs)
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
			zap.String("changefeed", s.changefeedID),
			zap.Int64("id", tableID))
		if s.addWorkingSpans(tableMap) {
			ddlSpanFound = true
		}
	}

	// add a table_event_trigger dispatcher if not found
	if !ddlSpanFound {
		s.addDDLDispatcher()
	}
	s.bootstrapped = true
	s.initialTables = nil
}

// GetTask queries a task by dispatcherID, return nil if not found
func (s *Scheduler) GetTask(dispatcherID common.DispatcherID) *scheduler.StateMachine[common.DispatcherID] {
	var stm *scheduler.StateMachine[common.DispatcherID]
	var ok bool
	for _, m := range s.totalMaps {
		stm, ok = m[dispatcherID]
		if ok {
			break
		}
	}
	return stm
}

func (s *Scheduler) RemoveAllTasks() {
	for _, m := range s.totalMaps {
		for _, stm := range m {
			s.RemoveTask(stm)
		}
	}
}

func (s *Scheduler) RemoveTask(stm *scheduler.StateMachine[common.DispatcherID]) {
	if stm == nil {
		log.Warn("dispatcher is not found",
			zap.String("cf", s.changefeedID),
			zap.Any("dispatcherID", s.changefeedID))
		return
	}
	if stm.State == scheduler.SchedulerStatusAbsent {
		replica := stm.Inferior.(*ReplicaSet)
		id := replica.ID
		log.Info("remove a absent task",
			zap.String("changefeed", s.changefeedID),
			zap.String("id", id.String()))
		delete(s.Absent(), id)
		delete(s.schemaTasks[replica.SchemaID], id)
		delete(s.tableTasks, replica.Span.TableID)
		return
	}
	oldState := stm.State
	oldPrimary := stm.Primary
	stm.HandleRemoveInferior()
	s.tryMoveTask(stm.ID, stm, oldState, oldPrimary, true)
}

func (s *Scheduler) GetTasksByTableIDs(tableIDs ...int64) []*scheduler.StateMachine[common.DispatcherID] {
	tableMap := make(map[int64]bool, len(tableIDs))
	for _, tableID := range tableIDs {
		tableMap[tableID] = true
	}
	var stms []*scheduler.StateMachine[common.DispatcherID]
	for _, m := range s.totalMaps {
		for _, stm := range m {
			replica := stm.Inferior.(*ReplicaSet)
			if !tableMap[replica.Span.TableID] {
				continue
			}
			stms = append(stms, stm)
		}
	}
	return stms
}

func (s *Scheduler) AddNewNode(id node.ID) {
	_, ok := s.nodeTasks[id]
	if ok {
		log.Info("node already exists",
			zap.String("changefeed", s.changefeedID),
			zap.Any("node", id))
		return
	}
	log.Info("add new node",
		zap.String("changefeed", s.changefeedID),
		zap.Any("node", id))
	s.nodeTasks[id] = make(map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID])
}

func (s *Scheduler) RemoveNode(id node.ID) {
	stmMap, ok := s.nodeTasks[id]
	if !ok {
		log.Info("node is maintained by scheduler, ignore",
			zap.String("changefeed", s.changefeedID),
			zap.Any("node", id))
		return
	}
	for key, value := range stmMap {
		oldState := value.State
		value.HandleCaptureShutdown(id)
		if value.Primary != "" && value.Primary != id {
			delete(s.nodeTasks[value.Primary], key)
		}
		s.tryMoveTask(key, value, oldState, id, false)
	}
	// check removing map, maybe some task are moving to this node
	for key, value := range s.Removing() {
		if value.Secondary == id {
			oldState := value.State
			value.HandleCaptureShutdown(id)
			s.tryMoveTask(key, value, oldState, id, false)
		}
	}
	delete(s.nodeTasks, id)
}

func (s *Scheduler) Schedule() {
	if len(s.nodeTasks) == 0 {
		log.Warn("scheduler has no node tasks", zap.String("changeeed", s.changefeedID))
		return
	}
	if s.NeedSchedule() {
		s.basicSchedule()
		return
	}
	// we scheduled all absent tasks, try to balance it if needed
	s.tryBalance()
}

func (s *Scheduler) NeedSchedule() bool {
	return s.GetTaskSizeByState(scheduler.SchedulerStatusAbsent) > 0
}

// ScheduleFinished return false if not all task are running in working state
func (s *Scheduler) ScheduleFinished() bool {
	return s.TaskSize() == s.GetTaskSizeByState(scheduler.SchedulerStatusWorking)
}

func (s *Scheduler) tryBalance() {
	if !s.ScheduleFinished() {
		// not in stable schedule state, skip balance
		return
	}
	now := time.Now()
	if now.Sub(s.lastRebalanceTime) < s.checkBalanceInterval {
		// skip balance.
		return
	}
	s.balanceTables()
}

func (s *Scheduler) basicSchedule() {
	totalSize := s.batchSize - len(s.Removing()) - len(s.Commiting())
	if totalSize <= 0 {
		// too many running tasks, skip schedule
		return
	}
	priorityQueue := heap.NewHeap[*Item]()
	for key, m := range s.nodeTasks {
		priorityQueue.AddOrUpdate(&Item{
			Node:     key,
			TaskSize: len(m),
		})
	}

	taskSize := 0
	absent := s.Absent()
	for key, value := range absent {
		item, _ := priorityQueue.PeekTop()
		value.HandleAddInferior(item.Node)

		s.Commiting()[key] = value
		s.nodeTasks[item.Node][key] = value
		delete(absent, key)

		item.TaskSize++
		priorityQueue.AddOrUpdate(item)
		taskSize++
		if taskSize >= totalSize {
			break
		}
	}
}

func (s *Scheduler) GetSchedulingMessages() []*messaging.TargetMessage {
	var msgs = make([]*messaging.TargetMessage, 0, s.batchSize)
	resend := func(m map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]) {
		for _, value := range m {
			if msg := value.GetSchedulingMessage(); msg != nil {
				msgs = append(msgs, msg)
			}
			if len(msgs) >= s.batchSize {
				return
			}
		}
	}
	resend(s.Commiting())
	resend(s.Removing())
	return msgs
}

func (s *Scheduler) balanceTables() {
	upperLimitPerCapture := int(math.Ceil(float64(s.TaskSize()) / float64(len(s.nodeTasks))))
	// victims holds tables which need to be moved
	victims := make([]*scheduler.StateMachine[common.DispatcherID], 0)
	priorityQueue := heap.NewHeap[*Item]()
	for nodeID, ts := range s.nodeTasks {
		var changefeeds []*scheduler.StateMachine[common.DispatcherID]
		for _, value := range ts {
			changefeeds = append(changefeeds, value)
		}
		if s.random != nil {
			// Complexity note: Shuffle has O(n), where `n` is the number of tables.
			// Also, during a single call of `Schedule`, Shuffle can be called at most
			// `c` times, where `c` is the number of captures (TiCDC nodes).
			// Only called when a rebalance is triggered, which happens rarely,
			// we do not expect a performance degradation as a result of adding
			// the randomness.
			s.random.Shuffle(len(changefeeds), func(i, j int) {
				changefeeds[i], changefeeds[j] = changefeeds[j], changefeeds[i]
			})
		} else {
			// sort the spans here so that the result is deterministic,
			// which would aid testing and debugging.
			sort.Slice(changefeeds, func(i, j int) bool {
				return changefeeds[i].ID.String() < changefeeds[j].ID.String()
			})
		}

		tableNum2Remove := len(changefeeds) - upperLimitPerCapture
		if tableNum2Remove <= 0 {
			priorityQueue.AddOrUpdate(&Item{
				Node:     nodeID,
				TaskSize: len(ts),
			})
			continue
		} else {
			priorityQueue.AddOrUpdate(&Item{
				Node:     nodeID,
				TaskSize: len(ts) - tableNum2Remove,
			})
		}

		for _, cf := range changefeeds {
			if tableNum2Remove <= 0 {
				break
			}
			victims = append(victims, cf)
			tableNum2Remove--
		}
	}
	if len(victims) == 0 {
		return
	}

	movedSize := 0
	// for each victim table, find the target for it
	for idx, cf := range victims {
		if idx >= s.batchSize {
			// We have reached the task limit.
			break
		}

		item, _ := priorityQueue.PeekTop()
		target := item.Node
		oldState := cf.State
		oldPrimary := cf.Primary
		cf.HandleMoveInferior(target)
		s.tryMoveTask(cf.ID, cf, oldState, oldPrimary, false)
		// update the task size priority queue
		item.TaskSize++
		priorityQueue.AddOrUpdate(item)
		movedSize++
	}
	if movedSize >= len(victims) {
		s.lastRebalanceTime = time.Now()
		log.Info("balance done",
			zap.String("changefeed", s.changefeedID))
	}
	log.Info("balance scheduled",
		zap.String("changefeed", s.changefeedID),
		zap.Int("movedSize", movedSize),
		zap.Int("victims", len(victims)))
}

func (s *Scheduler) HandleStatus(from node.ID, statusList []*heartbeatpb.TableSpanStatus) {
	stMap, ok := s.nodeTasks[from]
	if !ok {
		log.Warn("no server id found, ignore",
			zap.String("changefeed", s.changefeedID),
			zap.Any("from", from))
		return
	}
	for _, status := range statusList {
		span := common.NewDispatcherIDFromPB(status.ID)
		stm, ok := stMap[span]
		if !ok {
			log.Warn("no statemachine id found, ignore",
				zap.String("changefeed", s.changefeedID),
				zap.Any("from", from),
				zap.String("span", span.String()))
			continue
		}
		oldState := stm.State
		oldPrimary := stm.Primary
		stm.HandleInferiorStatus(status.ComponentStatus, status, from)
		s.tryMoveTask(span, stm, oldState, oldPrimary, true)
	}
}

func (s *Scheduler) TaskSize() int {
	size := 0
	for _, m := range s.totalMaps {
		size += len(m)
	}
	return size
}

func (s *Scheduler) Absent() map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID] {
	return s.getTaskByState(scheduler.SchedulerStatusAbsent)
}

func (s *Scheduler) Commiting() map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID] {
	return s.getTaskByState(scheduler.SchedulerStatusCommiting)
}

func (s *Scheduler) Working() map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID] {
	return s.getTaskByState(scheduler.SchedulerStatusWorking)
}

func (s *Scheduler) Removing() map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID] {
	return s.getTaskByState(scheduler.SchedulerStatusRemoving)
}

func (s *Scheduler) getTaskByState(state scheduler.SchedulerStatus) map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID] {
	return s.totalMaps[state]
}

func (s *Scheduler) GetTaskSizeByState(state scheduler.SchedulerStatus) int {
	return len(s.getTaskByState(state))
}

func (s *Scheduler) GetTaskSizeByNodeID(id node.ID) int {
	sm, ok := s.nodeTasks[id]
	if ok {
		return len(sm)
	}
	return 0
}

func (s *Scheduler) addDDLDispatcher() {
	ddlTableSpan := heartbeatpb.DDLSpan
	s.addNewSpans(heartbeatpb.DDLSpanSchemaID, ddlTableSpan.TableID,
		[]*heartbeatpb.TableSpan{ddlTableSpan}, s.startCheckpointTs)
	var dispatcherID common.DispatcherID
	for id := range s.schemaTasks[heartbeatpb.DDLSpanSchemaID] {
		dispatcherID = id
	}
	s.ddlDispatcherID = dispatcherID
	log.Info("create table event trigger dispatcher",
		zap.String("changefeed", s.changefeedID),
		zap.String("dispatcher", dispatcherID.String()))
}

func (s *Scheduler) addWorkingSpans(tableMap utils.Map[*heartbeatpb.TableSpan, *scheduler.StateMachine[common.DispatcherID]]) bool {
	ddlSpanFound := false
	tableMap.Ascend(func(span *heartbeatpb.TableSpan, stm *scheduler.StateMachine[common.DispatcherID]) bool {
		if stm.State != scheduler.SchedulerStatusWorking {
			log.Panic("unexpected state",
				zap.String("changefeed", s.changefeedID),
				zap.Any("stm", stm))
		}
		dispatcherID := stm.ID
		s.Working()[dispatcherID] = stm
		s.nodeTasks[stm.Primary][dispatcherID] = stm
		if span.TableID == 0 {
			ddlSpanFound = true
			s.ddlDispatcherID = dispatcherID
		}
		return true
	})
	return ddlSpanFound
}

func (s *Scheduler) addNewSpans(schemaID, tableID int64,
	tableSpans []*heartbeatpb.TableSpan, startTs uint64) {
	for _, newSpan := range tableSpans {
		newTableSpan := &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: newSpan.StartKey,
			EndKey:   newSpan.EndKey,
		}
		dispatcherID := common.NewDispatcherID()
		replicaSet := NewReplicaSet(model.DefaultChangeFeedID(s.changefeedID),
			dispatcherID, schemaID, newTableSpan, startTs).(*ReplicaSet)
		stm := scheduler.NewStateMachine(dispatcherID, nil, replicaSet)
		s.Absent()[dispatcherID] = stm
		schemaMap, ok := s.schemaTasks[schemaID]
		if !ok {
			schemaMap = make(map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID])
			s.schemaTasks[schemaID] = schemaMap
		}
		schemaMap[dispatcherID] = stm
		s.tableTasks[tableID] = struct{}{}
	}
}

// tryMoveTask moves the StateMachine to the right map and modified the node map if changed
func (s *Scheduler) tryMoveTask(dispatcherID common.DispatcherID,
	stm *scheduler.StateMachine[common.DispatcherID],
	oldSate scheduler.SchedulerStatus,
	oldPrimary node.ID,
	modifyNodeMap bool) {
	if oldSate != stm.State {
		delete(s.totalMaps[oldSate], dispatcherID)
		s.totalMaps[stm.State][dispatcherID] = stm
	}
	// state machine is remove after state changed, remove from all maps
	// if removed, new primary node must be empty so we also can
	// update nodesMap if modifyNodeMap is true
	if stm.HasRemoved() {
		for _, m := range s.totalMaps {
			delete(m, dispatcherID)
		}
		delete(s.schemaTasks[stm.Inferior.(*ReplicaSet).SchemaID], dispatcherID)
		delete(s.tableTasks, stm.Inferior.(*ReplicaSet).Span.TableID)
	}
	// keep node task map is updated
	if modifyNodeMap && oldPrimary != stm.Primary {
		taskMap, ok := s.nodeTasks[oldPrimary]
		if ok {
			delete(taskMap, dispatcherID)
		}
		taskMap, ok = s.nodeTasks[stm.Primary]
		if ok {
			taskMap[dispatcherID] = stm
		}
	}
}

type Item struct {
	Node     node.ID
	TaskSize int
	index    int
}

func (i *Item) SetHeapIndex(idx int) {
	i.index = idx
}

func (i *Item) GetHeapIndex() int {
	return i.index
}

func (i *Item) CompareTo(t *Item) int {
	return i.TaskSize - t.TaskSize
}
