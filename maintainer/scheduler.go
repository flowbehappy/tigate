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
	// absent track all table spans that need to be scheduled
	// when dispatcher reported a new table or remove a table, this field should be updated
	absent map[common.DispatcherID]*scheduler.StateMachine

	// removing and committing are the task that waiting for response
	removing   map[common.DispatcherID]*scheduler.StateMachine
	committing map[common.DispatcherID]*scheduler.StateMachine
	// working map hold all task that remote reported work state, will not schedule it
	working map[common.DispatcherID]*scheduler.StateMachine

	nodeTasks   map[string]map[common.DispatcherID]*scheduler.StateMachine
	schemaTasks map[int64]map[common.DispatcherID]*scheduler.StateMachine
	// totalMaps holds all state maps, absent, committing, working and removing
	totalMaps    []map[common.DispatcherID]*scheduler.StateMachine
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
		committing:           make(map[common.DispatcherID]*scheduler.StateMachine),
		removing:             make(map[common.DispatcherID]*scheduler.StateMachine),
		working:              make(map[common.DispatcherID]*scheduler.StateMachine),
		absent:               make(map[common.DispatcherID]*scheduler.StateMachine),
		nodeTasks:            make(map[string]map[common.DispatcherID]*scheduler.StateMachine),
		schemaTasks:          make(map[int64]map[common.DispatcherID]*scheduler.StateMachine),
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
	s.totalMaps = make([]map[common.DispatcherID]*scheduler.StateMachine, 4)
	s.totalMaps[scheduler.SchedulerStatusAbsent] = s.absent
	s.totalMaps[scheduler.SchedulerStatusCommiting] = s.committing
	s.totalMaps[scheduler.SchedulerStatusWorking] = s.working
	s.totalMaps[scheduler.SchedulerStatusRemoving] = s.removing
	return s
}

func (s *Scheduler) AddNewTable(table common.Table) {
	span := spanz.TableIDToComparableSpan(table.TableID)
	tableSpan := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{
		TableID:  uint64(table.TableID),
		StartKey: span.StartKey,
		EndKey:   span.EndKey,
	}}
	tableSpans := []*common.TableSpan{tableSpan}
	if s.spanReplicationEnabled {
		//split the whole table span base on the configuration, todo: background split table
		tableSpans = s.splitter.SplitSpans(context.Background(), tableSpan, len(s.nodeTasks))
	}
	s.addNewSpans(table.SchemaID, int64(tableSpan.TableID), tableSpans)
}

func (s *Scheduler) SetInitialTables(tables []common.Table) {
	s.initialTables = tables
}

// FinishBootstrap adds working state tasks to this scheduler directly,
// it reported by the bootstrap response
func (s *Scheduler) FinishBootstrap(workingMap map[uint64]utils.Map[*common.TableSpan, *scheduler.StateMachine]) {
	if s.bootstrapped {
		log.Panic("already bootstrapped",
			zap.String("changefeed", s.changefeedID),
			zap.Any("workingMap", workingMap))
	}
	for _, table := range s.initialTables {
		tableMap, ok := workingMap[uint64(table.TableID)]
		if !ok {
			s.AddNewTable(table)
		} else {
			span := spanz.TableIDToComparableSpan(table.TableID)
			tableSpan := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{
				TableID:  uint64(table.TableID),
				StartKey: span.StartKey,
				EndKey:   span.EndKey,
			}}
			log.Info("table already working in other server",
				zap.String("changefeed", s.changefeedID),
				zap.Int64("tableID", table.TableID))
			s.addWorkingSpans(tableMap)
			if s.spanReplicationEnabled {
				holes := split.FindHoles(tableMap, tableSpan)
				// todo: split the hole
				s.addNewSpans(table.SchemaID, table.TableID, holes)
			}
			// delete it
			delete(workingMap, uint64(table.TableID))
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
			zap.Uint64("id", tableID))
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
func (s *Scheduler) GetTask(dispatcherID common.DispatcherID) *scheduler.StateMachine {
	var stm *scheduler.StateMachine
	var ok bool
	for _, m := range s.totalMaps {
		stm, ok = m[dispatcherID]
		if ok {
			break
		}
	}
	return stm
}

// RemoveTask removes task by dispatcherID
func (s *Scheduler) RemoveTask(dispatcherID common.DispatcherID) *messaging.TargetMessage {
	var stm = s.GetTask(dispatcherID)
	if stm == nil {
		log.Warn("dispatcher is not found",
			zap.String("cf", s.changefeedID),
			zap.Any("dispatcherID", s.changefeedID))
		return nil
	}
	oldState := stm.State
	oldPrimary := stm.Primary
	msg := stm.HandleRemoveInferior()
	s.tryMoveTask(stm.ID.(common.DispatcherID), stm, oldState, oldPrimary, true)
	return msg
}

func (s *Scheduler) AddNewNode(id string) {
	_, ok := s.nodeTasks[id]
	if ok {
		log.Info("node already exists",
			zap.String("changeeed", s.changefeedID),
			zap.String("node", id))
		return
	}
	log.Info("add new node",
		zap.String("changeeed", s.changefeedID),
		zap.String("node", id))
	s.nodeTasks[id] = make(map[common.DispatcherID]*scheduler.StateMachine)
}

func (s *Scheduler) RemoveNode(nodeId string) []*messaging.TargetMessage {
	stmMap, ok := s.nodeTasks[nodeId]
	if !ok {
		log.Info("node is maintained by scheduler, ignore",
			zap.String("changeeed", s.changefeedID),
			zap.String("node", nodeId))
		return nil
	}
	var msgs []*messaging.TargetMessage
	for key, value := range stmMap {
		oldState := value.State
		msg, _ := value.HandleCaptureShutdown(nodeId)
		if msg != nil {
			msgs = append(msgs, msg)
		}
		if value.Primary != "" && value.Primary != nodeId {
			delete(s.nodeTasks[value.Primary], key)
		}
		s.tryMoveTask(key, value, oldState, nodeId, false)
	}
	// check removing map, maybe some task are moving to this node
	for key, value := range s.removing {
		if value.Secondary == nodeId {
			oldState := value.State
			msg, _ := value.HandleCaptureShutdown(nodeId)
			if msg != nil {
				msgs = append(msgs, msg)
			}
			s.tryMoveTask(key, value, oldState, nodeId, false)
		}
	}
	delete(s.nodeTasks, nodeId)
	return msgs
}

func (s *Scheduler) Schedule() []*messaging.TargetMessage {
	if len(s.nodeTasks) == 0 {
		log.Warn("scheduler has no node tasks", zap.String("changeeed", s.changefeedID))
		return nil
	}
	if !s.NeedSchedule() {
		return nil
	}
	totalSize := s.batchSize - len(s.committing) - len(s.removing)
	if totalSize <= 0 {
		// too many running tasks, skip schedule
		return nil
	}
	priorityQueue := heap.NewHeap[*Item]()
	for key, m := range s.nodeTasks {
		priorityQueue.AddOrUpdate(&Item{
			Node:     key,
			TaskSize: len(m),
		})
	}

	taskSize := 0
	var msgs = make([]*messaging.TargetMessage, 0, s.batchSize)
	for key, value := range s.absent {
		item, _ := priorityQueue.PeekTop()
		msg := value.HandleAddInferior(item.Node)
		if msg != nil {
			msgs = append(msgs, msg)
		}

		s.committing[key] = value
		s.nodeTasks[item.Node][key] = value
		delete(s.absent, key)

		item.TaskSize++
		priorityQueue.AddOrUpdate(item)
		taskSize++
		if taskSize >= totalSize {
			break
		}
	}
	return msgs
}

func (s *Scheduler) NeedSchedule() bool {
	return len(s.absent) > 0
}

func (s *Scheduler) ScheduleFinished() bool {
	return len(s.absent) == 0 && len(s.committing) == 0 && len(s.removing) == 0
}

func (s *Scheduler) TryBalance() []*messaging.TargetMessage {
	if !s.ScheduleFinished() {
		// not in stable schedule state, skip balance
		return nil
	}
	now := time.Now()
	if now.Sub(s.lastRebalanceTime) < s.checkBalanceInterval {
		// skip balance.
		return nil
	}
	s.lastRebalanceTime = now
	return s.balanceTables()
}

func (s *Scheduler) ResendMessage() []*messaging.TargetMessage {
	var msgs []*messaging.TargetMessage
	resend := func(m map[common.DispatcherID]*scheduler.StateMachine) {
		for _, value := range s.committing {
			if msg := value.HandleResend(); msg != nil {
				msgs = append(msgs, msg)
			}
		}
	}
	resend(s.committing)
	resend(s.removing)
	return msgs
}

func (s *Scheduler) balanceTables() []*messaging.TargetMessage {
	var messages = make([]*messaging.TargetMessage, 0)
	upperLimitPerCapture := int(math.Ceil(float64(len(s.working)) / float64(len(s.nodeTasks))))
	// victims holds tables which need to be moved
	victims := make([]*scheduler.StateMachine, 0)
	priorityQueue := heap.NewHeap[*Item]()
	for nodeID, ts := range s.nodeTasks {
		var changefeeds []*scheduler.StateMachine
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
		return nil
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
		msg := cf.HandleMoveInferior(target)
		messages = append(messages, msg)
		s.tryMoveTask(cf.ID.(common.DispatcherID), cf, oldState, oldPrimary, false)
		// update the task size priority queue
		item.TaskSize++
		priorityQueue.AddOrUpdate(item)
		movedSize++
	}
	log.Info("balance done",
		zap.String("changefeed", s.changefeedID),
		zap.Int("movedSize", movedSize),
		zap.Int("victims", len(victims)))
	return messages
}

func (s *Scheduler) HandleStatus(from string, statusList []*heartbeatpb.TableSpanStatus) []*messaging.TargetMessage {
	stMap, ok := s.nodeTasks[from]
	if !ok {
		log.Warn("no server id found, ignore",
			zap.String("changeeed", s.changefeedID),
			zap.String("from", from))
		return nil
	}
	var msgs = make([]*messaging.TargetMessage, 0)
	for _, status := range statusList {
		span := common.NewDispatcherIDFromPB(status.ID)
		stm, ok := stMap[span]
		if !ok {
			log.Warn("no statemachine id found, ignore",
				zap.String("changeeed", s.changefeedID),
				zap.String("from", from),
				zap.String("span", span.String()))
			continue
		}
		oldState := stm.State
		oldPrimary := stm.Primary
		var sch scheduler.InferiorStatus = ReplicaSetStatus{
			ID:           span,
			State:        status.ComponentStatus,
			CheckpointTs: status.CheckpointTs,
			DDLStatus:    status.State,
		}
		msg := stm.HandleInferiorStatus(sch, from)
		if msg != nil {
			msgs = append(msgs, msg)
		}
		s.tryMoveTask(span, stm, oldState, oldPrimary, true)
	}
	return msgs
}

func (s *Scheduler) TaskSize() int {
	return len(s.committing) + len(s.working) + len(s.absent) + len(s.removing)
}

func (s *Scheduler) GetTaskSizeByState(state scheduler.SchedulerStatus) int {
	return len(s.totalMaps[state])
}

func (s *Scheduler) GetTaskSizeByNodeID(nodeID string) int {
	sm, ok := s.nodeTasks[nodeID]
	if ok {
		return len(sm)
	}
	return 0
}

func (s *Scheduler) addDDLDispatcher() {
	ddlTableSpan := common.DDLSpan
	s.addNewSpans(common.DDLSpanSchemaID, int64(ddlTableSpan.TableID), []*common.TableSpan{ddlTableSpan})
	var dispatcherID common.DispatcherID
	for id := range s.schemaTasks[common.DDLSpanSchemaID] {
		dispatcherID = id
	}
	s.ddlDispatcherID = dispatcherID
	log.Info("create table event trigger dispatcher",
		zap.String("changefeed", s.changefeedID),
		zap.String("dispatcher", dispatcherID.String()))
}

func (s *Scheduler) addWorkingSpans(tableMap utils.Map[*common.TableSpan, *scheduler.StateMachine]) bool {
	ddlSpanFound := false
	tableMap.Ascend(func(span *common.TableSpan, stm *scheduler.StateMachine) bool {
		if stm.State != scheduler.SchedulerStatusWorking {
			log.Panic("unexpected state",
				zap.String("changefeed", s.changefeedID),
				zap.Any("stm", stm))
		}
		dispatcherID := stm.ID.(common.DispatcherID)
		s.working[dispatcherID] = stm
		s.nodeTasks[stm.Primary][dispatcherID] = stm
		if span.TableID == 0 {
			ddlSpanFound = true
			s.ddlDispatcherID = dispatcherID
		}
		return true
	})
	return ddlSpanFound
}

func (s *Scheduler) addNewSpans(schemaID, tableID int64, tableSpans []*common.TableSpan) {
	for _, newSpan := range tableSpans {
		newTableSpan := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{
			TableID:  uint64(tableID),
			StartKey: newSpan.StartKey,
			EndKey:   newSpan.EndKey,
		}}
		dispatcherID := common.NewDispatcherID()
		replicaSet := NewReplicaSet(model.DefaultChangeFeedID(s.changefeedID),
			dispatcherID, schemaID, newTableSpan, s.startCheckpointTs).(*ReplicaSet)
		stm := scheduler.NewStateMachine(dispatcherID, nil, replicaSet)
		s.absent[dispatcherID] = stm
		schemaMap, ok := s.schemaTasks[schemaID]
		if !ok {
			schemaMap = make(map[common.DispatcherID]*scheduler.StateMachine)
			s.schemaTasks[schemaID] = schemaMap
		}
		schemaMap[dispatcherID] = stm
	}
}

// tryMoveTask moves the StateMachine to the right map and modified the node map if changed
func (s *Scheduler) tryMoveTask(dispatcherID common.DispatcherID,
	stm *scheduler.StateMachine,
	oldSate scheduler.SchedulerStatus,
	oldPrimary string,
	modifyNodeMap bool) {
	if oldSate != stm.State {
		delete(s.totalMaps[oldSate], dispatcherID)
		s.totalMaps[stm.State][dispatcherID] = stm
	}
	// state machine is remove after state changed, remove from all maps
	// if removed, new primary node must be empty so we also can
	// update nodesMap if modifyNodeMap is true
	if stm.HasRemoved() {
		delete(s.removing, dispatcherID)
		delete(s.absent, dispatcherID)
		delete(s.committing, dispatcherID)
		delete(s.working, dispatcherID)
		delete(s.schemaTasks[stm.Inferior.(*ReplicaSet).SchemaID], dispatcherID)
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
	Node     string
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
