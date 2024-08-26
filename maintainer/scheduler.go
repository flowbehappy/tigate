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
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/utils"
	"github.com/flowbehappy/tigate/utils/heap"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// Scheduler schedules tables
type Scheduler struct {
	// tempTasks hold all tasks that before scheduler bootstrapped, use the table span as the key
	// statemachine hold the temp dispatcher ID, if a working state task reported, change the dispatcher ID
	tempTasks utils.Map[*common.TableSpan, *scheduler.StateMachine]
	// absent track all table spans that need to be scheduled
	// when dispatcher reported a new table or remove a table, this field should be updated
	absent map[common.DispatcherID]*scheduler.StateMachine

	// schedulingTask list the task that wait for response
	schedulingTask map[common.DispatcherID]*scheduler.StateMachine
	working        map[common.DispatcherID]*scheduler.StateMachine

	nodeTasks    map[string]map[common.DispatcherID]*scheduler.StateMachine
	bootstrapped bool

	changefeedID         string
	batchSize            int
	random               *rand.Rand
	lastRebalanceTime    time.Time
	checkBalanceInterval time.Duration
}

func NewScheduler(changefeedID string,
	batchSize int, balanceInterval time.Duration) *Scheduler {
	return &Scheduler{
		tempTasks:            utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine](),
		schedulingTask:       make(map[common.DispatcherID]*scheduler.StateMachine),
		working:              make(map[common.DispatcherID]*scheduler.StateMachine),
		absent:               make(map[common.DispatcherID]*scheduler.StateMachine),
		nodeTasks:            make(map[string]map[common.DispatcherID]*scheduler.StateMachine),
		changefeedID:         changefeedID,
		bootstrapped:         false,
		batchSize:            batchSize,
		random:               rand.New(rand.NewSource(time.Now().UnixNano())),
		checkBalanceInterval: balanceInterval,
		lastRebalanceTime:    time.Now(),
	}
}

func (s *Scheduler) AddNewTask(stm *scheduler.StateMachine) {
	s.absent[stm.ID.(common.DispatcherID)] = stm
}

func (s *Scheduler) AddTempTask(span *common.TableSpan, stm *scheduler.StateMachine) {
	s.tempTasks.ReplaceOrInsert(span, stm)
}

// FinishBootstrap adds working state tasks to this scheduler directly,
// it reported by the bootstrap response
func (s *Scheduler) FinishBootstrap(stms utils.Map[*common.TableSpan, *scheduler.StateMachine]) {
	if s.bootstrapped {
		log.Panic("already bootstrapped",
			zap.String("changefeed", s.changefeedID),
			zap.Any("stms", stms))
	}
	stms.Ascend(func(span *common.TableSpan, stm *scheduler.StateMachine) bool {
		if stm.State != scheduler.SchedulerStatusWorking {
			log.Panic("unexpected state",
				zap.String("changefeed", s.changefeedID),
				zap.Any("stm", stm))
		}
		if !s.tempTasks.Has(span) {
			dispatcherID := stm.ID.(common.DispatcherID)
			s.working[dispatcherID] = stm
			s.nodeTasks[stm.Primary][dispatcherID] = stm
		} else {
			s.tempTasks.ReplaceOrInsert(span, stm)
			span := stm.ID.(common.DispatcherID)
			s.working[span] = stm
			delete(s.absent, span)
			s.nodeTasks[stm.Primary][span] = stm
			if _, ok := s.schedulingTask[span]; ok {
				log.Warn("span state not expected, remove from commiting",
					zap.String("changefeed", s.changefeedID),
					zap.String("span", span.String()))
				delete(s.schedulingTask, span)
			}
		}
		return true
	})
	s.tempTasks.Ascend(func(key *common.TableSpan, stm *scheduler.StateMachine) bool {
		span := stm.ID.(common.DispatcherID)
		s.absent[span] = stm
		s.nodeTasks[stm.Primary][span] = stm
		return true
	})
	s.bootstrapped = true
	s.tempTasks = nil
}

func (s *Scheduler) RemoveTask(stm *scheduler.StateMachine) (*messaging.TargetMessage, error) {
	oldState := stm.State
	oldPrimary := stm.Primary
	msg, err := stm.HandleRemoveInferior()
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.tryMoveTask(stm.ID.(common.DispatcherID), stm, oldState, oldPrimary, true)
	return msg, nil
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
	delete(s.nodeTasks, nodeId)
	return msgs
}

func (s *Scheduler) Schedule() ([]*messaging.TargetMessage, error) {
	if len(s.nodeTasks) == 0 {
		log.Warn("scheduler has no node tasks", zap.String("changeeed", s.changefeedID))
		return nil, nil
	}
	if !s.NeedSchedule() {
		return nil, nil
	}
	totalSize := s.batchSize - len(s.schedulingTask)
	if totalSize <= 0 {
		// too many running tasks, skip schedule
		return nil, nil
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
	var err error
	for key, value := range s.absent {
		item, _ := priorityQueue.PeekTop()
		msg, err := value.HandleAddInferior(item.Node)
		if err != nil {
			return nil, errors.Trace(err)
		}
		msgs = append(msgs, msg)

		s.schedulingTask[key] = value
		s.nodeTasks[item.Node][key] = value
		delete(s.absent, key)

		item.TaskSize++
		priorityQueue.AddOrUpdate(item)
		taskSize++
		if taskSize >= totalSize {
			break
		}
	}
	return msgs, err
}

func (s *Scheduler) NeedSchedule() bool {
	return len(s.absent) > 0
}

func (s *Scheduler) ScheduleFinished() bool {
	return len(s.absent) == 0 && len(s.schedulingTask) == 0
}

func (s *Scheduler) TryBalance() ([]*messaging.TargetMessage, error) {
	if len(s.absent) > 0 || len(s.schedulingTask) > 0 {
		// not in stable schedule state, skip balance
		return nil, nil
	}
	now := time.Now()
	if now.Sub(s.lastRebalanceTime) < s.checkBalanceInterval {
		// skip balance.
		return nil, nil
	}
	s.lastRebalanceTime = now
	return s.balanceTables()
}

func (s *Scheduler) ResendMessage() []*messaging.TargetMessage {
	var msgs []*messaging.TargetMessage
	if len(s.schedulingTask) > 0 {
		msgs = make([]*messaging.TargetMessage, 0, len(s.schedulingTask))
		for _, value := range s.schedulingTask {
			if msg := value.HandleResend(); msg != nil {
				msgs = append(msgs, msg)
			}
		}
	}
	return msgs
}

func (s *Scheduler) balanceTables() ([]*messaging.TargetMessage, error) {
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
				return changefeeds[i].ID.Less(changefeeds[j].ID)
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
		return nil, nil
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
		msg, err := cf.HandleMoveInferior(target)
		if err != nil {
			return nil, errors.Trace(err)
		}
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
	return messages, nil
}

func (s *Scheduler) HandleStatus(from string, statusList []*heartbeatpb.TableSpanStatus) ([]*messaging.TargetMessage, error) {
	stMap, ok := s.nodeTasks[from]
	if !ok {
		log.Warn("no server id found, ignore",
			zap.String("changeeed", s.changefeedID),
			zap.String("from", from))
		return nil, nil
	}
	var msgs = make([]*messaging.TargetMessage, 0)
	for _, status := range statusList {
		span := common.NewDispatcherIDFromPB(status.Span)
		stm, ok := stMap[]
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
		msg, err := stm.HandleInferiorStatus(sch, from)
		if err != nil {
			log.Error("fail to handle inferior status",
				zap.String("changeeed", s.changefeedID),
				zap.String("span", span.String()))
			return nil, errors.Trace(err)
		}
		msgs = append(msgs, msg)
		s.tryMoveTask(span, stm, oldState, oldPrimary, true)
	}
	return msgs, nil
}

func (s *Scheduler) TaskSize() int {
	return len(s.schedulingTask) + len(s.working) + len(s.absent)
}

func (s *Scheduler) GetTaskSizeByState(state scheduler.SchedulerStatus) int {
	size := 0
	switch state {
	case scheduler.SchedulerStatusAbsent:
		size = len(s.absent)
	case scheduler.SchedulerStatusWorking:
		size = len(s.working)
	case scheduler.SchedulerStatusCommiting, scheduler.SchedulerStatusRemoving:
		for _, value := range s.schedulingTask {
			if state == value.State {
				size++
			}
		}
	}
	return size
}

func (s *Scheduler) GetTaskSizeByNodeID(nodeID string) int {
	sm, ok := s.nodeTasks[nodeID]
	if ok {
		return len(sm)
	}
	return 0
}

// tryMoveTask moves the StateMachine to the right map and modified the node map if changed
func (s *Scheduler) tryMoveTask(span common.DispatcherID,
	stm *scheduler.StateMachine,
	oldSate scheduler.SchedulerStatus,
	oldPrimary string,
	modifyNodeMap bool) {
	switch oldSate {
	case scheduler.SchedulerStatusAbsent:
		s.moveFromAbsent(span, stm)
	case scheduler.SchedulerStatusCommiting:
		s.moveFromCommiting(span, stm)
	case scheduler.SchedulerStatusWorking:
		s.moveFromWorking(span, stm)
	case scheduler.SchedulerStatusRemoving:
		s.moveFromRemoving(span, stm)
	}
	// state machine is remove after state changed, remove from all maps
	// if removed, new primary node must be empty so we also can
	// update nodesMap if modifyNodeMap is true
	if stm.HasRemoved() {
		delete(s.schedulingTask, span)
		delete(s.absent, span)
		delete(s.working, span)
	}
	// keep node task map is updated
	if modifyNodeMap && oldPrimary != stm.Primary {
		taskMap, ok := s.nodeTasks[oldPrimary]
		if ok {
			delete(taskMap, span)
		}
		taskMap, ok = s.nodeTasks[stm.Primary]
		if ok {
			taskMap[span] = stm
		}
	}
}

func (s *Scheduler) moveFromAbsent(span common.DispatcherID,
	stm *scheduler.StateMachine) {
	switch stm.State {
	case scheduler.SchedulerStatusAbsent:
	case scheduler.SchedulerStatusCommiting:
		delete(s.absent, span)
		s.schedulingTask[span] = stm
	case scheduler.SchedulerStatusWorking:
		delete(s.absent, span)
		s.working[span] = stm
	case scheduler.SchedulerStatusRemoving:
		delete(s.absent, span)
		s.schedulingTask[span] = stm
	}
}

func (s *Scheduler) moveFromCommiting(span common.DispatcherID,
	stm *scheduler.StateMachine) {
	switch stm.State {
	case scheduler.SchedulerStatusAbsent:
		delete(s.schedulingTask, span)
		s.absent[span] = stm
	case scheduler.SchedulerStatusCommiting, scheduler.SchedulerStatusRemoving:
		// state not changed, primary should not be changefeed
	case scheduler.SchedulerStatusWorking:
		delete(s.schedulingTask, span)
		s.working[span] = stm
	}
}

func (s *Scheduler) moveFromWorking(span common.DispatcherID,
	stm *scheduler.StateMachine) {
	switch stm.State {
	case scheduler.SchedulerStatusAbsent:
		delete(s.working, span)
		s.absent[span] = stm
	case scheduler.SchedulerStatusCommiting, scheduler.SchedulerStatusRemoving:
		delete(s.working, span)
		s.schedulingTask[span] = stm
	case scheduler.SchedulerStatusWorking:
		// state not changed, primary should not be changefeed
	}
}

func (s *Scheduler) moveFromRemoving(span common.DispatcherID,
	stm *scheduler.StateMachine) {
	switch stm.State {
	case scheduler.SchedulerStatusAbsent:
		delete(s.schedulingTask, span)
		s.absent[span] = stm
	case scheduler.SchedulerStatusCommiting,
		scheduler.SchedulerStatusRemoving:
		s.schedulingTask[span] = stm
	case scheduler.SchedulerStatusWorking:
		delete(s.schedulingTask, span)
		s.working[span] = stm
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
