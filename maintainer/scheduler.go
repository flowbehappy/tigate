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
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/utils"
	"github.com/flowbehappy/tigate/utils/heap"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"math"
	"math/rand"
	"sort"
	"time"
)

// Scheduler schedules tables
type Scheduler struct {
	// absent track all table spans that need to be scheduled
	// when dispatcher reported a new table or remove a table, this field should be updated
	absent utils.Map[*common.TableSpan, *scheduler.StateMachine]

	// schedulingTask list the task that wait for response
	schedulingTask utils.Map[*common.TableSpan, *scheduler.StateMachine]
	working        utils.Map[*common.TableSpan, *scheduler.StateMachine]

	nodeTasks map[string]utils.Map[*common.TableSpan, *scheduler.StateMachine]

	changefeedID         string
	batchSize            int
	random               *rand.Rand
	lastRebalanceTime    time.Time
	checkBalanceInterval time.Duration
}

func NewScheduler(changefeedID string,
	batchSize int, balanceInterval time.Duration) *Scheduler {
	return &Scheduler{
		schedulingTask:       utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine](),
		working:              utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine](),
		absent:               utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine](),
		nodeTasks:            make(map[string]utils.Map[*common.TableSpan, *scheduler.StateMachine]),
		changefeedID:         changefeedID,
		batchSize:            batchSize,
		random:               rand.New(rand.NewSource(time.Now().UnixNano())),
		checkBalanceInterval: balanceInterval,
		lastRebalanceTime:    time.Now(),
	}
}

func (s *Scheduler) AddNewTask(stm *scheduler.StateMachine) {
	s.absent.ReplaceOrInsert(stm.ID.(*common.TableSpan), stm)
}

// AddWorkingTask adds a working state task to this scheduler directly,
// it reported by the bootstrap response
func (s *Scheduler) AddWorkingTask(stm *scheduler.StateMachine) {
	if stm.State != scheduler.SchedulerStatusWorking {
		log.Panic("unexpected state",
			zap.String("changefeed", s.changefeedID),
			zap.Any("stm", stm))
	}
	span := stm.ID.(*common.TableSpan)
	s.working.ReplaceOrInsert(span, stm)
	s.absent.Delete(stm.ID.(*common.TableSpan))
	s.nodeTasks[stm.Primary].ReplaceOrInsert(span, stm)
	if s.schedulingTask.Has(span) {
		log.Warn("span state not expected, remove from commiting",
			zap.String("changefeed", s.changefeedID),
			zap.String("span", span.String()))
		s.schedulingTask.Delete(stm.ID.(*common.TableSpan))
	}
}

func (s *Scheduler) RemoveTask(stm *scheduler.StateMachine) (*messaging.TargetMessage, error) {
	oldState := stm.State
	oldPrimary := stm.Primary
	msg, err := stm.HandleRemoveInferior()
	if err != nil {
		return nil, err
	}
	s.tryMoveTask(stm.ID.(*common.TableSpan), stm, oldState, oldPrimary, true)
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
	s.nodeTasks[id] = utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine]()
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
	stmMap.Ascend(func(key *common.TableSpan, value *scheduler.StateMachine) bool {
		oldState := value.State
		msg, _ := value.HandleCaptureShutdown(nodeId)
		if msg != nil {
			msgs = append(msgs, msg)
		}
		if value.Primary != "" && value.Primary != nodeId {
			s.nodeTasks[value.Primary].Delete(key)
		}
		s.tryMoveTask(key, value, oldState, nodeId, false)
		return true
	})
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
	totalSize := s.batchSize - s.schedulingTask.Len()
	if totalSize <= 0 {
		// too many running tasks, skip schedule
		return nil, nil
	}
	priorityQueue := heap.NewHeap[*Item]()
	for key, m := range s.nodeTasks {
		priorityQueue.AddOrUpdate(&Item{
			Node:     key,
			TaskSize: m.Len(),
		})
	}

	taskSize := 0
	var msgs = make([]*messaging.TargetMessage, 0, s.batchSize)
	var scheduled = make([]*common.TableSpan, 0, s.batchSize)
	var err error
	s.absent.Ascend(func(key *common.TableSpan, value *scheduler.StateMachine) bool {
		item, _ := priorityQueue.PeekTop()
		msg, err1 := value.HandleAddInferior(item.Node)
		if err1 != nil {
			err = errors.Trace(err1)
			return false
		}
		msgs = append(msgs, msg)

		s.schedulingTask.ReplaceOrInsert(key, value)
		s.nodeTasks[item.Node].ReplaceOrInsert(key, value)
		scheduled = append(scheduled, key)

		item.TaskSize++
		priorityQueue.AddOrUpdate(item)
		taskSize++
		return taskSize < totalSize
	})
	for _, key := range scheduled {
		s.absent.Delete(key)
	}
	return msgs, err
}

func (s *Scheduler) NeedSchedule() bool {
	return s.absent.Len() > 0
}

func (s *Scheduler) ScheduleFinished() bool {
	return s.absent.Len() == 0 && s.schedulingTask.Len() == 0
}

func (s *Scheduler) TryBalance() ([]*messaging.TargetMessage, error) {
	if s.absent.Len() > 0 || s.schedulingTask.Len() > 0 {
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
	if s.schedulingTask.Len() > 0 {
		msgs = make([]*messaging.TargetMessage, 0, s.schedulingTask.Len())
		s.schedulingTask.Ascend(func(key *common.TableSpan, value *scheduler.StateMachine) bool {
			if msg := value.HandleResend(); msg != nil {
				msgs = append(msgs, msg)
			}
			return true
		})
	}
	return msgs
}

func (s *Scheduler) balanceTables() ([]*messaging.TargetMessage, error) {
	var messages = make([]*messaging.TargetMessage, 0)
	upperLimitPerCapture := int(math.Ceil(float64(s.working.Len()) / float64(len(s.nodeTasks))))
	// victims holds tables which need to be moved
	victims := make([]*scheduler.StateMachine, 0)
	priorityQueue := heap.NewHeap[*Item]()
	for nodeID, ts := range s.nodeTasks {
		var changefeeds []*scheduler.StateMachine
		ts.Ascend(func(key *common.TableSpan, value *scheduler.StateMachine) bool {
			changefeeds = append(changefeeds, value)
			return true
		})
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
				TaskSize: ts.Len(),
			})
			continue
		} else {
			priorityQueue.AddOrUpdate(&Item{
				Node:     nodeID,
				TaskSize: ts.Len() - tableNum2Remove,
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
		s.tryMoveTask(cf.ID.(*common.TableSpan), cf, oldState, oldPrimary, false)
		// update the task size priority queue
		item.TaskSize++
		priorityQueue.AddOrUpdate(item)
		movedSize++
	}
	log.Info("blance done",
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
		span := &common.TableSpan{TableSpan: status.Span}
		stm, ok := stMap.Get(span)
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
	return s.schedulingTask.Len() + s.working.Len() + s.absent.Len()
}

func (s *Scheduler) GetTaskSizeByState(state scheduler.SchedulerStatus) int {
	size := 0
	switch state {
	case scheduler.SchedulerStatusAbsent:
		size = s.absent.Len()
	case scheduler.SchedulerStatusWorking:
		size = s.working.Len()
	case scheduler.SchedulerStatusCommiting, scheduler.SchedulerStatusRemoving:
		s.schedulingTask.Ascend(func(key *common.TableSpan, value *scheduler.StateMachine) bool {
			if state == value.State {
				size++
			}
			return true
		})
	}
	return size
}

func (s *Scheduler) GetTaskSizeByNodeID(nodeID string) int {
	sm, ok := s.nodeTasks[nodeID]
	if ok {
		return sm.Len()
	}
	return 0
}

// tryMoveTask moves the StateMachine to the right map and modified the node map if changed
func (s *Scheduler) tryMoveTask(span *common.TableSpan,
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
		s.schedulingTask.Delete(span)
		s.absent.Delete(span)
		s.working.Delete(span)
	}
	// keep node task map is updated
	if modifyNodeMap && oldPrimary != stm.Primary {
		taskMap, ok := s.nodeTasks[oldPrimary]
		if ok {
			taskMap.Delete(span)
		}
		taskMap, ok = s.nodeTasks[stm.Primary]
		if ok {
			taskMap.ReplaceOrInsert(span, stm)
		}
	}
}

func (s *Scheduler) moveFromAbsent(span *common.TableSpan,
	stm *scheduler.StateMachine) {
	switch stm.State {
	case scheduler.SchedulerStatusAbsent:
	case scheduler.SchedulerStatusCommiting:
		s.absent.Delete(span)
		s.schedulingTask.ReplaceOrInsert(span, stm)
	case scheduler.SchedulerStatusWorking:
		s.absent.Delete(span)
		s.working.ReplaceOrInsert(span, stm)
	case scheduler.SchedulerStatusRemoving:
		s.absent.Delete(span)
		s.schedulingTask.ReplaceOrInsert(span, stm)
	}
}

func (s *Scheduler) moveFromCommiting(span *common.TableSpan,
	stm *scheduler.StateMachine) {
	switch stm.State {
	case scheduler.SchedulerStatusAbsent:
		s.schedulingTask.Delete(span)
		s.absent.ReplaceOrInsert(span, stm)
	case scheduler.SchedulerStatusCommiting, scheduler.SchedulerStatusRemoving:
		// state not changed, primary should not be changefeed
	case scheduler.SchedulerStatusWorking:
		s.schedulingTask.Delete(span)
		s.working.ReplaceOrInsert(span, stm)
	}
}

func (s *Scheduler) moveFromWorking(span *common.TableSpan,
	stm *scheduler.StateMachine) {
	switch stm.State {
	case scheduler.SchedulerStatusAbsent:
		s.working.Delete(span)
		s.absent.ReplaceOrInsert(span, stm)
	case scheduler.SchedulerStatusCommiting, scheduler.SchedulerStatusRemoving:
		s.working.Delete(span)
		s.schedulingTask.ReplaceOrInsert(span, stm)
	case scheduler.SchedulerStatusWorking:
		// state not changed, primary should not be changefeed
	}
}

func (s *Scheduler) moveFromRemoving(span *common.TableSpan,
	stm *scheduler.StateMachine) {
	switch stm.State {
	case scheduler.SchedulerStatusAbsent:
		s.schedulingTask.Delete(span)
		s.absent.ReplaceOrInsert(span, stm)
	case scheduler.SchedulerStatusCommiting,
		scheduler.SchedulerStatusRemoving:
		s.schedulingTask.ReplaceOrInsert(span, stm)
	case scheduler.SchedulerStatusWorking:
		s.schedulingTask.Delete(span)
		s.working.ReplaceOrInsert(span, stm)
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
