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
)

type Scheduler struct {
	// absent track all table spans that need to be scheduled
	// when dispatcher reported a new table or remove a table, this field should be updated
	absent utils.Map[*common.TableSpan, *scheduler.StateMachine]

	// schedulingTask list the task that wait for response
	schedulingTask utils.Map[*common.TableSpan, *scheduler.StateMachine]
	working        utils.Map[*common.TableSpan, *scheduler.StateMachine]

	nodeTasks map[string]utils.Map[*common.TableSpan, *scheduler.StateMachine]

	batchSize int
}

func NewScheduler(batchSize int) *Scheduler {
	return &Scheduler{
		schedulingTask: utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine](),
		working:        utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine](),
		absent:         utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine](),

		nodeTasks: make(map[string]utils.Map[*common.TableSpan, *scheduler.StateMachine]),
		batchSize: batchSize,
	}
}

func (s *Scheduler) AddNewNode(id string) {
	_, ok := s.nodeTasks[id]
	if ok {
		log.Info("node already exists", zap.String("id", id))
		return
	}
	log.Info("add new node", zap.String("id", id))
	s.nodeTasks[id] = utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine]()
}

func (s *Scheduler) RemoveNode(nodeId string) []*messaging.TargetMessage {
	stmMap, ok := s.nodeTasks[nodeId]
	if !ok {
		log.Info("node is maintained by scheduler, ignore", zap.String("id", nodeId))
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
		log.Warn("scheduler has no node tasks")
		return nil, nil
	}
	if !s.NeedSchedule() {
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
		s.absent.Delete(key)
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
		return taskSize < s.batchSize
	})
	for _, key := range scheduled {
		s.absent.Delete(key)
	}
	return msgs, err
}

func (s *Scheduler) NeedSchedule() bool {
	return s.absent.Len() > 0
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

func (s *Scheduler) HandleStatus(from string, statusList []*heartbeatpb.TableSpanStatus) ([]*messaging.TargetMessage, error) {
	stMap, ok := s.nodeTasks[from]
	if !ok {
		log.Warn("no server id found, ignore", zap.String("from", from))
		return nil, nil
	}
	var msgs = make([]*messaging.TargetMessage, 0)
	for _, status := range statusList {
		span := &common.TableSpan{
			TableSpan: status.Span,
		}
		stm, ok := stMap.Get(span)
		if !ok {
			log.Warn("no statemachine id found, ignore",
				zap.String("span", span.String()))
		}
		oldState := stm.State
		oldPrimary := stm.Primary
		var sch scheduler.InferiorStatus = &ReplicaSetStatus{
			ID:           span,
			State:        status.ComponentStatus,
			CheckpointTs: status.CheckpointTs,
		}
		msg, err := stm.HandleInferiorStatus(sch, from)
		if err != nil {
			log.Error("fail to handle inferior status", zap.String("span", span.String()))
			return nil, errors.Trace(err)
		}
		msgs = append(msgs, msg)
		s.tryMoveTask(span, stm, oldState, oldPrimary, true)
	}
	return msgs, nil
}

func (s *Scheduler) tryMoveTask(span *common.TableSpan,
	stm *scheduler.StateMachine,
	oldSate scheduler.SchedulerStatus,
	oldPrimary string,
	modifyNodeMap bool) {
	switch oldSate {
	case scheduler.SchedulerStatusAbsent:
		s.moveFromAbsent(span, stm, oldPrimary, modifyNodeMap)
	case scheduler.SchedulerStatusCommiting:
		s.moveFromCommiting(span, stm, oldPrimary, modifyNodeMap)
	case scheduler.SchedulerStatusWorking:
		s.moveFromWorking(span, stm, oldPrimary, modifyNodeMap)
	case scheduler.SchedulerStatusRemoving:
		s.moveFromRemoving(span, stm, oldPrimary, modifyNodeMap)
	}
}

func (s *Scheduler) moveFromAbsent(span *common.TableSpan,
	stm *scheduler.StateMachine,
	oldPrimary string,
	modifyNodeMap bool) {
	switch stm.State {
	case scheduler.SchedulerStatusAbsent:
	case scheduler.SchedulerStatusCommiting:
		s.schedulingTask.ReplaceOrInsert(span, stm)
		if modifyNodeMap {
			s.nodeTasks[stm.Primary].ReplaceOrInsert(span, stm)
		}
	case scheduler.SchedulerStatusWorking:
		s.working.ReplaceOrInsert(span, stm)
		if modifyNodeMap {
			s.nodeTasks[stm.Primary].ReplaceOrInsert(span, stm)
		}
	case scheduler.SchedulerStatusRemoving:
		s.working.ReplaceOrInsert(span, stm)
		if modifyNodeMap {
			s.nodeTasks[stm.Primary].ReplaceOrInsert(span, stm)
		}
	}
}

func (s *Scheduler) moveFromCommiting(span *common.TableSpan,
	stm *scheduler.StateMachine,
	oldPrimary string,
	modifyNodeMap bool) {
	switch stm.State {
	case scheduler.SchedulerStatusAbsent:
		s.schedulingTask.Delete(span)
		s.absent.ReplaceOrInsert(span, stm)
		if modifyNodeMap {
			s.nodeTasks[stm.Primary].ReplaceOrInsert(span, stm)
		}
	case scheduler.SchedulerStatusCommiting:
		// state not changed, primary should not be changefeed
	case scheduler.SchedulerStatusWorking:
		s.schedulingTask.Delete(span)
		s.working.ReplaceOrInsert(span, stm)
	case scheduler.SchedulerStatusRemoving:
		// still in running task map
	}
}

func (s *Scheduler) moveFromWorking(span *common.TableSpan,
	stm *scheduler.StateMachine,
	oldPrimary string,
	modifyNodeMap bool) {
	switch stm.State {
	case scheduler.SchedulerStatusAbsent:
		s.working.Delete(span)
		s.absent.ReplaceOrInsert(span, stm)
		if modifyNodeMap {
			s.nodeTasks[stm.Primary].ReplaceOrInsert(span, stm)
		}
	case scheduler.SchedulerStatusCommiting, scheduler.SchedulerStatusRemoving:
		s.working.Delete(span)
		s.schedulingTask.ReplaceOrInsert(span, stm)
	case scheduler.SchedulerStatusWorking:
		// state not changed, primary should not be changefeed
	}
}

func (s *Scheduler) moveFromRemoving(span *common.TableSpan,
	stm *scheduler.StateMachine,
	oldPrimary string,
	modifyNodeMap bool) {
	switch stm.State {
	case scheduler.SchedulerStatusAbsent:
		s.working.Delete(span)
		s.absent.ReplaceOrInsert(span, stm)
		if modifyNodeMap {
			s.nodeTasks[stm.Primary].ReplaceOrInsert(span, stm)
		}
	case scheduler.SchedulerStatusCommiting, scheduler.SchedulerStatusRemoving:
		s.working.Delete(span)
		s.schedulingTask.ReplaceOrInsert(span, stm)
	case scheduler.SchedulerStatusWorking:
		s.schedulingTask.Delete(span)
		s.working.ReplaceOrInsert(span, stm)
		if modifyNodeMap && oldPrimary != stm.Primary {
			s.nodeTasks[stm.Primary].ReplaceOrInsert(span, stm)
			s.nodeTasks[oldPrimary].Delete(span)
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
