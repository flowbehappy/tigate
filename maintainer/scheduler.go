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
	}
	log.Info("add new node", zap.String("id", id))
	s.nodeTasks[id] = utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine]()
}

func (s *Scheduler) Schedule() ([]*messaging.TargetMessage, error) {
	if len(s.nodeTasks) == 0 {
		log.Warn("scheduler has no node tasks")
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
		s.tryMoveTask(span, stm, oldState)
	}
	return msgs, nil
}

func (s *Scheduler) tryMoveTask(span *common.TableSpan,
	stm *scheduler.StateMachine,
	oldSate scheduler.SchedulerStatus) {
	if stm.HasRemoved() {
		s.schedulingTask.Delete(span)
		// remove from node tasks
		s.nodeTasks[stm.Primary].Delete(span)
	}
	if stm.State != oldSate && stm.State == scheduler.SchedulerStatusWorking {
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
