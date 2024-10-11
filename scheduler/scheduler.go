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

package scheduler

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/utils/heap"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Scheduler[T comparable] struct {
	batchSize            int
	changefeedID         string
	random               *rand.Rand
	lastRebalanceTime    time.Time
	checkBalanceInterval time.Duration
}

func NewScheduler[T comparable](batchSize int, changefeedID string,
	balanceInterval time.Duration) *Scheduler[T] {
	return &Scheduler[T]{
		batchSize:            batchSize,
		random:               rand.New(rand.NewSource(time.Now().UnixNano())),
		changefeedID:         changefeedID,
		checkBalanceInterval: balanceInterval,
		lastRebalanceTime:    time.Now(),
	}
}

func (s *Scheduler[T]) Schedule(
	absent map[T]*StateMachine[T],
	commiting map[T]*StateMachine[T],
	removing map[T]*StateMachine[T],
	working map[T]*StateMachine[T],
	nodeTasks map[node.ID]map[T]*StateMachine[T]) []ScheduleTask[T] {
	if len(absent) > 0 {
		return s.basicSchedule(absent, commiting, removing, working, nodeTasks)
	}
	// we scheduled all absent tasks, try to balance it if needed
	return s.Balance(absent, commiting, removing, working, nodeTasks)
}

func (s *Scheduler[T]) basicSchedule(
	absent map[T]*StateMachine[T],
	commiting map[T]*StateMachine[T],
	removing map[T]*StateMachine[T],
	working map[T]*StateMachine[T],
	nodeTasks map[node.ID]map[T]*StateMachine[T]) []ScheduleTask[T] {
	totalSize := s.batchSize - len(removing) - len(commiting)
	if totalSize <= 0 {
		// too many running tasks, skip schedule
		return nil
	}
	tasks := make([]ScheduleTask[T], 0, totalSize)
	priorityQueue := heap.NewHeap[*Item]()
	for key, m := range nodeTasks {
		priorityQueue.AddOrUpdate(&Item{
			Node:     key,
			TaskSize: len(m),
		})
	}

	taskSize := 0
	for key, _ := range absent {
		item, _ := priorityQueue.PeekTop()
		tasks = append(tasks, ScheduleTask[T]{
			AddInferior: &AddInferior[T]{
				ID:        key,
				CaptureID: item.Node,
			}})
		item.TaskSize++
		priorityQueue.AddOrUpdate(item)
		taskSize++
		if taskSize >= totalSize {
			break
		}
	}
	return tasks
}

func (s *Scheduler[T]) Balance(
	absent map[T]*StateMachine[T],
	commiting map[T]*StateMachine[T],
	removing map[T]*StateMachine[T],
	working map[T]*StateMachine[T],
	nodeTasks map[node.ID]map[T]*StateMachine[T]) []ScheduleTask[T] {
	if time.Since(s.lastRebalanceTime) < s.checkBalanceInterval {
		return nil
	}
	if len(absent) > 0 || len(commiting) > 0 || len(removing) > 0 {
		// not in stable schedule state, skip balance
		return nil
	}
	now := time.Now()
	if now.Sub(s.lastRebalanceTime) < s.checkBalanceInterval {
		// skip balance.
		return nil
	}
	s.lastRebalanceTime = now
	return s.balanceTables(absent, commiting, removing, working, nodeTasks)
}

func (s *Scheduler[T]) balanceTables(
	absent map[T]*StateMachine[T],
	commiting map[T]*StateMachine[T],
	removing map[T]*StateMachine[T],
	working map[T]*StateMachine[T],
	nodeTasks map[node.ID]map[T]*StateMachine[T]) []ScheduleTask[T] {
	upperLimitPerCapture := int(math.Ceil(float64(len(working)) / float64(len(nodeTasks))))
	// victims holds tables which need to be moved
	victims := make([]*StateMachine[T], 0)
	scheduleTasks := make([]ScheduleTask[T], 0)
	priorityQueue := heap.NewHeap[*Item]()
	for nodeID, ts := range nodeTasks {
		var stms []*StateMachine[T]
		for _, value := range ts {
			stms = append(stms, value)
		}

		// Complexity note: Shuffle has O(n), where `n` is the number of tables.
		// Also, during a single call of `Schedule`, Shuffle can be called at most
		// `c` times, where `c` is the number of captures (TiCDC nodes).
		// Only called when a rebalance is triggered, which happens rarely,
		// we do not expect a performance degradation as a result of adding
		// the randomness.
		s.random.Shuffle(len(stms), func(i, j int) {
			stms[i], stms[j] = stms[j], stms[i]
		})

		tableNum2Remove := len(stms) - upperLimitPerCapture
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

		for _, cf := range stms {
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
		scheduleTasks = append(scheduleTasks, ScheduleTask[T]{
			MoveInferior: &MoveInferior[T]{
				ID:          cf.ID,
				DestCapture: item.Node,
			},
		})
		// update the task size priority queue
		item.TaskSize++
		priorityQueue.AddOrUpdate(item)
		movedSize++
	}
	log.Info("balance done",
		zap.String("changefeed", s.changefeedID),
		zap.Int("movedSize", movedSize),
		zap.Int("victims", len(victims)))
	return scheduleTasks
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

// ScheduleTask is a schedule task that wraps add/move/remove tasks.
type ScheduleTask[T comparable] struct { //nolint:revive
	MoveInferior *MoveInferior[T]
	AddInferior  *AddInferior[T]
}

// Name returns the name of a schedule task.
func (s *ScheduleTask[T]) Name() string {
	if s.MoveInferior != nil {
		return "moveInferior"
	} else if s.AddInferior != nil {
		return "addInferior"
	}
	return "unknown"
}

func (s *ScheduleTask[T]) String() string {
	if s.MoveInferior != nil {
		return s.MoveInferior.String()
	}
	if s.AddInferior != nil {
		return s.AddInferior.String()
	}
	return ""
}

// MoveInferior is a schedule task for moving a inferior.
type MoveInferior[T any] struct {
	ID          T
	DestCapture node.ID
}

func (t MoveInferior[T]) String() string {
	return fmt.Sprintf("MoveInferior, id: %v, dest: %s",
		t.ID, t.DestCapture)
}

// AddInferior is a schedule task for adding an inferior.
type AddInferior[T any] struct {
	ID        T
	CaptureID node.ID
}

func (t AddInferior[T]) String() string {
	return fmt.Sprintf("Add, ID: %v, server: %s",
		t.ID, t.CaptureID)
}
