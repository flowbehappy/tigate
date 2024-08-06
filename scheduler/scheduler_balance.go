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
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/flowbehappy/tigate/utils"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

var _ Scheduler = &balanceScheduler{}

// The scheduler for balancing tables among all captures.
type balanceScheduler struct {
	id                   InferiorID
	random               *rand.Rand
	lastRebalanceTime    time.Time
	checkBalanceInterval time.Duration
	// forceBalance forces the scheduler to produce schedule tasks regardless of
	// `checkBalanceInterval`.
	// It is set to true when the last time `Schedule` produces some tasks,
	// and it is likely there are more tasks will be produced in the next
	// `Schedule`.
	// It speeds up rebalance.
	forceBalance bool

	maxTaskConcurrency int
	changefeedID       model.ChangeFeedID
}

func NewBalanceScheduler(interval time.Duration, concurrency int) *balanceScheduler {
	return &balanceScheduler{
		random:               rand.New(rand.NewSource(time.Now().UnixNano())),
		checkBalanceInterval: interval,
		maxTaskConcurrency:   concurrency,
	}
}

func (b *balanceScheduler) Name() string {
	return "balance-scheduler"
}

func (b *balanceScheduler) Schedule(
	allInferiors utils.Map[InferiorID, Inferior],
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	stateMachines utils.Map[InferiorID, *StateMachine],
	batchSize int,
) []*ScheduleTask {
	if b.maxTaskConcurrency < batchSize {
		batchSize = b.maxTaskConcurrency
	}
	if !b.forceBalance {
		now := time.Now()
		if now.Sub(b.lastRebalanceTime) < b.checkBalanceInterval {
			// skip balance.
			return nil
		}
		b.lastRebalanceTime = now
	}

	tasks := buildBalanceMoveTables(
		b.random, aliveCaptures, stateMachines, batchSize)
	b.forceBalance = len(tasks) != 0
	log.Info("balance scheduler generate tasks",
		zap.String("id", b.id.String()),
		zap.String("changefeed", string(b.changefeedID.String())),
		zap.Int("task count", len(tasks)))
	return tasks
}

func buildBalanceMoveTables(
	random *rand.Rand,
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	stateMachines utils.Map[InferiorID, *StateMachine],
	maxTaskConcurrency int,
) []*ScheduleTask {
	moves := newBalanceMoveTables(
		random, aliveCaptures, stateMachines, maxTaskConcurrency)
	tasks := make([]*ScheduleTask, 0, len(moves))
	for i := 0; i < len(moves); i++ {
		tasks = append(tasks, &ScheduleTask{
			MoveInferior: moves[i],
		})
	}
	return tasks
}

func newBalanceMoveTables(
	random *rand.Rand,
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	stateMachines utils.Map[InferiorID, *StateMachine],
	maxTaskLimit int,
) []*MoveInferior {
	tablesPerCapture := make(map[model.CaptureID]utils.Map[InferiorID, *StateMachine])
	for captureID := range aliveCaptures {
		tablesPerCapture[captureID] = utils.NewBtreeMap[InferiorID, *StateMachine]()
	}

	stateMachines.Ascend(func(key InferiorID, value *StateMachine) bool {
		if value.State == SchedulerStatusWorking {
			tablesPerCapture[value.Primary].ReplaceOrInsert(key, value)
		}
		return true
	})

	// findVictim return tables which need to be moved
	upperLimitPerCapture := int(math.Ceil(float64(stateMachines.Len()) / float64(len(aliveCaptures))))

	victims := make([]*StateMachine, 0)
	for _, ts := range tablesPerCapture {
		var changefeeds []*StateMachine
		ts.Ascend(func(key InferiorID, value *StateMachine) bool {
			changefeeds = append(changefeeds, value)
			return true
		})
		if random != nil {
			// Complexity note: Shuffle has O(n), where `n` is the number of tables.
			// Also, during a single call of `Schedule`, Shuffle can be called at most
			// `c` times, where `c` is the number of captures (TiCDC nodes).
			// Only called when a rebalance is triggered, which happens rarely,
			// we do not expect a performance degradation as a result of adding
			// the randomness.
			random.Shuffle(len(changefeeds), func(i, j int) {
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
			continue
		}

		for _, cf := range changefeeds {
			if tableNum2Remove <= 0 {
				break
			}
			victims = append(victims, cf)
			ts.Delete(cf.ID)
			tableNum2Remove--
		}
	}
	if len(victims) == 0 {
		return nil
	}

	captureWorkload := make(map[model.CaptureID]int)
	for captureID, ts := range tablesPerCapture {
		captureWorkload[captureID] = randomizeWorkload(random, ts.Len())
	}
	// for each victim table, find the target for it
	moveTables := make([]*MoveInferior, 0, len(victims))
	for idx, cf := range victims {
		target := ""
		minWorkload := math.MaxInt64

		for captureID, workload := range captureWorkload {
			if workload < minWorkload {
				minWorkload = workload
				target = captureID
			}
		}

		if minWorkload == math.MaxInt64 {
			log.Panic("schedulerv3: rebalance meet unexpected min workload " +
				"when try to the the target capture")
		}
		if idx >= maxTaskLimit {
			// We have reached the task limit.
			break
		}

		moveTables = append(moveTables, &MoveInferior{
			ID:          cf.ID,
			DestCapture: target,
		})
		tablesPerCapture[target].ReplaceOrInsert(cf.ID, cf)
		captureWorkload[target] = randomizeWorkload(random, tablesPerCapture[target].Len())
	}

	return moveTables
}

const (
	randomPartBitSize = 8
	randomPartMask    = (1 << randomPartBitSize) - 1
)

// randomizeWorkload injects small randomness into the workload, so that
// when two captures tied in competing for the minimum workload, the result
// will not always be the same.
// The bitwise layout of the return value is:
// 63                8                0
// |----- input -----|-- random val --|
func randomizeWorkload(random *rand.Rand, input int) int {
	var randomPart int
	if random != nil {
		randomPart = int(random.Uint32() & randomPartMask)
	}
	// randomPart is a small random value that only affects the
	// result of comparison of workloads when two workloads are equal.
	return (input << randomPartBitSize) | randomPart
}
