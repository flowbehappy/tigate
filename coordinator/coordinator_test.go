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

package coordinator

import (
	"testing"
	"time"

	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

func TestCoordinatorRun(t *testing.T) {
	allM := scheduler.NewBtreeMap[scheduler.InferiorID, *scheduler.StateMachine]()
	allGoM := make(map[model.ChangeFeedID]*scheduler.StateMachine)
	for id, _ := range allChangefeeds {
		allM.ReplaceOrInsert(ChangefeedID(id), &scheduler.StateMachine{})
		allGoM[id] = &scheduler.StateMachine{}
	}

	now := time.Now()
	for id, _ := range allChangefeeds {
		_, _ = allM.Get(ChangefeedID(id))
	}
	log.Info("TestCoordinatorRun", zap.Duration("time", time.Since(now)))

	now = time.Now()
	for id, _ := range allChangefeeds {
		_, _ = allGoM[id]
	}
	log.Info("TestCoordinatorRun", zap.Duration("time", time.Since(now)))
}
