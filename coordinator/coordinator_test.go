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

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/utils"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

type CFID model.ChangeFeedID

func (c CFID) Less(t any) bool {
	cf := t.(CFID)
	return c.ID < cf.ID
}

func TestCoordinatorRun(t *testing.T) {
	allGoM := make(map[model.ChangeFeedID]*StateMachine)
	utilM := utils.NewBtreeMap[CFID, *StateMachine]()
	captureID := uuid.New().String()
	for id, _ := range allChangefeeds {
		st := &StateMachine{
			ID:         id,
			State:      SchedulerStatusWorking,
			Primary:    captureID,
			Servers:    make(map[string]Role),
			changefeed: &changefeed{},
		}
		st.setCapture(captureID, RolePrimary)
		allGoM[id] = st
		utilM.ReplaceOrInsert(CFID(id), st)
	}

	for k := 0; k < 1000; k++ {
		now := time.Now()
		j := 0
		for _, value := range allGoM {
			value.HandleInferiorStatus(&heartbeatpb.MaintainerStatus{
				ChangefeedID: value.ID.ID,
				State:        heartbeatpb.ComponentState_Working,
			}, captureID)
			j++
		}
		log.Info("TestCoordinatorRun", zap.Int("j", j), zap.Duration("time", time.Since(now)))

		now = time.Now()
		i := 0
		utilM.Ascend(func(key CFID, value *StateMachine) bool {
			value.HandleInferiorStatus(&heartbeatpb.MaintainerStatus{
				ChangefeedID: value.ID.ID,
				State:        heartbeatpb.ComponentState_Working,
			}, captureID)
			i++
			return true
		})
		log.Info("TestCoordinatorRun", zap.Int("i", i), zap.Duration("time", time.Since(now)))
	}
}
