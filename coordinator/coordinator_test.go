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

	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

func TestCoordinatorRun(t *testing.T) {
	allGoM := make(map[model.ChangeFeedID]*StateMachine)
	captureID := uuid.New().String()
	for id, _ := range allChangefeeds {
		st := &StateMachine{
			ID:       ChangefeedID(id),
			State:    SchedulerStatusWorking,
			Primary:  captureID,
			Servers:  make(map[string]Role),
			Inferior: &changefeed{},
		}
		st.setCapture(captureID, RolePrimary)
		allGoM[id] = st
	}

	now := time.Now()
	sp := Supervisor{
		stateMachines: allGoM,
	}
	status := make([]InferiorStatus, 0, len(allGoM))
	for id, _ := range allChangefeeds {
		status = append(status, &ChangefeedStatus{
			ID:     ChangefeedID(id),
			Status: ComponentStatusWorking,
		})
	}
	sp.HandleStatus(captureID, status)
	log.Info("TestCoordinatorRun", zap.Duration("time", time.Since(now)))
}
