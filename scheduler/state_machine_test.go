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
	"testing"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/rpc"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

type changefeed struct {
	id     ChangefeedID
	status InferiorStatus
}

func newChangefeed(id InferiorID) Inferior {
	return &changefeed{id: id.(ChangefeedID)}
}
func (c *changefeed) GetID() InferiorID {
	return c.id
}
func (c *changefeed) UpdateStatus(status InferiorStatus) {
	c.status = status
}
func (c *changefeed) IsAlive() bool {
	return true
}
func (c *changefeed) NewInferiorStatus(state heartbeatpb.ComponentState) InferiorStatus {
	return &CfStatus{
		id:    c.id,
		state: state,
	}
}
func (c *changefeed) NewAddInferiorMessage(model.CaptureID, bool) rpc.Message {
	return &messaging.TargetMessage{}
}

func (c *changefeed) NewRemoveInferiorMessage(model.CaptureID) rpc.Message {
	return &messaging.TargetMessage{}
}

type CfStatus struct {
	id    ChangefeedID
	state heartbeatpb.ComponentState
}

func (s *CfStatus) GetInferiorID() InferiorID {
	return s.id
}
func (s *CfStatus) GetInferiorState() heartbeatpb.ComponentState {
	return s.state
}

var (
	cfID1 = ChangefeedID(model.DefaultChangeFeedID("cf-1"))
)

func TestInferiorStoppedWhenWorking(t *testing.T) {
	sup := NewSupervisor(cfID1, newChangefeed,
		func(id model.CaptureID) rpc.Message {
			return &messaging.TargetMessage{}
		})
	scheduler := NewCombineScheduler(NewBasicScheduler(ChangefeedID{}),
		NewBalanceScheduler(time.Minute, 1000))
	allInferior := []InferiorID{cfID1}

	captureInfo := &model.CaptureInfo{ID: "capture-1"}
	aliveCaptures := map[model.CaptureID]*CaptureStatus{
		captureInfo.ID: &CaptureStatus{state: CaptureStateInitialized, capture: captureInfo},
	}
	tasks := scheduler.Schedule(allInferior, aliveCaptures, sup.StateMachines)
	msgs, err := sup.handleScheduleTasks(tasks)
	require.NoError(t, err)
	require.NotNil(t, msgs)
	working := []InferiorStatus{
		&CfStatus{
			id:    cfID1,
			state: heartbeatpb.ComponentState_Working,
		},
	}
	sup.HandleStatus(captureInfo.ID, working)
	tasks = scheduler.Schedule(allInferior, aliveCaptures, sup.StateMachines)

	sup.HandleStatus(captureInfo.ID, []InferiorStatus{
		&CfStatus{
			id:    cfID1,
			state: heartbeatpb.ComponentState_Working,
		},
		&CfStatus{
			id:    cfID1,
			state: heartbeatpb.ComponentState_Stopped,
		},
	})
	tasks = scheduler.Schedule(allInferior, aliveCaptures, sup.StateMachines)
	msgs, err = sup.handleScheduleTasks(tasks)
	tasks = scheduler.Schedule(allInferior, aliveCaptures, sup.StateMachines)
	msgs, err = sup.handleScheduleTasks(tasks)

}
