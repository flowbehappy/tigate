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
	"encoding/json"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

type ChangeFeedDB interface {
	GetChangefeedConfig(id model.ChangeFeedID) *model.ChangefeedConfig
}

// changefeed tracks the scheduled maintainer on coordinator side
type changefeed struct {
	ID    model.ChangeFeedID
	State *MaintainerStatus

	Info *model.ChangeFeedInfo

	lastHeartBeat time.Time

	checkpointTs uint64
	configBytes  []byte

	coordinator  *coordinator
	stateMachine *scheduler.StateMachine
}

func newChangefeed(c *coordinator,
	cfID model.ChangeFeedID,
	info *model.ChangeFeedInfo,
	checkpointTs uint64) *changefeed {
	bytes, err := json.Marshal(info)
	if err != nil {
		log.Panic("unable to marshal changefeed config",
			zap.Error(err))
	}
	return &changefeed{
		coordinator:  c,
		ID:           cfID,
		Info:         info,
		configBytes:  bytes,
		checkpointTs: checkpointTs,
		// init the first status
		State: &MaintainerStatus{
			MaintainerStatus: &heartbeatpb.MaintainerStatus{
				CheckpointTs: checkpointTs,
				FeedState:    string(info.State),
			},
		},
	}
}

func (c *changefeed) UpdateStatus(status scheduler.InferiorStatus) {
	c.State = status.(*MaintainerStatus)
	if c.State != nil && c.State.CheckpointTs > c.checkpointTs {
		c.checkpointTs = c.State.CheckpointTs
	}
	c.lastHeartBeat = time.Now()
}
func (c *changefeed) SetStateMachine(state *scheduler.StateMachine) {
	c.stateMachine = state
}

func (c *changefeed) GetStateMachine() *scheduler.StateMachine {
	return c.stateMachine
}

type MaintainerStatus struct {
	*heartbeatpb.MaintainerStatus
}

func (s *MaintainerStatus) GetInferiorID() scheduler.InferiorID {
	return scheduler.ChangefeedID(model.DefaultChangeFeedID(s.ChangefeedID))
}
func (s *MaintainerStatus) GetInferiorState() heartbeatpb.ComponentState {
	return s.State
}

func (c *changefeed) NewAddInferiorMessage(server model.CaptureID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(messaging.ServerId(server),
		messaging.MaintainerManagerTopic,
		&heartbeatpb.AddMaintainerRequest{
			Id:           c.ID.ID,
			CheckpointTs: c.checkpointTs,
			Config:       c.configBytes,
		})
}

func (c *changefeed) NewRemoveInferiorMessage(server model.CaptureID) *messaging.TargetMessage {
	cf, ok := c.coordinator.lastState.Changefeeds[c.ID]
	cascade := !ok || cf == nil || !shouldRunChangefeed(cf.Info.State)
	return messaging.NewSingleTargetMessage(messaging.ServerId(server),
		messaging.MaintainerManagerTopic,
		&heartbeatpb.RemoveMaintainerRequest{
			Id:      c.ID.ID,
			Cascade: cascade,
		})
}
