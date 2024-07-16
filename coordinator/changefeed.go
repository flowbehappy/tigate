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
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/rpc"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/tiflow/cdc/model"
)

// changefeed tracks the scheduled maintainer on coordinator side
type changefeed struct {
	ID    model.ChangeFeedID
	State *MaintainerStatus

	Info   *model.ChangeFeedInfo
	Status *model.ChangeFeedStatus

	lastHeartBeat time.Time
}

func newChangefeed(ID scheduler.InferiorID) scheduler.Inferior {
	cfID := model.ChangeFeedID(ID.(scheduler.ChangefeedID))
	return &changefeed{
		ID:     model.ChangeFeedID(ID.(scheduler.ChangefeedID)),
		Info:   allChangefeeds[cfID],
		Status: &model.ChangeFeedStatus{},
	}
}

func (c *changefeed) GetID() scheduler.InferiorID {
	return scheduler.ChangefeedID(c.ID)
}

func (c *changefeed) UpdateStatus(status scheduler.InferiorStatus) {
	c.State = status.(*MaintainerStatus)
	c.lastHeartBeat = time.Now()
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

func (c *changefeed) NewInferiorStatus(status heartbeatpb.ComponentState) scheduler.InferiorStatus {
	return &MaintainerStatus{MaintainerStatus: &heartbeatpb.MaintainerStatus{
		ChangefeedID: c.ID.ID,
		State:        status,
	}}
}

func (c *changefeed) IsAlive() bool {
	return true
}

func (c *changefeed) NewAddInferiorMessage(server model.CaptureID, secondary bool) rpc.Message {
	return messaging.NewTargetMessage(messaging.ServerId(server),
		maintainerMangerTopic,
		&heartbeatpb.DispatchMaintainerRequest{
			AddMaintainers: []*heartbeatpb.AddMaintainerRequest{
				{
					Id:          c.ID.ID,
					IsSecondary: secondary,
				},
			},
		})
}

func (c *changefeed) NewRemoveInferiorMessage(server model.CaptureID) rpc.Message {
	return messaging.NewTargetMessage(messaging.ServerId(server), maintainerMangerTopic,
		&heartbeatpb.DispatchMaintainerRequest{
			RemoveMaintainers: []*heartbeatpb.RemoveMaintainerRequest{
				{
					Id:      c.ID.ID,
					Cascade: false,
				},
			},
		})
}
