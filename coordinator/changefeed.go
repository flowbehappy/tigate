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
	"github.com/pingcap/tiflow/cdc/model"
)

// changefeed tracks the scheduled maintainer on coordinator side
type changefeed struct {
	ID    model.ChangeFeedID
	State *heartbeatpb.MaintainerStatus

	Info   *model.ChangeFeedInfo
	Status *model.ChangeFeedStatus

	lastHeartBeat time.Time
}

func newChangefeed(ID model.ChangeFeedID) *changefeed {
	return &changefeed{
		ID:     ID,
		Info:   allChangefeeds[ID],
		Status: &model.ChangeFeedStatus{},
	}
}

func (c *changefeed) UpdateStatus(status *heartbeatpb.MaintainerStatus) {
	c.State = status
	c.lastHeartBeat = time.Now()
}

func (c *changefeed) NewInferiorStatus(status heartbeatpb.ComponentState) *heartbeatpb.MaintainerStatus {
	return &heartbeatpb.MaintainerStatus{
		ChangefeedID: c.ID.ID,
		State:        status,
	}
}

func (c *changefeed) NewAddInferiorMessage(server model.CaptureID, secondary bool) rpc.Message {
	return messaging.NewTargetMessage(messaging.ServerId(server),
		maintainerManagerTopic,
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
	return messaging.NewTargetMessage(messaging.ServerId(server), maintainerManagerTopic,
		&heartbeatpb.DispatchMaintainerRequest{
			RemoveMaintainers: []*heartbeatpb.RemoveMaintainerRequest{
				{
					Id:      c.ID.ID,
					Cascade: false,
				},
			},
		})
}
