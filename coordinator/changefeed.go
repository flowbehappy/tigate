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

	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/rpc"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/google/uuid"
	"github.com/pingcap/tiflow/cdc/model"
)

// changefeed tracks the scheduled maintainer on coordinator side
type changefeed struct {
	ID    model.ChangeFeedID
	State *ChangefeedStatus

	Info   *model.ChangeFeedInfo
	Status *model.ChangeFeedStatus

	lastHeartBeat time.Time
}

func NewChangefeed(ID scheduler.InferiorID) scheduler.Inferior {
	return &changefeed{
		ID: model.ChangeFeedID(ID.(ChangefeedID)),
	}
}

func (c *changefeed) UpdateStatus(status scheduler.InferiorStatus) {
	c.State = status.(*ChangefeedStatus)
	c.lastHeartBeat = time.Now()
}

func (c *changefeed) GetID() scheduler.InferiorID {
	return ChangefeedID(c.ID)
}

func (c *changefeed) NewInferiorStatus(status scheduler.ComponentStatus) scheduler.InferiorStatus {
	return &ChangefeedStatus{
		ID:     ChangefeedID(c.ID),
		Status: status,
	}
}

func (c *changefeed) IsAlive() bool {
	return time.Now().Sub(c.lastHeartBeat) < 10*time.Second
}

func (c *changefeed) NewAddInferiorMessage(server model.CaptureID, secondary bool) rpc.Message {
	return &rpc.CoordinatorRequest{
		To: messaging.ServerId(uuid.MustParse(server)),
		DispatchMaintainerRequest: &rpc.DispatchMaintainerRequest{
			AddMaintainerRequests: []*rpc.AddMaintainerRequest{
				{
					ID:          c.ID,
					Config:      c.Info,
					Status:      c.Status,
					IsSecondary: secondary,
				}}},
	}
}

func (c *changefeed) NewRemoveInferiorMessage(server model.CaptureID) rpc.Message {
	return &rpc.CoordinatorRequest{
		To: messaging.ServerId(uuid.MustParse(server)),
		DispatchMaintainerRequest: &rpc.DispatchMaintainerRequest{
			RemoveMaintainerRequests: []*rpc.RemoveMaintainerRequest{
				{
					ID:      c.ID,
					Cascade: false,
				}}},
	}
}

type ChangefeedStatus struct {
	ID              ChangefeedID
	Status          scheduler.ComponentStatus
	ChangefeedState model.FeedState
	CheckpointTs    uint64
}

func (c *ChangefeedStatus) GetInferiorID() scheduler.InferiorID {
	return scheduler.InferiorID(c.ID)
}

func (c *ChangefeedStatus) GetInferiorState() scheduler.ComponentStatus {
	return c.Status
}

type ChangefeedID model.ChangeFeedID

func (m ChangefeedID) String() string {
	return model.ChangeFeedID(m).String()
}
func (m ChangefeedID) Equal(id scheduler.InferiorID) bool {
	return model.ChangeFeedID(m).String() == id.String()
}
func (m ChangefeedID) Less(id scheduler.InferiorID) bool {
	return model.ChangeFeedID(m).String() < id.String()
}
