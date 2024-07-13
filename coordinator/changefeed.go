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

	c *coordinator
}

func (c *coordinator) NewChangefeed(ID InferiorID) Inferior {
	cfID := model.ChangeFeedID(ID.(ChangefeedID))
	return &changefeed{
		ID: cfID,
		c:  c,
		//Info:   allChangefeeds[cfID],
		//Status: &model.ChangeFeedStatus{},
	}
}

func (c *changefeed) UpdateStatus(status InferiorStatus) {
	c.State = status.(*ChangefeedStatus)
	c.lastHeartBeat = time.Now()
}

func (c *changefeed) GetID() InferiorID {
	return ChangefeedID(c.ID)
}

func (c *changefeed) NewInferiorStatus(status ComponentStatus) InferiorStatus {
	return &ChangefeedStatus{
		ID:     ChangefeedID(c.ID),
		Status: status,
	}
}

func (c *changefeed) IsAlive() bool {
	return time.Now().Sub(c.lastHeartBeat) < 10*time.Second
}

func (c *changefeed) NewAddInferiorMessage(server model.CaptureID, secondary bool) rpc.Message {
	return messaging.NewTargetMessage(messaging.ServerId(uuid.MustParse(server)),
		maintainerMangerTopic,
		messaging.TypeDispatchMaintainerRequest,
		&heartbeatpb.DispatchMaintainerRequest{
			AddMaintainers: []*heartbeatpb.AddMaintainerRequest{
				{
					Id:          c.ID.ID,
					IsSecondary: secondary,
				},
			},
		})
	//msg, ok := c.c.dispatchMsgs[server]
	//if !ok {
	//	msg = &rpc.CoordinatorRequest{To: uuid.MustParse(server),
	//		DispatchMaintainerRequest: &rpc.DispatchMaintainerRequest{
	//			AddMaintainerRequests: make([]*rpc.AddMaintainerRequest, 0),
	//		},
	//	}
	//	c.c.dispatchMsgs[server] = msg
	//}
	//msg.DispatchMaintainerRequest.AddMaintainerRequests = append(msg.DispatchMaintainerRequest.AddMaintainerRequests,
	//	&rpc.AddMaintainerRequest{
	//		ID:          c.ID,
	//		Config:      c.Info,
	//		Status:      c.Status,
	//		IsSecondary: secondary,
	//	})
	//return msg
}

func (c *changefeed) NewRemoveInferiorMessage(server model.CaptureID) rpc.Message {
	return messaging.NewTargetMessage(messaging.ServerId(uuid.MustParse(server)),
		maintainerMangerTopic,
		messaging.TypeDispatchMaintainerRequest,
		&heartbeatpb.DispatchMaintainerRequest{
			RemoveMaintainers: []*heartbeatpb.RemoveMaintainerRequest{
				{
					Id:      c.ID.ID,
					Cascade: false,
				},
			},
		})
	//msg, ok := c.c.dispatchMsgs[server]
	//if !ok {
	//	msg = &rpc.CoordinatorRequest{To: uuid.MustParse(server),
	//		DispatchMaintainerRequest: &rpc.DispatchMaintainerRequest{
	//			RemoveMaintainerRequests: make([]*rpc.RemoveMaintainerRequest, 0),
	//		},
	//	}
	//	c.c.dispatchMsgs[server] = msg
	//}
	//msg.DispatchMaintainerRequest.RemoveMaintainerRequests = append(msg.DispatchMaintainerRequest.RemoveMaintainerRequests,
	//	&rpc.RemoveMaintainerRequest{
	//		ID:      c.ID,
	//		Cascade: false,
	//	})
	//return msg
}

type ChangefeedStatus struct {
	ID              ChangefeedID
	Status          ComponentStatus
	ChangefeedState model.FeedState
	CheckpointTs    uint64
}

func (c *ChangefeedStatus) GetInferiorID() InferiorID {
	return InferiorID(c.ID)
}

func (c *ChangefeedStatus) GetInferiorState() ComponentStatus {
	return c.Status
}

type ChangefeedID model.ChangeFeedID

func (m ChangefeedID) String() string {
	return model.ChangeFeedID(m).ID
}

func (m ChangefeedID) Equal(id InferiorID) bool {
	return model.ChangeFeedID(m).ID == id.(ChangefeedID).ID
}
func (m ChangefeedID) Less(id InferiorID) bool {
	return model.ChangeFeedID(m).ID < id.(ChangefeedID).ID
}
