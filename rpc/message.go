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

package rpc

import (
	"encoding/json"

	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

// Message for rpc
// todo: define it in proto file
type Message interface {
}

type CoordinatorRequest struct {
	To                        messaging.ServerId           `json:"-"`
	BootstrapRequest          *CoordinatorBootstrapRequest `json:"bootstrap_request"`
	DispatchMaintainerRequest *DispatchMaintainerRequest   `json:"dispatch_maintainer_request"`
}

func (c *CoordinatorRequest) Encode() ([]byte, error) {
	return json.Marshal(c)
}

func DecodeCoordinatorRequest(data []byte) (*CoordinatorRequest, error) {
	var req CoordinatorRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &req, nil
}

type CoordinatorBootstrapRequest struct {
	Version int64
}

type DispatchMaintainerRequest struct {
	AddMaintainerRequests    []*AddMaintainerRequest    `json:"add_maintainer_requests"`
	RemoveMaintainerRequests []*RemoveMaintainerRequest `json:"remove_maintainer_requests"`
}

type BatchRemoveMaintainerRequest struct {
	Requests []*RemoveMaintainerRequest `json:"requests"`
}

type AddMaintainerRequest struct {
	ID          model.ChangeFeedID      `json:"id"`
	Config      *model.ChangeFeedInfo   `json:"config"`
	Status      *model.ChangeFeedStatus `json:"status"`
	IsSecondary bool                    `json:"is_secondary"`
}

type RemoveMaintainerRequest struct {
	ID      model.ChangeFeedID `json:"id,omitempty"`
	Cascade bool               `json:"cascade,omitempty"`
}

type MaintainerManagerRequest struct {
	MaintainerStatus []*MaintainerStatus `json:"maintainer_status"`
}

func (m *MaintainerManagerRequest) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func DecodeMaintainerManagerRequest(data []byte) (*MaintainerManagerRequest, error) {
	var req MaintainerManagerRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &req, nil
}

type MaintainerStatus struct {
	ID              model.ChangeFeedID `json:"id"`
	ChangefeedState model.FeedState    `json:"changefeed_state"`
	SchedulerState  int                `json:"scheduler_state"`
	CheckpointTs    uint64             `json:"checkpoint_ts"`
}
