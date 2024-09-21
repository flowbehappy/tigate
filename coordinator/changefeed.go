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
	"net/url"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/node"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink"
	"go.uber.org/zap"
)

type ChangeFeedDB interface {
	GetChangefeedConfig(id model.ChangeFeedID) *model.ChangefeedConfig
}

// changefeed tracks the scheduled maintainer on coordinator side
type changefeed struct {
	ID     model.ChangeFeedID
	Status *heartbeatpb.MaintainerStatus

	Info *model.ChangeFeedInfo

	configBytes []byte

	coordinator  *coordinator
	stateMachine *scheduler.StateMachine[common.MaintainerID]

	lastSavedCheckpointTs uint64
	isMQSink              bool
}

func newChangefeed(c *coordinator,
	cfID model.ChangeFeedID,
	info *model.ChangeFeedInfo,
	checkpointTs uint64) *changefeed {
	uri, err := url.Parse(info.SinkURI)
	if err != nil {
		log.Panic("unable to marshal changefeed config",
			zap.Error(err))
	}
	bytes, err := json.Marshal(info)
	if err != nil {
		log.Panic("unable to marshal changefeed config",
			zap.Error(err))
	}
	return &changefeed{
		coordinator:           c,
		ID:                    cfID,
		Info:                  info,
		configBytes:           bytes,
		lastSavedCheckpointTs: checkpointTs,
		isMQSink:              sink.IsMQScheme(uri.Scheme),
		// init the first status
		Status: &heartbeatpb.MaintainerStatus{
			CheckpointTs: checkpointTs,
			FeedState:    string(info.State),
		},
	}
}

func (c *changefeed) UpdateStatus(status any) {
	if status != nil {
		newStatus := status.(*heartbeatpb.MaintainerStatus)
		if newStatus.CheckpointTs > c.Status.CheckpointTs {
			c.Status = newStatus
		}
	}
}

func (c *changefeed) NewAddInferiorMessage(server node.ID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(server,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.AddMaintainerRequest{
			Id:           c.ID.ID,
			CheckpointTs: c.Status.CheckpointTs,
			Config:       c.configBytes,
		})
}

func (c *changefeed) NewRemoveInferiorMessage(server node.ID) *messaging.TargetMessage {
	cf, ok := c.coordinator.lastState.Changefeeds[c.ID]
	cascade := !ok || cf == nil || !shouldRunChangefeed(cf.Info.State)
	return messaging.NewSingleTargetMessage(server,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.RemoveMaintainerRequest{
			Id:      c.ID.ID,
			Cascade: cascade,
		})
}

func (c *changefeed) NewCheckpointTsMessage(ts uint64) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(c.stateMachine.Primary,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.CheckpointTsMessage{
			ChangefeedID: c.ID.ID,
			CheckpointTs: ts,
		})
}
