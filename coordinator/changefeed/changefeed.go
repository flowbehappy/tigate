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

package changefeed

import (
	"encoding/json"
	"net/url"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Changefeed is a memory present for changefeed info and status
type Changefeed struct {
	ID       model.ChangeFeedID
	Status   *heartbeatpb.MaintainerStatus
	Info     *model.ChangeFeedInfo
	IsMQSink bool

	nodeID      node.ID
	configBytes []byte

	lastSavedCheckpointTs *atomic.Uint64
}

// NewChangefeed creates a new changefeed instance
func NewChangefeed(cfID model.ChangeFeedID,
	info *model.ChangeFeedInfo,
	checkpointTs uint64) *Changefeed {
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
	log.Info("changefeed instance created",
		zap.String("id", cfID.String()),
		zap.String("state", string(info.State)))
	return &Changefeed{
		ID:                    cfID,
		Info:                  info,
		configBytes:           bytes,
		lastSavedCheckpointTs: atomic.NewUint64(checkpointTs),
		IsMQSink:              sink.IsMQScheme(uri.Scheme),
		// init the first Status
		Status: &heartbeatpb.MaintainerStatus{
			CheckpointTs: checkpointTs,
			FeedState:    string(info.State),
		},
	}
}

// SetNodeID set the node id of the changefeed, todo: make it thread-safe
func (c *Changefeed) SetNodeID(n node.ID) {
	c.nodeID = n
}

func (c *Changefeed) GetNodeID() node.ID {
	return c.nodeID
}

func (c *Changefeed) UpdateStatus(status any) {
	if status != nil {
		newStatus := status.(*heartbeatpb.MaintainerStatus)
		if newStatus.CheckpointTs > c.Status.CheckpointTs {
			c.Status = newStatus
		}
	}
}

func (c *Changefeed) SetLastSavedCheckPointTs(ts uint64) {
	c.lastSavedCheckpointTs.Store(ts)
}

func (c *Changefeed) GetLastSavedCheckPointTs() uint64 {
	return c.lastSavedCheckpointTs.Load()
}

func (c *Changefeed) NewAddMaintainerMessage(server node.ID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(server,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.AddMaintainerRequest{
			Id:           c.ID.ID,
			CheckpointTs: c.Status.CheckpointTs,
			Config:       c.configBytes,
		})
}

func (c *Changefeed) NewRemoveMaintainerMessage(server node.ID, caseCade bool) *messaging.TargetMessage {
	return RemoveMaintainerMessage(c.ID.ID, server, caseCade)
}

func (c *Changefeed) NewCheckpointTsMessage(ts uint64) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(c.nodeID,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.CheckpointTsMessage{
			ChangefeedID: c.ID.ID,
			CheckpointTs: ts,
		})
}

func RemoveMaintainerMessage(id string, server node.ID, caseCade bool) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(server,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.RemoveMaintainerRequest{
			Id:      id,
			Cascade: caseCade,
		})
}
