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
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestNewChangefeed(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test")
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   model.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	checkpointTs := uint64(100)
	cf := NewChangefeed(cfID, info, checkpointTs)

	require.Equal(t, cfID, cf.ID)
	require.Equal(t, info, cf.GetInfo())
	require.Equal(t, checkpointTs, cf.GetLastSavedCheckPointTs())
	require.True(t, cf.IsMQSink())
}

func TestChangefeed_GetSetInfo(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test")
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   model.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100)

	newInfo := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9097",
		State:   model.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf.SetInfo(newInfo)
	require.Equal(t, newInfo, cf.GetInfo())
}

func TestChangefeed_GetSetNodeID(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test")
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   model.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100)

	nodeID := node.ID("node-1")
	cf.setNodeID(nodeID)
	require.Equal(t, nodeID, cf.GetNodeID())
}

func TestChangefeed_UpdateStatus(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test")
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   model.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100)

	newStatus := &heartbeatpb.MaintainerStatus{CheckpointTs: 200}
	updated, state, err := cf.UpdateStatus(newStatus)
	require.False(t, updated)
	require.Equal(t, model.StateNormal, state)
	require.Nil(t, err)
	require.Equal(t, newStatus, cf.GetStatus())
}

func TestChangefeed_IsMQSink(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test")
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   model.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100)

	require.True(t, cf.IsMQSink())
}

func TestChangefeed_GetSetLastSavedCheckPointTs(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test")
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   model.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100)

	newTs := uint64(200)
	cf.SetLastSavedCheckPointTs(newTs)
	require.Equal(t, newTs, cf.GetLastSavedCheckPointTs())
}

func TestChangefeed_NewAddMaintainerMessage(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test")
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   model.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100)

	server := node.ID("server-1")
	msg := cf.NewAddMaintainerMessage(server)
	require.Equal(t, server, msg.To)
	require.Equal(t, messaging.MaintainerManagerTopic, msg.Topic)
}

func TestChangefeed_NewRemoveMaintainerMessage(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test")
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   model.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100)

	server := node.ID("server-1")
	msg := cf.NewRemoveMaintainerMessage(server, true, true)
	require.Equal(t, server, msg.To)
	require.Equal(t, messaging.MaintainerManagerTopic, msg.Topic)
}

func TestChangefeed_NewCheckpointTsMessage(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test")
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   model.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100)

	ts := uint64(200)
	msg := cf.NewCheckpointTsMessage(ts)
	require.Equal(t, cf.nodeID, msg.To)
	require.Equal(t, messaging.MaintainerManagerTopic, msg.Topic)
}

func TestRemoveMaintainerMessage(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test")
	server := node.ID("server-1")
	msg := RemoveMaintainerMessage(cfID, server, true, true)
	require.Equal(t, server, msg.To)
	require.Equal(t, messaging.MaintainerManagerTopic, msg.Topic)
}
