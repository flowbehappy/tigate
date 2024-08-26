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

package maintainer

import (
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestHandleBootstrapResponse(t *testing.T) {
	b := NewBootstrapper("test", func(id model.CaptureID) *messaging.TargetMessage {
		return &messaging.TargetMessage{}
	})
	msgs := b.HandleNewNodes([]*common.NodeInfo{{ID: "ab"}, {ID: "cd"}})
	require.Len(t, msgs, 2)
	// not found
	cached := b.HandleBootstrapResponse(
		"ef",
		&heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: "cf",
			Statuses:     []*heartbeatpb.TableSpanStatus{{}},
		})
	require.Nil(t, cached)

	// not all bootstrapped
	cached = b.HandleBootstrapResponse(
		"ab",
		&heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: "cf",
			Statuses:     []*heartbeatpb.TableSpanStatus{{}},
		})
	require.Nil(t, cached)
	// all node bootstrapped
	cached = b.HandleBootstrapResponse(
		"cd",
		&heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: "cf",
			Statuses:     []*heartbeatpb.TableSpanStatus{{}, {}},
		})
	require.NotNil(t, cached)
	require.Equal(t, 1, len(cached["ab"].Statuses))
	require.Equal(t, 2, len(cached["cd"].Statuses))
	require.True(t, b.CheckAllNodeInitialized())
}

func TestAddNewNode(t *testing.T) {
	b := NewBootstrapper("test", func(id model.CaptureID) *messaging.TargetMessage {
		return &messaging.TargetMessage{}
	})
	msgs := b.HandleNewNodes([]*common.NodeInfo{{ID: "ab"}})
	require.Len(t, msgs, 1)
	require.True(t, b.nodes["ab"].state == NodeStateUninitialized)
	msgs = b.HandleNewNodes([]*common.NodeInfo{{
		ID: "ab",
	}, {ID: "cd"}})
	require.Len(t, msgs, 1)
	require.True(t, b.nodes["ab"].state == NodeStateUninitialized)
	require.True(t, b.nodes["cd"].state == NodeStateUninitialized)
}

func TestHandleRemoveNodes(t *testing.T) {
	b := NewBootstrapper("test", func(id model.CaptureID) *messaging.TargetMessage {
		return &messaging.TargetMessage{}
	})
	msgs := b.HandleNewNodes([]*common.NodeInfo{{ID: "ab"}, {ID: "cd"}})
	require.Len(t, msgs, 2)
	// bootstrap one node and the remove another, bootstrapper should be initialized
	cached := b.HandleRemoveNodes([]string{"ef"})
	require.Nil(t, cached)
	cached = b.HandleBootstrapResponse(
		"ab",
		&heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: "cf",
			Statuses:     []*heartbeatpb.TableSpanStatus{{}, {}},
		})
	require.Nil(t, cached)
	cached = b.HandleRemoveNodes([]string{"cd"})
	require.Equal(t, 2, len(cached["ab"].Statuses))
	require.True(t, b.CheckAllNodeInitialized())
}

func TestResendBootstrapMessage(t *testing.T) {
	b := NewBootstrapper("test", func(id model.CaptureID) *messaging.TargetMessage {
		return &messaging.TargetMessage{
			To: messaging.ServerId(id),
		}
	})
	b.resendInterval = time.Second * 2
	b.timeNowFunc = func() time.Time { return time.Unix(0, 0) }
	msgs := b.HandleNewNodes([]*common.NodeInfo{{ID: "ab"}})
	require.Len(t, msgs, 1)
	b.timeNowFunc = func() time.Time {
		return time.Unix(1, 0)
	}
	msgs = b.HandleNewNodes([]*common.NodeInfo{{ID: "cd"}})
	require.Len(t, msgs, 1)
	b.timeNowFunc = func() time.Time {
		return time.Unix(2, 0)
	}
	msgs = b.ResendBootstrapMessage()
	require.Len(t, msgs, 1)
	require.Equal(t, msgs[0].To, messaging.ServerId("ab"))
}

func TestCheckAllNodeInitialized(t *testing.T) {
	b := NewBootstrapper("test", func(id model.CaptureID) *messaging.TargetMessage {
		return &messaging.TargetMessage{}
	})
	msgs := b.HandleNewNodes([]*common.NodeInfo{{ID: "ab"}})
	require.Len(t, msgs, 1)
	require.False(t, b.CheckAllNodeInitialized())
	b.HandleBootstrapResponse(
		"ab",
		&heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: "cf",
			Statuses:     []*heartbeatpb.TableSpanStatus{{}},
		})
	require.True(t, b.CheckAllNodeInitialized())
}

func TestGetAllNodes(t *testing.T) {
	b := NewBootstrapper("test", func(id model.CaptureID) *messaging.TargetMessage {
		return &messaging.TargetMessage{}
	})
	b.HandleNewNodes([]*common.NodeInfo{{ID: "ab"}, {ID: "cd"}})
	nodes := b.GetAllNodes()
	require.Equal(t, 2, len(nodes))
	// modify nodes out of bootstrap
	delete(nodes, "ab")
	require.Equal(t, 1, len(nodes))
}
