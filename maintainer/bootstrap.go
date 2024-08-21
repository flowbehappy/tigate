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
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"time"
)

type NodeState int

const (
	// NodeStateUninitialized means the server status is unknown,
	// no heartbeat response received yet.
	NodeStateUninitialized NodeState = 1
	// NodeStateInitialized means scheduler has received heartbeat response.
	NodeStateInitialized NodeState = 2
)

func NewNodeStatus(node *common.NodeInfo) *NodeStatus {
	return &NodeStatus{
		state: NodeStateUninitialized,
		node:  node,
	}
}

type NodeStatus struct {
	state               NodeState
	node                *common.NodeInfo
	cachedBootstrapResp *heartbeatpb.MaintainerBootstrapResponse
	lastBootstrapTime   time.Time
}

type Bootstrapper struct {
	nodes           map[common.NodeID]*NodeStatus
	bootstrapped    bool
	newBootstrapMsg scheduler.NewBootstrapFn
}

func NewBootstrapper(newBootstrapMsg scheduler.NewBootstrapFn) *Bootstrapper {
	return &Bootstrapper{
		nodes:           make(map[common.NodeID]*NodeStatus),
		bootstrapped:    false,
		newBootstrapMsg: newBootstrapMsg,
	}
}

// HandleBootstrapResponse cache the message reported remote node
func (b *Bootstrapper) HandleBootstrapResponse(from messaging.ServerId, msg *heartbeatpb.MaintainerBootstrapResponse) map[common.NodeID]*heartbeatpb.MaintainerBootstrapResponse {
	nodeID := common.NodeID(from)
	node, ok := b.nodes[nodeID]
	if !ok {
		log.Warn("node is not found, ignore", zap.String("from", nodeID))
		return nil
	}
	node.cachedBootstrapResp = msg
	node.state = NodeStateInitialized
	// first bootstrapped time, return the cached resp and clear it
	if !b.bootstrapped && b.checkAllCaptureInitialized() {
		b.bootstrapped = true
		allCachedResp := make(map[common.NodeID]*heartbeatpb.MaintainerBootstrapResponse, len(b.nodes))
		for _, node := range b.nodes {
			allCachedResp[node.node.ID] = node.cachedBootstrapResp
			// clear the cached data
			node.cachedBootstrapResp = nil
		}
	}
	return nil
}

func (b *Bootstrapper) HandleNewNodes(nodes map[common.NodeID]*common.NodeInfo) []*messaging.TargetMessage {
	msgs := make([]*messaging.TargetMessage, 0, len(nodes))
	for id, info := range nodes {
		if _, ok := b.nodes[info.ID]; !ok {
			// A new server.
			b.nodes[info.ID] = NewNodeStatus(info)
			log.Info("find a new server",
				zap.String("captureAddr", info.AdvertiseAddr),
				zap.String("server", info.ID))
			msgs = append(msgs, b.newBootstrapMsg(id))
			b.nodes[info.ID].lastBootstrapTime = time.Now()
		}
	}
	return msgs
}

func (b *Bootstrapper) ResendBootstrapMessage() []*messaging.TargetMessage {
	var msgs []*messaging.TargetMessage
	if !b.CheckAllNodeInitialized() {
		for id, node := range b.nodes {
			if node.state == NodeStateUninitialized &&
				time.Since(node.lastBootstrapTime) > time.Millisecond*500 {
				msgs = append(msgs, b.newBootstrapMsg(id))
				node.lastBootstrapTime = time.Now()
			}
		}
	}
	return msgs
}

// CheckAllNodeInitialized check if all server is initialized.
// returns true when all server reports the bootstrap response
func (b *Bootstrapper) CheckAllNodeInitialized() bool {
	return b.bootstrapped && b.checkAllCaptureInitialized()
}

func (b *Bootstrapper) checkAllCaptureInitialized() bool {
	for _, captureStatus := range b.nodes {
		// CaptureStateStopping is also considered initialized, because when
		// a server shutdown, it becomes stopping, we need to move its tables
		// to other captures.
		if captureStatus.state == NodeStateUninitialized {
			return false
		}
	}
	return len(b.nodes) != 0
}
