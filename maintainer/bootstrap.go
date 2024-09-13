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
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"time"
)

// Bootstrapper handle the logic of the maintainer startup
// when a maintainer is started, it must wait of node to reported their managed dispatchers
// only all dispatcher has reported its status, maintainer can schedule tables
// maintainer collects all working dispatchers and
type Bootstrapper struct {
	// changefeedID is the log identifier
	changefeedID string
	// bootstrap identify is the bootstrapper is already bootstrapped
	bootstrapped bool

	nodes           map[node.ID]*NodeStatus
	newBootstrapMsg scheduler.NewBootstrapFn

	// for ut test
	timeNowFunc    func() time.Time
	resendInterval time.Duration
}

// NewBootstrapper create a new bootstrap for a changefeed maintainer
func NewBootstrapper(cfID string, newBootstrapMsg scheduler.NewBootstrapFn) *Bootstrapper {
	return &Bootstrapper{
		changefeedID:    cfID,
		nodes:           make(map[node.ID]*NodeStatus),
		bootstrapped:    false,
		newBootstrapMsg: newBootstrapMsg,
		timeNowFunc:     time.Now,
		resendInterval:  time.Millisecond * 500,
	}
}

// HandleBootstrapResponse cache the message reported remote node
func (b *Bootstrapper) HandleBootstrapResponse(
	from node.ID,
	msg *heartbeatpb.MaintainerBootstrapResponse) map[node.ID]*heartbeatpb.MaintainerBootstrapResponse {
	node, ok := b.nodes[from]
	if !ok {
		log.Warn("node is not found, ignore",
			zap.String("changefeed", b.changefeedID),
			zap.Any("from", from))
		return nil
	}
	node.cachedBootstrapResp = msg
	node.state = NodeStateInitialized
	return b.fistBootstrap()
}

// HandleNewNodes add node to bootstrapper and return rpc messages that need to be sent to remote node
func (b *Bootstrapper) HandleNewNodes(nodes []*node.Info) []*messaging.TargetMessage {
	msgs := make([]*messaging.TargetMessage, 0, len(nodes))
	for _, info := range nodes {
		if _, ok := b.nodes[info.ID]; !ok {
			// A new server.
			b.nodes[info.ID] = NewNodeStatus(info)
			log.Info("find a new server",
				zap.String("changefeed", b.changefeedID),
				zap.String("captureAddr", info.AdvertiseAddr),
				zap.Any("server", info.ID))
			msgs = append(msgs, b.newBootstrapMsg(info.ID))
			b.nodes[info.ID].lastBootstrapTime = b.timeNowFunc()
		}
	}
	return msgs
}

// HandleRemoveNodes remove node from bootstrapper,
// finished bootstrap if all node are initialized after these node removed
// return cached bootstrap
func (b *Bootstrapper) HandleRemoveNodes(nodeIDs []node.ID) map[node.ID]*heartbeatpb.MaintainerBootstrapResponse {
	for _, id := range nodeIDs {
		status, ok := b.nodes[id]
		if ok {
			delete(b.nodes, id)
			log.Info("remove node from bootstrap",
				zap.String("changefeed", b.changefeedID),
				zap.Int("status", int(status.state)),
				zap.Any("id", id))
		} else {
			log.Info("node is node tracked by bootstrap",
				zap.String("changefeed", b.changefeedID),
				zap.Any("id", id))
		}
	}
	return b.fistBootstrap()
}

// ResendBootstrapMessage return rpc message that need to be resent
func (b *Bootstrapper) ResendBootstrapMessage() []*messaging.TargetMessage {
	var msgs []*messaging.TargetMessage
	if !b.CheckAllNodeInitialized() {
		now := b.timeNowFunc()
		for id, node := range b.nodes {
			if node.state == NodeStateUninitialized &&
				now.Sub(node.lastBootstrapTime) >= b.resendInterval {
				msgs = append(msgs, b.newBootstrapMsg(id))
				node.lastBootstrapTime = now
			}
		}
	}
	return msgs
}

// GetAllNodes return all nodes the tracked by bootstrapper, the returned value must not be modified
func (b *Bootstrapper) GetAllNodes() map[node.ID]*NodeStatus {
	return b.nodes
}

// CheckAllNodeInitialized check if all server is initialized.
// returns true when all server reports the bootstrap response and bootstrapped
func (b *Bootstrapper) CheckAllNodeInitialized() bool {
	return b.bootstrapped && b.checkAllCaptureInitialized()
}

// return true if all node reports the bootstrap response
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

// fistBootstrap check if bootstrapper is initialized first time,
// return nil is not bootstrapped or already bootstrapped before
// return cached heartbeatpb.MaintainerBootstrapResponse map if it's not bootstrapped before
// bootstrapper only return once
func (b *Bootstrapper) fistBootstrap() map[node.ID]*heartbeatpb.MaintainerBootstrapResponse {
	// first bootstrapped time, return the cached resp and clear it
	if !b.bootstrapped && b.checkAllCaptureInitialized() {
		b.bootstrapped = true
		allCachedResp := make(map[node.ID]*heartbeatpb.MaintainerBootstrapResponse, len(b.nodes))
		for _, node := range b.nodes {
			allCachedResp[node.node.ID] = node.cachedBootstrapResp
			// clear the cached data
			node.cachedBootstrapResp = nil
		}
		return allCachedResp
	}
	return nil
}

type NodeState int

const (
	// NodeStateUninitialized means the server status is unknown,
	// no bootstrap response received yet.
	NodeStateUninitialized NodeState = 1
	// NodeStateInitialized means scheduler has received bootstrap response.
	NodeStateInitialized NodeState = 2
)

func NewNodeStatus(node *node.Info) *NodeStatus {
	return &NodeStatus{
		state: NodeStateUninitialized,
		node:  node,
	}
}

// NodeStatus identify the node the need be bootstrapped
type NodeStatus struct {
	state               NodeState
	node                *node.Info
	cachedBootstrapResp *heartbeatpb.MaintainerBootstrapResponse
	lastBootstrapTime   time.Time
}
