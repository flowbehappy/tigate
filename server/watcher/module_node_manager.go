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

package watcher

import (
	"context"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const NodeManagerName = "node-manager"

type NodeChangeHandler func(newNodes []*common.NodeInfo, removedNodes []*common.NodeInfo)

// NodeManager manager the read view of all captures, other modules can get the captures information from it
// and register server update event handler
type NodeManager struct {
	session    *concurrency.Session
	etcdClient etcd.CDCEtcdClient
	nodes      map[common.NodeID]*common.NodeInfo

	nodeChangeHandlers struct {
		sync.RWMutex
		m map[string]NodeChangeHandler
	}
}

func NewNodeManager(
	session *concurrency.Session,
	etcdClient etcd.CDCEtcdClient,
) *NodeManager {
	return &NodeManager{
		session:    session,
		etcdClient: etcdClient,
		nodes:      make(map[common.NodeID]*common.NodeInfo),
		nodeChangeHandlers: struct {
			sync.RWMutex
			m map[string]NodeChangeHandler
		}{m: make(map[string]NodeChangeHandler)},
	}
}

func (c *NodeManager) Name() string {
	return NodeManagerName
}

// Tick is triggered by the server update events
func (c *NodeManager) Tick(
	ctx context.Context,
	raw orchestrator.ReactorState,
) (orchestrator.ReactorState, error) {
	state := raw.(*orchestrator.GlobalReactorState)
	// find changes
	removed := make([]*common.NodeInfo, 0)
	newNodes := make([]*common.NodeInfo, 0)
	allNodes := make(map[common.NodeID]*common.NodeInfo, len(state.Captures))

	for _, node := range c.nodes {
		if _, exist := state.Captures[node.ID]; !exist {
			removed = append(removed, node)
		}
	}

	for _, capture := range state.Captures {
		if _, exist := c.nodes[capture.ID]; !exist {
			node := common.CaptureInfoToNodeInfo(capture)
			newNodes = append(newNodes, node)
		}
		allNodes[capture.ID] = common.CaptureInfoToNodeInfo(capture)
	}

	if len(removed) != 0 || len(newNodes) != 0 {
		log.Info("server change detected", zap.Any("removed", removed),
			zap.Any("new", newNodes))
	}

	c.nodes = allNodes

	// handle node change event
	c.nodeChangeHandlers.RLock()
	defer c.nodeChangeHandlers.RUnlock()
	for _, handler := range c.nodeChangeHandlers.m {
		handler(newNodes, removed)
	}
	return state, nil
}

// GetAliveNodes get all alive captures, the caller mustn't modify the returned map
func (c *NodeManager) GetAliveNodes() map[common.NodeID]*common.NodeInfo {
	return c.nodes
}

func (c *NodeManager) Run(ctx context.Context) error {
	cfg := config.GetGlobalServerConfig()
	watcher := NewEtcdWatcher(c.etcdClient,
		c.session,
		// captures info key prefix
		etcd.BaseKey(c.etcdClient.GetClusterID())+"/__cdc_meta__/capture",
		"capture-manager")

	return watcher.RunEtcdWorker(ctx, c,
		orchestrator.NewGlobalState(c.etcdClient.GetClusterID(),
			cfg.CaptureSessionTTL), time.Millisecond*50)
}

func (c *NodeManager) RegisterNodeChangeHandler(name string, handler NodeChangeHandler) {
	c.nodeChangeHandlers.Lock()
	defer c.nodeChangeHandlers.Unlock()
	c.nodeChangeHandlers.m[name] = handler
}

func (c *NodeManager) Close(ctx context.Context) error {
	return nil
}
