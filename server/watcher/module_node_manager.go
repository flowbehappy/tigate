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
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/pingcap/tiflow/cdc/model"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const NodeManagerName = "node-manager"

type NodeChangeHandler func(map[node.ID]*node.Info)

// NodeManager manager the read view of all captures, other modules can get the captures information from it
// and register server update event handler
type NodeManager struct {
	session    *concurrency.Session
	etcdClient etcd.CDCEtcdClient
	nodes      map[node.ID]*node.Info

	nodeChangeHandlers struct {
		sync.RWMutex
		m map[node.ID]NodeChangeHandler
	}
}

func NewNodeManager(
	session *concurrency.Session,
	etcdClient etcd.CDCEtcdClient,
) *NodeManager {
	return &NodeManager{
		session:    session,
		etcdClient: etcdClient,
		nodes:      make(map[node.ID]*node.Info),
		nodeChangeHandlers: struct {
			sync.RWMutex
			m map[node.ID]NodeChangeHandler
		}{m: make(map[node.ID]NodeChangeHandler)},
	}
}

func (c *NodeManager) Name() string {
	return NodeManagerName
}

// Tick is triggered by the server update events
func (c *NodeManager) Tick(
	_ context.Context,
	raw orchestrator.ReactorState,
) (orchestrator.ReactorState, error) {
	state := raw.(*orchestrator.GlobalReactorState)
	// find changes
	changed := false
	allNodes := make(map[node.ID]*node.Info, len(state.Captures))

	for _, node := range c.nodes {
		if _, exist := state.Captures[model.CaptureID(node.ID)]; !exist {
			changed = true
		}
	}

	for _, capture := range state.Captures {
		if _, exist := c.nodes[node.ID(capture.ID)]; !exist {
			changed = true
		}
		allNodes[node.ID(capture.ID)] = node.CaptureInfoToNodeInfo(capture)
	}
	c.nodes = allNodes
	if changed {
		log.Info("server change detected")
		// handle node change event
		c.nodeChangeHandlers.RLock()
		defer c.nodeChangeHandlers.RUnlock()
		for _, handler := range c.nodeChangeHandlers.m {
			handler(allNodes)
		}
	}
	return state, nil
}

// GetAliveNodes get all alive captures, the caller mustn't modify the returned map
func (c *NodeManager) GetAliveNodes() map[node.ID]*node.Info {
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

func (c *NodeManager) RegisterNodeChangeHandler(name node.ID, handler NodeChangeHandler) {
	c.nodeChangeHandlers.Lock()
	defer c.nodeChangeHandlers.Unlock()
	c.nodeChangeHandlers.m[name] = handler
}

func (c *NodeManager) Close(_ context.Context) error {
	return nil
}
