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

package logcoordinator

import (
	"context"
	"sync"
	"time"

	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type LogCoordinator interface {
	Run(ctx context.Context) error
}

type logCoordinator struct {
	messageCenter messaging.MessageCenter

	mu    sync.RWMutex
	nodes map[node.ID]*node.Info
}

func New() LogCoordinator {
	messageCenter := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	c := &logCoordinator{
		messageCenter: messageCenter,

		nodes: make(map[node.ID]*node.Info),
	}
	// recv and handle messages
	messageCenter.RegisterHandler(messaging.LogCoordinatorTopic, c.handleMessage)
	// watch node changes
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodes := nodeManager.GetAliveNodes()
	for id, n := range nodes {
		c.nodes[id] = n
	}
	nodeManager.RegisterNodeChangeHandler("log-coordinator", c.handleNodeChange)
	return c
}

func (c *logCoordinator) Run(ctx context.Context) error {
	tick := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			// TODO: send heartbeat messages
		}
	}
}

func (c *logCoordinator) handleMessage(_ context.Context, msg *messaging.TargetMessage) error {
	// TODO: implement this
	return nil
}

func (c *logCoordinator) handleNodeChange(allNodes map[node.ID]*node.Info) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id := range c.nodes {
		if _, ok := allNodes[id]; !ok {
			delete(c.nodes, id)
			log.Info("node removed", zap.String("nodeId", id.String()))
		}
	}
	for id, n := range allNodes {
		if _, ok := c.nodes[id]; !ok {
			c.nodes[id] = n
			log.Info("node added", zap.String("nodeId", id.String()))
		}
	}
}
