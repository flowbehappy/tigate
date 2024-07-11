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

package server

import (
	"context"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/util"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const NodeManagerName = "node-manager"

var TempEpoch = uint64(1)

// NodeManager manager the read view of all captures, other modules can get the captures information from it
// and register server update event handler
type NodeManager struct {
	session       *concurrency.Session
	etcdClient    etcd.CDCEtcdClient
	messageCenter messaging.MessageCenter
	nodes         map[string]*model.CaptureInfo

	handleRWLock sync.RWMutex
	handles      map[string]func([]*model.CaptureInfo, []*model.CaptureInfo)
}

func NewCaptureManager(
	session *concurrency.Session,
	etcdClient etcd.CDCEtcdClient,
	mc messaging.MessageCenter,
) *NodeManager {
	return &NodeManager{
		session:       session,
		etcdClient:    etcdClient,
		messageCenter: mc,
		nodes:         make(map[string]*model.CaptureInfo),
	}
}

func (c *NodeManager) Name() string {
	return NodeManagerName
}

// Tick is triggered by the server update events
func (c *NodeManager) Tick(ctx context.Context,
	raw orchestrator.ReactorState) (orchestrator.ReactorState, error) {
	state := raw.(*orchestrator.GlobalReactorState)
	log.Info("node manager tick", zap.Any("captures", state.Captures), zap.Any("owner", state.Owner))
	if len(c.nodes) != len(state.Captures) {
		// find changes
		removed := make([]*model.CaptureInfo, 0)
		newCaptures := make([]*model.CaptureInfo, 0)
		allCaptures := make(map[string]*model.CaptureInfo, len(state.Captures))
		for _, capture := range c.nodes {
			if _, exist := state.Captures[capture.ID]; !exist {
				sid := messaging.ServerId(uuid.MustParse(capture.ID))
				c.messageCenter.RemoveTarget(sid)
				removed = append(removed, capture)
			}
		}

		for _, capture := range state.Captures {
			if _, exist := c.nodes[capture.ID]; !exist {

				sid := messaging.ServerId(uuid.MustParse(capture.ID))
				c.messageCenter.AddTarget(sid, TempEpoch, capture.AdvertiseAddr)
				newCaptures = append(newCaptures, capture)
			}
			allCaptures[capture.ID] = capture
		}
		log.Info("server change detected", zap.Any("removed", removed),
			zap.Any("new", newCaptures))
		c.nodes = allCaptures

		// notify handler
		c.handleRWLock.RLock()
		for _, handle := range c.handles {
			handle(newCaptures, removed)
		}
		c.handleRWLock.RUnlock()
	}
	return state, nil
}

// GetAliveCaptures get all alive captures, the caller mustn't modify the returned map
func (c *NodeManager) GetAliveCaptures() map[string]*model.CaptureInfo {
	return c.nodes
}

func (c *NodeManager) Run(ctx context.Context) error {
	cfg := config.GetGlobalServerConfig()
	watcher := NewEtcdWatcher(c.etcdClient,
		c.session,
		// captures info key prefix
		etcd.BaseKey(c.etcdClient.GetClusterID())+"/__cdc_meta__/capture",
		util.RoleOwner.String())

	return watcher.runEtcdWorker(ctx, c,
		orchestrator.NewGlobalState(c.etcdClient.GetClusterID(),
			cfg.CaptureSessionTTL), time.Millisecond*50)
}

func (c *NodeManager) RegisterCaptureChangeHandler(name string,
	f func([]*model.CaptureInfo, []*model.CaptureInfo)) {
	c.handleRWLock.Lock()
	c.handles[name] = f
	c.handleRWLock.Unlock()
}

func (c *NodeManager) Close(ctx context.Context) error {
	return nil
}
