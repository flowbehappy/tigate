// Copyright 2022 PingCAP, Inc.
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

package upstream

import (
	"context"
	"strings"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	pd "github.com/tikv/pd/client"
	clientV3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Manager manages all upstream.
type Manager struct {
	// upstreamID map to *Upstream.
	ups *sync.Map
	// all upstream should be spawn from this ctx.
	ctx context.Context
	// Only use in Close().
	cancel func()
	// lock this mutex when add or delete a value of Manager.ups.
	mu sync.Mutex

	defaultUpstream *Upstream

	initUpstreamFunc func(context.Context, *Upstream) error
}

// NewManager creates a new Manager.
// ctx will be used to initialize upstream spawned by this Manager.
func NewManager(ctx context.Context) *Manager {
	ctx, cancel := context.WithCancel(ctx)
	return &Manager{
		ups:              new(sync.Map),
		ctx:              ctx,
		cancel:           cancel,
		initUpstreamFunc: initUpstream,
	}
}

// AddDefaultUpstream add the default upstream
func (m *Manager) AddDefaultUpstream(
	pdEndpoints []string,
	conf *security.Credential,
	pdClient pd.Client,
	etcdClient *clientV3.Client,
) (*Upstream, error) {
	// use the pdClient and etcdClient pass from cdc server as the default upstream
	// to reduce the creation times of pdClient to make cdc server more stable
	up := &Upstream{
		PdEndpoints:       pdEndpoints,
		SecurityConfig:    conf,
		PDClient:          pdClient,
		etcdCli:           etcdClient,
		isDefaultUpstream: true,
		status:            uninit,
		wg:                new(sync.WaitGroup),
		clock:             clock.New(),
	}
	if err := m.initUpstreamFunc(m.ctx, up); err != nil {
		return nil, cerror.Trace(err)
	}
	m.defaultUpstream = up
	m.ups.Store(up.ID, up)
	log.Info("default upstream is added", zap.Uint64("id", up.ID))
	return up, nil
}

// GetDefaultUpstream returns the default upstream
func (m *Manager) GetDefaultUpstream() (*Upstream, error) {
	if m.defaultUpstream == nil {
		return nil, cerror.ErrUpstreamNotFound
	}
	return m.defaultUpstream, nil
}

func (m *Manager) add(upstreamID uint64,
	pdEndpoints []string, conf *security.Credential,
) *Upstream {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.ups.Load(upstreamID)
	if ok {
		up := v.(*Upstream)
		up.resetIdleTime()
		return up
	}
	securityConf := &security.Credential{}
	if conf != nil {
		securityConf = &security.Credential{
			CAPath:        conf.CAPath,
			CertPath:      conf.CertPath,
			KeyPath:       conf.KeyPath,
			CertAllowedCN: conf.CertAllowedCN,
		}
	}
	up := newUpstream(pdEndpoints, securityConf)
	m.ups.Store(upstreamID, up)
	go func() {
		err := m.initUpstreamFunc(m.ctx, up)
		up.err.Store(err)
	}()
	up.resetIdleTime()
	log.Info("new upstream is added", zap.Uint64("id", up.ID))
	return up
}

// UpstreamInfo store in etcd.
type UpstreamInfo struct {
	ID            uint64   `json:"id"`
	PDEndpoints   string   `json:"pd-endpoints"`
	KeyPath       string   `json:"key-path"`
	CertPath      string   `json:"cert-path"`
	CAPath        string   `json:"ca-path"`
	CertAllowedCN []string `json:"cert-allowed-cn"`
}

// AddUpstream adds an upstream and init it.
func (m *Manager) AddUpstream(info *UpstreamInfo) *Upstream {
	return m.add(info.ID,
		strings.Split(info.PDEndpoints, ","),
		&security.Credential{
			CAPath:        info.CAPath,
			CertPath:      info.CertPath,
			KeyPath:       info.KeyPath,
			CertAllowedCN: info.CertAllowedCN,
		})
}

// Get gets a upstream by upstreamID.
func (m *Manager) Get(upstreamID uint64) (*Upstream, bool) {
	v, ok := m.ups.Load(upstreamID)
	if !ok {
		return nil, false
	}
	up := v.(*Upstream)
	return up, true
}

// Close closes all upstreams.
// Please make sure it will only be called once when capture exits.
func (m *Manager) Close() {
	m.cancel()
	m.ups.Range(func(k, v interface{}) bool {
		v.(*Upstream).Close()
		m.ups.Delete(k)
		return true
	})
}

// Visit on each upstream, return error on the first
func (m *Manager) Visit(visitor func(up *Upstream) error) error {
	var err error
	m.ups.Range(func(k, v interface{}) bool {
		err = visitor(v.(*Upstream))
		return err == nil
	})
	return err
}
