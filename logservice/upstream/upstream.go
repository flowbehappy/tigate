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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flowbehappy/tigate/pkg/common"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/driver"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/security"
	tikvconfig "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	uatomic "github.com/uber-go/atomic"
	clientV3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	// indicate an upstream is created but not initialized.
	uninit int32 = iota
	// indicate an upstream is initialized and can work normally.
	normal
	// indicate an upstream is closing
	closing
	// indicate an upstream is closed.
	closed

	maxIdleDuration = time.Minute * 30
)

// Upstream holds resources of a TiDB cluster, it can be shared by many changefeeds
// and processors. All public fields and method of an upstream should be thread-safe.
// Please be careful that never change any exported field of an Upstream.
type Upstream struct {
	ID uint64

	PdEndpoints    []string
	SecurityConfig *security.Credential
	PDClient       pd.Client
	etcdCli        *clientV3.Client
	session        *concurrency.Session

	KVStorage   tidbkv.Storage
	RegionCache *tikv.RegionCache
	PDClock     pdutil.Clock
	// Only use in Close().
	cancel func()
	mu     sync.Mutex
	// record the time when Upstream.hc becomes zero.
	idleTime time.Time
	// use clock to facilitate unit test
	clock  clock.Clock
	wg     *sync.WaitGroup
	status int32

	err               uatomic.Error
	isDefaultUpstream bool
}

func newUpstream(pdEndpoints []string,
	securityConfig *security.Credential,
) *Upstream {
	return &Upstream{
		PdEndpoints:    pdEndpoints,
		SecurityConfig: securityConfig,
		status:         uninit,
		wg:             new(sync.WaitGroup),
		clock:          clock.New(),
	}
}

// CreateTiStore creates a tikv storage client
// Note: It will return a same storage if the urls connect to a same pd cluster,
// so must be careful when you call storage.Close().
func CreateTiStore(urls string, credential *security.Credential) (tidbkv.Storage, error) {
	urlv, err := common.NewURLsValue(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tiPath := fmt.Sprintf("tikv://%s?disableGC=true", urlv.HostString())
	securityCfg := tikvconfig.Security{
		ClusterSSLCA:    credential.CAPath,
		ClusterSSLCert:  credential.CertPath,
		ClusterSSLKey:   credential.KeyPath,
		ClusterVerifyCN: credential.CertAllowedCN,
	}
	d := driver.TiKVDriver{}
	// we should use OpenWithOptions to open a storage to avoid modifying tidb's GlobalConfig
	// so that we can create different storage in TiCDC by different urls and credential
	tiStore, err := d.OpenWithOptions(tiPath, driver.WithSecurity(securityCfg))
	if err != nil {
		return nil, errors.WrapError(errors.ErrNewStore, err)
	}
	return tiStore, nil
}

// init initializes the upstream
func initUpstream(ctx context.Context, up *Upstream) error {
	ctx, up.cancel = context.WithCancel(ctx)
	grpcTLSOption, err := up.SecurityConfig.ToGRPCDialOption()
	if err != nil {
		up.err.Store(err)
		return errors.Trace(err)
	}
	// init the tikv client tls global config
	initGlobalConfig(up.SecurityConfig)
	// default upstream always use the pdClient pass from cdc server
	if !up.isDefaultUpstream {
		up.PDClient, err = pd.NewClientWithContext(
			ctx, up.PdEndpoints, up.SecurityConfig.PDSecurityOption(),
			// the default `timeout` is 3s, maybe too small if the pd is busy,
			// set to 10s to avoid frequent timeout.
			pd.WithCustomTimeoutOption(10*time.Second),
			pd.WithGRPCDialOptions(
				grpcTLSOption,
				grpc.WithConnectParams(grpc.ConnectParams{
					Backoff: backoff.Config{
						BaseDelay:  time.Second,
						Multiplier: 1.1,
						Jitter:     0.1,
						MaxDelay:   3 * time.Second,
					},
					MinConnectTimeout: 3 * time.Second,
				}),
			))
		if err != nil {
			up.err.Store(err)
			return errors.Trace(err)
		}

		etcdCli, err := CreateRawEtcdClient(up.SecurityConfig, grpcTLSOption, up.PdEndpoints...)
		if err != nil {
			return errors.Trace(err)
		}
		up.etcdCli = etcdCli
	}
	clusterID := up.PDClient.GetClusterID(ctx)
	if up.ID != 0 && up.ID != clusterID {
		err := fmt.Errorf("upstream id missmatch expected %d, actual: %d",
			up.ID, clusterID)
		up.err.Store(err)
		return errors.Trace(err)
	}
	up.ID = clusterID

	up.KVStorage, err = CreateTiStore(strings.Join(up.PdEndpoints, ","), up.SecurityConfig)
	if err != nil {
		up.err.Store(err)
		return errors.Trace(err)
	}

	up.RegionCache = tikv.NewRegionCache(up.PDClient)

	up.PDClock, err = pdutil.NewClock(ctx, up.PDClient)
	if err != nil {
		up.err.Store(err)
		return errors.Trace(err)
	}

	// Update meta-region label to ensure that meta region isolated from data regions.
	pc, err := pdutil.NewPDAPIClient(up.PDClient, up.SecurityConfig)
	if err != nil {
		log.Error("create pd api client failed", zap.Error(err))
		return errors.Trace(err)
	}
	defer pc.Close()

	err = pc.UpdateMetaLabel(ctx)
	if err != nil {
		log.Warn("Fail to verify region label rule",
			zap.Error(err),
			zap.Uint64("upstreamID", up.ID),
			zap.Strings("upstreamEndpoints", up.PdEndpoints))
	}

	up.wg.Add(1)
	go func() {
		defer up.wg.Done()
		up.PDClock.Run(ctx)
	}()

	log.Info("upstream initialize successfully", zap.Uint64("upstreamID", up.ID))
	atomic.StoreInt32(&up.status, normal)
	return nil
}

// initGlobalConfig initializes the global config for tikv client tls.
// region cache health check will use the global config.
// TODO: remove this function after tikv client tls is refactored.
func initGlobalConfig(secCfg *security.Credential) {
	if secCfg.CAPath != "" || secCfg.CertPath != "" || secCfg.KeyPath != "" {
		conf := tikvconfig.GetGlobalConfig()
		conf.Security.ClusterSSLCA = secCfg.CAPath
		conf.Security.ClusterSSLCert = secCfg.CertPath
		conf.Security.ClusterSSLKey = secCfg.KeyPath
		conf.Security.ClusterVerifyCN = secCfg.CertAllowedCN
		tikvconfig.StoreGlobalConfig(conf)
	}
}

// Close all resources.
func (up *Upstream) Close() {
	up.mu.Lock()
	defer up.mu.Unlock()
	up.cancel()
	if atomic.LoadInt32(&up.status) == closed ||
		atomic.LoadInt32(&up.status) == closing {
		return
	}
	atomic.StoreInt32(&up.status, closing)

	// should never close default upstream's pdClient and etcdClient here
	// because it's shared in the cdc server
	if !up.isDefaultUpstream {
		if up.PDClient != nil {
			up.PDClient.Close()
		}
		if up.etcdCli != nil {
			err := up.etcdCli.Close()
			if err != nil {
				log.Warn("etcd client close failed", zap.Error(err))
			}
		}
	}

	if up.KVStorage != nil {
		err := up.KVStorage.Close()
		if err != nil {
			log.Warn("kv store close failed", zap.Error(err))
		}
	}

	if up.RegionCache != nil {
		up.RegionCache.Close()
	}
	if up.PDClock != nil {
		up.PDClock.Stop()
	}
	if up.session != nil {
		err := up.session.Close()
		if err != nil {
			log.Warn("etcd session close failed", zap.Error(err))
		}
	}

	up.wg.Wait()
	atomic.StoreInt32(&up.status, closed)
	log.Info("upstream closed", zap.Uint64("upstreamID", up.ID))
}

// Error returns the error during init this stream
func (up *Upstream) Error() error {
	return up.err.Load()
}

// IsNormal returns true if the upstream is normal.
func (up *Upstream) IsNormal() bool {
	return atomic.LoadInt32(&up.status) == normal && up.err.Load() == nil
}

// IsClosed returns true if the upstream is closed.
func (up *Upstream) IsClosed() bool {
	return atomic.LoadInt32(&up.status) == closed
}

// resetIdleTime set the upstream idle time to true
func (up *Upstream) resetIdleTime() {
	up.mu.Lock()
	defer up.mu.Unlock()

	if !up.idleTime.IsZero() {
		log.Info("upstream idle time is set to 0",
			zap.Uint64("id", up.ID))
		up.idleTime = time.Time{}
	}
}

// trySetIdleTime set the upstream idle time if it's not zero
func (up *Upstream) trySetIdleTime() {
	up.mu.Lock()
	defer up.mu.Unlock()
	// reset idleTime
	if up.idleTime.IsZero() {
		log.Info("upstream idle time is set to current time",
			zap.Uint64("id", up.ID))
		up.idleTime = up.clock.Now()
	}
}

// shouldClose returns true if
// this upstream idleTime reaches maxIdleDuration.
func (up *Upstream) shouldClose() bool {
	// default upstream should never be closed.
	if up.isDefaultUpstream {
		return false
	}

	if !up.idleTime.IsZero() &&
		up.clock.Since(up.idleTime) >= maxIdleDuration {
		return true
	}

	return false
}
