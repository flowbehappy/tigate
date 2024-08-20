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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/flowbehappy/tigate/downstreamadapter/dispatcher"
	"github.com/flowbehappy/tigate/downstreamadapter/dispatchermanager"
	dispatchermanagermanager "github.com/flowbehappy/tigate/downstreamadapter/dispathermanagermanager"
	"github.com/flowbehappy/tigate/downstreamadapter/eventcollector"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/utils/dynstream"

	"github.com/dustin/go-humanize"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/version"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	cdcconfig "github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/fsutil"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	defaultDataDir = "/tmp/cdc_data"
	// dataDirThreshold is used to warn if the free space of the specified data-dir is lower than it, unit is GB
	dataDirThreshold = 500
	// maxGcTunerMemory is used to limit the max memory usage of cdc server. if the memory is larger than it, gc tuner will be disabled
	maxGcTunerMemory = 512 * 1024 * 1024 * 1024
)

func (c *serverImpl) prepare(ctx context.Context) error {
	conf := cdcconfig.GetGlobalServerConfig()
	grpcTLSOption, err := conf.Security.ToGRPCDialOption()
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("create pd client", zap.Strings("endpoints", c.pdEndpoints))
	c.pdClient, err = pd.NewClientWithContext(
		ctx, c.pdEndpoints, conf.Security.PDSecurityOption(),
		// the default `timeout` is 3s, maybe too small if the pd is busy,
		// set to 10s to avoid frequent timeout.
		pd.WithCustomTimeoutOption(10*time.Second),
		pd.WithGRPCDialOptions(
			grpcTLSOption,
			grpc.WithBlock(),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
		),
		pd.WithForwardingOption(cdcconfig.EnablePDForwarding))
	if err != nil {
		return errors.Trace(err)
	}
	pdAPIClient, err := pdutil.NewPDAPIClient(c.pdClient, conf.Security)
	if err != nil {
		return errors.Trace(err)
	}
	defer pdAPIClient.Close()
	log.Info("create etcdCli", zap.Strings("endpoints", c.pdEndpoints))
	// we do not pass a `context` to create an etcd client,
	// to prevent it's cancelled when the server is closing.
	// For example, when the non-owner watcher goes offline,
	// it would resign the campaign key which was put by call `campaign`,
	// if this is not done due to the passed context cancelled,
	// the key will be kept for the lease TTL, which is 10 seconds,
	// then cause the new owner cannot be elected immediately after the old owner offline.
	// see https://github.com/etcd-io/etcd/blob/525d53bd41/client/v3/concurrency/election.go#L98
	etcdCli, err := etcd.CreateRawEtcdClient(conf.Security, grpcTLSOption, c.pdEndpoints...)
	if err != nil {
		return errors.Trace(err)
	}

	cdcEtcdClient, err := etcd.NewCDCEtcdClient(ctx, etcdCli, conf.ClusterID)
	if err != nil {
		return errors.Trace(err)
	}
	c.EtcdClient = cdcEtcdClient

	// Collect all endpoints from pd here to make the server more robust.
	// Because in some scenarios, the deployer may only provide one pd endpoint,
	// this will cause the TiCDC server to fail to restart when some pd watcher is down.
	allPDEndpoints, err := pdAPIClient.CollectMemberEndpoints(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	c.pdEndpoints = append(c.pdEndpoints, allPDEndpoints...)

	c.KVStorage, err = kv.CreateTiStore(strings.Join(allPDEndpoints, ","), conf.Security)
	if err != nil {
		return errors.Trace(err)
	}

	c.RegionCache = tikv.NewRegionCache(c.pdClient)
	c.PDClock, err = pdutil.NewClock(ctx, c.pdClient)
	if err != nil {
		return errors.Trace(err)
	}

	if err := c.initDir(); err != nil {
		return errors.Trace(err)
	}
	c.setMemoryLimit()

	session, err := c.newEtcdSession(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	deployPath, err := os.Executable()
	if err != nil {
		deployPath = ""
	}
	// TODO: Get id from disk after restart.
	id := messaging.NewServerId()
	c.info = &common.NodeInfo{
		ID:             id.String(),
		AdvertiseAddr:  conf.AdvertiseAddr,
		Version:        version.ReleaseVersion,
		GitHash:        version.GitHash,
		DeployPath:     deployPath,
		StartTimestamp: time.Now().Unix(),
	}
	c.serverID = id
	c.session = session

	ddlActionDynamicStream := dynstream.NewDynamicStreamDefault(&dispatcher.DDLActionsHandler{})
	appcontext.SetService(appcontext.DDLActionDynamicStream, ddlActionDynamicStream)
	appcontext.SetService(appcontext.HeartBeatResponseDynamicStream, dynstream.NewDynamicStreamDefault(&dispatchermanager.HeartBeatResponseHandler{DDLActionDynamicStream: ddlActionDynamicStream}))

	// TODO: dynamic stream start
	appcontext.SetService(appcontext.MessageCenter, messaging.NewMessageCenter(ctx, id, c.info.Epoch, config.NewDefaultMessageCenterConfig()))
	appcontext.SetService(appcontext.EventCollector, eventcollector.NewEventCollector(100*1024*1024*1024, id)) // 100GB for demo
	appcontext.SetService(appcontext.HeartbeatCollector, dispatchermanager.NewHeartBeatCollector(id))

	c.dispatcherManagerManager = dispatchermanagermanager.NewDispatcherManagerManager()
	return nil
}

func (c *serverImpl) setMemoryLimit() {
	conf := cdcconfig.GetGlobalServerConfig()
	if conf.GcTunerMemoryThreshold > maxGcTunerMemory {
		// If total memory is larger than 512GB, we will not set memory limit.
		// Because the memory limit is not accurate, and it is not necessary to set memory limit.
		log.Info("total memory is larger than 512GB, skip setting memory limit",
			zap.Uint64("bytes", conf.GcTunerMemoryThreshold),
			zap.String("memory", humanize.IBytes(conf.GcTunerMemoryThreshold)),
		)
		return
	}
	if conf.GcTunerMemoryThreshold > 0 {
		gctuner.EnableGOGCTuner.Store(true)
		gctuner.Tuning(conf.GcTunerMemoryThreshold)
		log.Info("enable gctuner, set memory limit",
			zap.Uint64("bytes", conf.GcTunerMemoryThreshold),
			zap.String("memory", humanize.IBytes(conf.GcTunerMemoryThreshold)),
		)
	}
}

func (c *serverImpl) initDir() error {
	c.setUpDir()
	conf := cdcconfig.GetGlobalServerConfig()
	// Ensure data dir exists and read-writable.
	diskInfo, err := checkDir(conf.DataDir)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info(fmt.Sprintf("%s is set as data-dir (%dGB available), sort-dir=%s. "+
		"It is recommended that the disk for data-dir at least have %dGB available space",
		conf.DataDir, diskInfo.Avail, conf.Sorter.SortDir, dataDirThreshold))

	// Ensure sorter dir exists and read-writable.
	_, err = checkDir(conf.Sorter.SortDir)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *serverImpl) setUpDir() {
	conf := cdcconfig.GetGlobalServerConfig()
	if conf.DataDir != "" {
		conf.Sorter.SortDir = filepath.Join(conf.DataDir, cdcconfig.DefaultSortDir)
		cdcconfig.StoreGlobalServerConfig(conf)
		return
	}

	conf.DataDir = defaultDataDir
	conf.Sorter.SortDir = filepath.Join(conf.DataDir, cdcconfig.DefaultSortDir)
	cdcconfig.StoreGlobalServerConfig(conf)
}

// registerNodeToEtcd the server by put the server's information in etcd
func (c *serverImpl) registerNodeToEtcd(ctx context.Context) error {
	cInfo := &model.CaptureInfo{
		ID:             c.info.ID,
		AdvertiseAddr:  c.info.AdvertiseAddr,
		Version:        c.info.Version,
		GitHash:        c.info.GitHash,
		DeployPath:     c.info.DeployPath,
		StartTimestamp: c.info.StartTimestamp,
	}
	err := c.EtcdClient.PutCaptureInfo(ctx, cInfo, c.session.Lease())
	if err != nil {
		return cerror.WrapError(cerror.ErrCaptureRegister, err)
	}
	return nil
}

func (c *serverImpl) newEtcdSession(ctx context.Context) (*concurrency.Session, error) {
	cfg := cdcconfig.GetGlobalServerConfig()
	lease, err := c.EtcdClient.GetEtcdClient().Grant(ctx, int64(cfg.CaptureSessionTTL))
	if err != nil {
		return nil, errors.Trace(err)
	}
	sess, err := concurrency.NewSession(
		c.EtcdClient.GetEtcdClient().Unwrap(), concurrency.WithLease(lease.ID))
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("create session successfully", zap.Any("session", sess))
	return sess, nil
}

func checkDir(dir string) (*fsutil.DiskInfo, error) {
	err := os.MkdirAll(dir, 0o700)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err := fsutil.IsDirReadWritable(dir); err != nil {
		return nil, errors.Trace(err)
	}
	return fsutil.GetDiskInfo(dir)
}
