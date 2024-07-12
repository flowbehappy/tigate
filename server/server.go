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
	"strings"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/maintainer"
	"github.com/pingcap/tiflow/pkg/tcpserver"

	"github.com/flowbehappy/tigate/coordinator"
	appctx "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/tikv/client-go/v2/tikv"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/etcd"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	cleanMetaDuration = 10 * time.Second
)

type serverImpl struct {
	captureMu sync.Mutex
	info      *model.CaptureInfo
	serverID  messaging.ServerId

	liveness model.Liveness

	pdClient      pd.Client
	pdEndpoints   []string
	coordinatorMu sync.Mutex
	coordinator   coordinator.Coordinator
	messageCenter messaging.MessageCenter

	// session keeps alive between the server and etcd
	session *concurrency.Session

	EtcdClient etcd.CDCEtcdClient

	KVStorage   kv.Storage
	RegionCache *tikv.RegionCache
	PDClock     pdutil.Clock

	cancel context.CancelFunc

	tcpServer  tcpserver.TCPServer
	subModules []SubModule
}

// NewServer returns a new Server instance
func NewServer(pdEndpoints []string) (appctx.Server, error) {
	conf := config.GetGlobalServerConfig()

	// This is to make communication between nodes possible.
	// In other words, the nodes have to trust each other.
	if len(conf.Security.CertAllowedCN) != 0 {
		err := conf.Security.AddSelfCommonName()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// tcpServer is the unified frontend of the CDC server that serves
	// both RESTful APIs and gRPC APIs.
	// Note that we pass the TLS config to the tcpServer, so there is no need to
	// configure TLS elsewhere.
	tcpServer, err := tcpserver.NewTCPServer(conf.Addr, conf.Security)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s := &serverImpl{
		pdEndpoints: pdEndpoints,
		tcpServer:   tcpServer,
	}

	log.Info("CDC server created",
		zap.Strings("pd", pdEndpoints), zap.Stringer("config", conf))
	return s, nil
}

// initialize the server before run it.
func (c *serverImpl) initialize(ctx context.Context) error {
	if err := c.prepare(ctx); err != nil {
		return errors.Trace(err)
	}
	c.subModules = []SubModule{
		NewCaptureManager(c.session, c.EtcdClient, c.messageCenter),
		NewElector(c),
		NewHttpServer(c, c.tcpServer.HTTP1Listener()),
		NewGrpcServer(c.tcpServer.GrpcListener(), c.messageCenter),
		maintainer.NewMaintainerManager(c.messageCenter, c.serverID),
	}
	// register it into global var
	for _, subModule := range c.subModules {
		appctx.SetService(subModule.Name(), subModule)
	}
	log.Info("server initialized", zap.Any("server", c.info))
	return nil
}

// Run runs the server
func (c *serverImpl) Run(stdCtx context.Context) error {
	err := c.initialize(stdCtx)
	if err != nil {
		log.Error("init server failed", zap.Error(err))
		return errors.Trace(err)
	}
	defer func() {
		c.Close(stdCtx)
	}()

	g, stdCtx := errgroup.WithContext(stdCtx)
	// start tcp server
	g.Go(func() error {
		return c.tcpServer.Run(stdCtx)
	})
	// start all submodules
	for _, sub := range c.subModules {
		func(m SubModule) {
			g.Go(func() error {
				log.Info("starting sub module", zap.String("module", m.Name()))
				return m.Run(stdCtx)
			})
		}(sub)
	}
	return errors.Trace(g.Wait())
}

// SelfCaptureInfo gets the server info
func (c *serverImpl) SelfInfo() (*model.CaptureInfo, error) {
	// when c.reset has not been called yet, c.info is nil.
	if c.info != nil {
		return c.info, nil
	}
	return nil, cerror.ErrCaptureNotInitialized.GenWithStackByArgs()
}

func (c *serverImpl) setCoordinator(co coordinator.Coordinator) {
	c.coordinatorMu.Lock()
	defer c.coordinatorMu.Unlock()
	c.coordinator = co
}

// GetCoordinator returns coordinator if it is the coordinator.
func (c *serverImpl) GetCoordinator() (coordinator.Coordinator, error) {
	c.coordinatorMu.Lock()
	defer c.coordinatorMu.Unlock()
	if c.coordinator == nil {
		return nil, cerror.ErrNotOwner.GenWithStackByArgs()
	}
	return c.coordinator, nil
}

// Close closes the server by deregister it from etcd,
// it also closes the coordinator and processorManager
// Note: this function should be reentrant
func (c *serverImpl) Close(ctx context.Context) {
	defer c.cancel()
	// Safety: Here we mainly want to stop the coordinator
	// and ignore it if the coordinator does not exist or is not set.
	o, _ := c.GetCoordinator()
	if o != nil {
		o.AsyncStop()
		log.Info("coordinator closed", zap.String("captureID", c.info.ID))
	}

	for _, subModule := range c.subModules {
		if err := subModule.Close(ctx); err != nil {
			log.Warn("failed to close sub module",
				zap.String("module", subModule.Name()),
				zap.Error(err))
		}
	}

	// delete server info from etcd
	timeoutCtx, cancel := context.WithTimeout(context.Background(), cleanMetaDuration)
	if err := c.EtcdClient.DeleteCaptureInfo(timeoutCtx, c.info.ID); err != nil {
		log.Warn("failed to delete server info when server exited",
			zap.String("captureID", c.info.ID),
			zap.Error(err))
	}
	cancel()
}

// Liveness returns liveness of the server.
func (c *serverImpl) Liveness() model.Liveness {
	return c.liveness.Load()
}

// IsCoordinator returns whether the server is an coordinator
func (c *serverImpl) IsCoordinator() bool {
	c.coordinatorMu.Lock()
	defer c.coordinatorMu.Unlock()
	return c.coordinator != nil
}

func (c *serverImpl) GetPdClient() pd.Client {
	return c.pdClient
}

// GetCoordinatorInfo return the controller server info of current TiCDC cluster
func (c *serverImpl) GetCoordinatorInfo(ctx context.Context) (*model.CaptureInfo, error) {
	_, captureInfos, err := c.EtcdClient.GetCaptures(ctx)
	if err != nil {
		return nil, err
	}

	coordinatorID, err := c.EtcdClient.GetOwnerID(ctx)
	if err != nil {
		return nil, err
	}

	for _, captureInfo := range captureInfos {
		if captureInfo.ID == coordinatorID {
			return captureInfo, nil
		}
	}
	return nil, cerror.ErrOwnerNotFound.FastGenByArgs()
}

func isErrCompacted(err error) bool {
	return strings.Contains(err.Error(), "required revision has been compacted")
}

func (c *serverImpl) GetEtcdClient() etcd.CDCEtcdClient {
	return c.EtcdClient
}
