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
	"github.com/flowbehappy/tigate/pkg/node"
	"strings"
	"sync"
	"time"

	dispatchermanagermanager "github.com/flowbehappy/tigate/downstreamadapter/dispathermanagermanager"
	"github.com/flowbehappy/tigate/logservice/eventstore"
	"github.com/flowbehappy/tigate/logservice/schemastore"
	"github.com/flowbehappy/tigate/maintainer"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/eventservice"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/pingcap/tiflow/pkg/tcpserver"

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

type server struct {
	captureMu sync.Mutex
	info      *node.Info

	liveness model.Liveness

	pdClient      pd.Client
	pdAPIClient   pdutil.PDAPIClient
	pdEndpoints   []string
	coordinatorMu sync.Mutex
	coordinator   node.Coordinator

	dispatcherManagerManager *dispatchermanagermanager.DispatcherManagerManager

	// session keeps alive between the server and etcd
	session *concurrency.Session

	EtcdClient etcd.CDCEtcdClient

	KVStorage   kv.Storage
	RegionCache *tikv.RegionCache
	PDClock     pdutil.Clock

	tcpServer  tcpserver.TCPServer
	subModules []common.SubModule
}

// NewServer returns a new Server instance
func NewServer(conf *config.ServerConfig, pdEndpoints []string) (node.Server, error) {
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

	s := &server{
		pdEndpoints: pdEndpoints,
		tcpServer:   tcpServer,
	}
	return s, nil
}

// initialize the server before run it.
func (c *server) initialize(ctx context.Context) error {
	if err := c.prepare(ctx); err != nil {
		log.Error("server prepare failed", zap.Any("server", c.info), zap.Error(err))
		return errors.Trace(err)
	}
	conf := config.GetGlobalServerConfig()
	nodeManager := watcher.NewNodeManager(c.session, c.EtcdClient)
	nodeManager.RegisterNodeChangeHandler(
		appcontext.MessageCenter,
		appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).OnNodeChanges)

	schemaStore := schemastore.NewSchemaStore(ctx, conf.DataDir, c.pdClient, c.RegionCache, c.PDClock, c.KVStorage)

	c.subModules = []common.SubModule{
		nodeManager,
		schemaStore,
		NewElector(c),
		NewHttpServer(c, c.tcpServer.HTTP1Listener()),
		NewGrpcServer(c.tcpServer.GrpcListener()),
		maintainer.NewMaintainerManager(c.info, c.pdAPIClient, c.RegionCache),
		eventstore.NewEventStore(ctx, conf.DataDir, c.pdClient, c.RegionCache, c.PDClock, c.KVStorage, schemaStore),
	}
	// register it into global var
	for _, subModule := range c.subModules {
		appctx.SetService(subModule.Name(), subModule)
	}

	// initialize eventService, it relies on eventStore, so we need to initialize it after eventStore
	eventService := eventservice.NewEventService()
	c.subModules = append(c.subModules, eventService)
	appctx.SetService(eventService.Name(), eventService)

	log.Info("server initialized", zap.Any("server", c.info))
	return nil
}

// Run runs the server
func (c *server) Run(ctx context.Context) error {
	err := c.initialize(ctx)
	if err != nil {
		log.Error("init server failed", zap.Error(err))
		return errors.Trace(err)
	}
	defer func() {
		c.Close(ctx)
	}()

	g, ctx := errgroup.WithContext(ctx)
	// start tcp server
	g.Go(func() error {
		return c.tcpServer.Run(ctx)
	})
	// start all submodules
	for _, sub := range c.subModules {
		func(m common.SubModule) {
			g.Go(func() error {
				log.Info("starting sub module", zap.String("module", m.Name()))
				return m.Run(ctx)
			})
		}(sub)
	}
	// register server to etcd after we started all modules
	err = c.registerNodeToEtcd(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(g.Wait())
}

// SelfInfo gets the server info
func (c *server) SelfInfo() (*node.Info, error) {
	// when c.reset has not been called yet, c.info is nil.
	if c.info != nil {
		return c.info, nil
	}
	return nil, cerror.ErrCaptureNotInitialized.GenWithStackByArgs()
}

func (c *server) setCoordinator(co node.Coordinator) {
	c.coordinatorMu.Lock()
	defer c.coordinatorMu.Unlock()
	c.coordinator = co
}

// GetCoordinator returns coordinator if it is the coordinator.
func (c *server) GetCoordinator() (node.Coordinator, error) {
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
func (c *server) Close(ctx context.Context) {
	// Safety: Here we mainly want to stop the coordinator
	// and ignore it if the coordinator does not exist or is not set.
	o, _ := c.GetCoordinator()
	if o != nil {
		o.AsyncStop()
		log.Info("coordinator closed", zap.String("captureID", c.info.ID))
	}

	for _, subModule := range c.subModules {
		if err := subModule.Close(ctx); err != nil {
			log.Warn("failed to close sub watcher",
				zap.String("watcher", subModule.Name()),
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
func (c *server) Liveness() model.Liveness {
	return c.liveness.Load()
}

// IsCoordinator returns whether the server is an coordinator
func (c *server) IsCoordinator() bool {
	c.coordinatorMu.Lock()
	defer c.coordinatorMu.Unlock()
	return c.coordinator != nil
}

func (c *server) GetPdClient() pd.Client {
	return c.pdClient
}

// GetCoordinatorInfo return the controller server info of current TiCDC cluster
func (c *server) GetCoordinatorInfo(ctx context.Context) (*node.Info, error) {
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
			res := &node.Info{
				ID:            captureInfo.ID,
				AdvertiseAddr: captureInfo.AdvertiseAddr,

				Version:        captureInfo.Version,
				DeployPath:     captureInfo.DeployPath,
				StartTimestamp: captureInfo.StartTimestamp,

				// Epoch is now not used in TiCDC, so we just set it to 0.
				Epoch: 0,
			}
			return res, nil
		}
	}
	return nil, cerror.ErrOwnerNotFound.FastGenByArgs()
}

func isErrCompacted(err error) bool {
	return strings.Contains(err.Error(), "required revision has been compacted")
}

func (c *server) GetEtcdClient() etcd.CDCEtcdClient {
	return c.EtcdClient
}
