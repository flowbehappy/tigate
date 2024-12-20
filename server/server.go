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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatchermanager"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcherorchestrator"
	"github.com/pingcap/ticdc/downstreamadapter/eventcollector"
	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/logservice/txnutil"
	"github.com/pingcap/ticdc/maintainer"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	appctx "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/eventservice"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/tcpserver"
	"github.com/tikv/client-go/v2/tikv"
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

	dispatcherOrchestrator *dispatcherorchestrator.DispatcherOrchestrator

	// session keeps alive between the server and etcd
	session *concurrency.Session

	EtcdClient etcd.CDCEtcdClient

	KVStorage   kv.Storage
	RegionCache *tikv.RegionCache
	PDClock     pdutil.Clock

	tcpServer  tcpserver.TCPServer
	subModules []common.SubModule
}

// New returns a new Server instance
func New(conf *config.ServerConfig, pdEndpoints []string) (node.Server, error) {
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

	messageCenter := messaging.NewMessageCenter(ctx, c.info.ID, c.info.Epoch, config.NewDefaultMessageCenterConfig())
	appcontext.SetID(c.info.ID.String())
	appcontext.SetService(appcontext.MessageCenter, messageCenter)

	appcontext.SetService(appcontext.EventCollector, eventcollector.New(ctx, 100*1024*1024*1024, c.info.ID)) // 100GB for demo
	appcontext.SetService(appcontext.HeartbeatCollector, dispatchermanager.NewHeartBeatCollector(c.info.ID))
	c.dispatcherOrchestrator = dispatcherorchestrator.New()

	nodeManager := watcher.NewNodeManager(c.session, c.EtcdClient)
	nodeManager.RegisterNodeChangeHandler(
		appcontext.MessageCenter,
		appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).OnNodeChanges)
	subscriptionClient := logpuller.NewSubscriptionClient(
		&logpuller.SubscriptionClientConfig{
			RegionRequestWorkerPerStore: 16,
		}, c.pdClient, c.RegionCache, c.PDClock,
		txnutil.NewLockerResolver(c.KVStorage.(tikv.Storage)), &security.Credential{},
	)
	conf := config.GetGlobalServerConfig()
	schemaStore := schemastore.New(ctx, conf.DataDir, subscriptionClient, c.pdClient, c.PDClock, c.KVStorage)
	eventStore := eventstore.New(ctx, conf.DataDir, subscriptionClient, c.PDClock)
	eventService := eventservice.New(eventStore, schemaStore)
	c.subModules = []common.SubModule{
		nodeManager,
		subscriptionClient,
		schemaStore,
		NewElector(c),
		NewHttpServer(c, c.tcpServer.HTTP1Listener()),
		NewGrpcServer(c.tcpServer.GrpcListener()),
		maintainer.NewMaintainerManager(c.info, conf.Debug.Scheduler,
			c.pdAPIClient, c.pdClient, c.RegionCache),
		eventStore,
		eventService,
	}
	// register it into global var
	for _, subModule := range c.subModules {
		appctx.SetService(subModule.Name(), subModule)
	}
	// start tcp server
	go func() {
		err := c.tcpServer.Run(ctx)
		if err != nil {
			log.Error("tcp server exist", zap.Error(cerror.Trace(err)))
		}
	}()
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
		log.Info("coordinator closed", zap.String("captureID", string(c.info.ID)))
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
	if err := c.EtcdClient.DeleteCaptureInfo(timeoutCtx, model.CaptureID(c.info.ID)); err != nil {
		log.Warn("failed to delete server info when server exited",
			zap.String("captureID", string(c.info.ID)),
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
				ID:            node.ID(captureInfo.ID),
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
