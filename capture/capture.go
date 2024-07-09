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

package capture

import (
	"context"
	"os"
	"strings"
	"sync"
	"time"

	appctx "github.com/flowbehappy/tigate/common/context"
	"github.com/flowbehappy/tigate/coordinator"
	"github.com/flowbehappy/tigate/version"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/tikv/client-go/v2/tikv"

	"github.com/google/uuid"
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

// Capture represents a Capture server, it monitors the changefeed
// information in etcd and schedules Task on it.
type Capture interface {
	Run(ctx context.Context) error
	Close()

	SelfCaptureInfo() (*model.CaptureInfo, error)
	Liveness() model.Liveness

	GetCoordinator() (coordinator.Coordinator, error)
	IsCoordinator() bool

	// GetCoordinatorInfo returns the coordinator capture， it will be used when forward api request
	GetCoordinatorInfo(ctx context.Context) (*model.CaptureInfo, error)

	GetPdClient() pd.Client
	GetEtcdClient() etcd.CDCEtcdClient
}

type captureImpl struct {
	// captureMu is used to protect the capture info and processorManager.
	captureMu sync.Mutex
	info      *model.CaptureInfo
	liveness  model.Liveness
	config    *config.ServerConfig

	pdClient      pd.Client
	pdEndpoints   []string
	coordinatorMu sync.Mutex
	coordinator   coordinator.Coordinator

	// session keeps alive between the capture and etcd
	session *concurrency.Session

	EtcdClient etcd.CDCEtcdClient

	KVStorage   kv.Storage
	RegionCache *tikv.RegionCache
	PDClock     pdutil.Clock

	cancel context.CancelFunc

	subModules []SubModule
}

// NewCapture returns a new Capture instance
func NewCapture(pdEndpoints []string,
	etcdClient etcd.CDCEtcdClient,
	pdClient pd.Client,
) Capture {
	return &captureImpl{
		config:      config.GetGlobalServerConfig(),
		liveness:    model.LivenessCaptureAlive,
		EtcdClient:  etcdClient,
		cancel:      func() {},
		pdEndpoints: pdEndpoints,
		info:        &model.CaptureInfo{},
		pdClient:    pdClient,
	}
}

// initialize the capture before run it.
func (c *captureImpl) initialize(ctx context.Context) error {
	session, err := c.newEtcdSession(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	c.captureMu.Lock()
	defer c.captureMu.Unlock()
	deployPath, err := os.Executable()
	if err != nil {
		deployPath = ""
	}

	c.info = &model.CaptureInfo{
		ID:             uuid.New().String(),
		AdvertiseAddr:  c.config.AdvertiseAddr,
		Version:        version.ReleaseVersion,
		GitHash:        version.GitHash,
		DeployPath:     deployPath,
		StartTimestamp: time.Now().Unix(),
	}
	c.session = session
	c.subModules = []SubModule{
		NewCaptureManager(c.session, c.EtcdClient),
		NewElector(c),
	}
	// register it into global var
	for _, subModule := range c.subModules {
		appctx.SetService(subModule.Name(), subModule)
	}

	log.Info("capture initialized", zap.Any("capture", c.info))
	return nil
}

// Run runs the capture
func (c *captureImpl) Run(stdCtx context.Context) error {
	err := c.initialize(stdCtx)
	if err != nil {
		log.Error("init capture failed", zap.Error(err))
		return errors.Trace(err)
	}

	err = c.registerCaptureToEtcd(stdCtx)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), cleanMetaDuration)
		if err := c.EtcdClient.DeleteCaptureInfo(timeoutCtx, c.info.ID); err != nil {
			log.Warn("failed to delete capture info when capture exited",
				zap.String("captureID", c.info.ID),
				zap.Error(err))
		}
		cancel()
	}()

	defer func() {
		c.Close()
	}()

	g, stdCtx := errgroup.WithContext(stdCtx)
	for _, sub := range c.subModules {
		func(m SubModule) {
			g.Go(func() error {
				return m.Run(stdCtx)
			})
		}(sub)
	}
	return errors.Trace(g.Wait())
}

// SelfCaptureInfo gets the capture info
func (c *captureImpl) SelfCaptureInfo() (*model.CaptureInfo, error) {
	c.captureMu.Lock()
	defer c.captureMu.Unlock()
	// when c.reset has not been called yet, c.info is nil.
	if c.info != nil {
		return c.info, nil
	}
	return nil, cerror.ErrCaptureNotInitialized.GenWithStackByArgs()
}

func (c *captureImpl) setCoordinator(co coordinator.Coordinator) {
	c.coordinatorMu.Lock()
	defer c.coordinatorMu.Unlock()
	c.coordinator = co
}

// GetCoordinator returns coordinator if it is the coordinator.
func (c *captureImpl) GetCoordinator() (coordinator.Coordinator, error) {
	c.coordinatorMu.Lock()
	defer c.coordinatorMu.Unlock()
	if c.coordinator == nil {
		return nil, cerror.ErrNotOwner.GenWithStackByArgs()
	}
	return c.coordinator, nil
}

// registerCaptureToEtcd the capture by put the capture's information in etcd
func (c *captureImpl) registerCaptureToEtcd(ctx context.Context) error {
	err := c.EtcdClient.PutCaptureInfo(ctx, c.info, c.session.Lease())
	if err != nil {
		return cerror.WrapError(cerror.ErrCaptureRegister, err)
	}
	return nil
}

// Close closes the capture by deregister it from etcd,
// it also closes the coordinator and processorManager
// Note: this function should be reentrant
func (c *captureImpl) Close() {
	defer c.cancel()
	// Safety: Here we mainly want to stop the coordinator
	// and ignore it if the coordinator does not exist or is not set.
	o, _ := c.GetCoordinator()
	if o != nil {
		o.AsyncStop()
		log.Info("coordinator closed", zap.String("captureID", c.info.ID))
	}

	c.captureMu.Lock()
	defer c.captureMu.Unlock()

	log.Info("message router closed", zap.String("captureID", c.info.ID))
}

// Liveness returns liveness of the capture.
func (c *captureImpl) Liveness() model.Liveness {
	return c.liveness.Load()
}

// IsCoordinator returns whether the capture is an coordinator
func (c *captureImpl) IsCoordinator() bool {
	c.coordinatorMu.Lock()
	defer c.coordinatorMu.Unlock()
	return c.coordinator != nil
}

func (c *captureImpl) GetPdClient() pd.Client {
	return c.pdClient
}

// GetCoordinatorInfo return the controller capture info of current TiCDC cluster
func (c *captureImpl) GetCoordinatorInfo(ctx context.Context) (*model.CaptureInfo, error) {
	_, captureInfos, err := c.EtcdClient.GetCaptures(ctx)
	if err != nil {
		return nil, err
	}

	ownerID, err := c.EtcdClient.GetOwnerID(ctx)
	if err != nil {
		return nil, err
	}

	for _, captureInfo := range captureInfos {
		if captureInfo.ID == ownerID {
			return captureInfo, nil
		}
	}
	return nil, cerror.ErrOwnerNotFound.FastGenByArgs()
}

func isErrCompacted(err error) bool {
	return strings.Contains(err.Error(), "required revision has been compacted")
}

func (c *captureImpl) GetEtcdClient() etcd.CDCEtcdClient {
	return c.EtcdClient
}

func (c *captureImpl) newEtcdSession(ctx context.Context) (*concurrency.Session, error) {
	lease, err := c.EtcdClient.GetEtcdClient().Grant(ctx, int64(c.config.CaptureSessionTTL))
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
