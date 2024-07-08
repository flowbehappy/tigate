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

	"github.com/flowbehappy/tigate/coordinator"
	"github.com/flowbehappy/tigate/version"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tiflow/pkg/migrate"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/tikv/client-go/v2/tikv"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/util"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/mvcc"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

const (
	cleanMetaDuration = 10 * time.Second
)

// Capture represents a Capture server, it monitors the changefeed
// information in etcd and schedules Task on it.
type Capture interface {
	Run(ctx context.Context) error
	Close()
	Liveness() model.Liveness

	GetOwner() (coordinator.Coordinator, error)
	GetOwnerCaptureInfo(ctx context.Context) (*model.CaptureInfo, error)
	IsOwner() bool

	Info() (model.CaptureInfo, error)

	GetPdClient() pd.Client
	GetEtcdClient() etcd.CDCEtcdClient

	GetUpstreamInfo(context.Context, model.UpstreamID, string) (*model.UpstreamInfo, error)
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
	session  *concurrency.Session
	election election

	EtcdClient etcd.CDCEtcdClient

	KVStorage   kv.Storage
	RegionCache *tikv.RegionCache
	PDClock     pdutil.Clock

	cancel context.CancelFunc
}

func (c *captureImpl) GetUpstreamInfo(ctx context.Context, id model.UpstreamID, namespace string) (*model.UpstreamInfo, error) {
	return c.GetEtcdClient().GetUpstreamInfo(ctx, id, namespace)
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

func (c *captureImpl) GetEtcdClient() etcd.CDCEtcdClient {
	return c.EtcdClient
}

// reset the capture before run it.
func (c *captureImpl) reset(ctx context.Context) error {
	lease, err := c.EtcdClient.GetEtcdClient().Grant(ctx, int64(c.config.CaptureSessionTTL))
	if err != nil {
		return errors.Trace(err)
	}
	sess, err := concurrency.NewSession(
		c.EtcdClient.GetEtcdClient().Unwrap(), concurrency.WithLease(lease.ID))
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("reset session successfully", zap.Any("session", sess))

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

	if c.session != nil {
		// It can't be handled even after it fails, so we ignore it.
		_ = c.session.Close()
	}
	c.session = sess
	c.election = newElection(sess, etcd.CaptureOwnerKey(c.EtcdClient.GetClusterID()))

	log.Info("capture initialized", zap.Any("capture", c.info))
	return nil
}

// Run runs the capture
func (c *captureImpl) Run(ctx context.Context) error {
	defer log.Info("the capture routine has exited")
	// Limit the frequency of reset capture to avoid frequent recreating of resources
	rl := rate.NewLimiter(0.05, 2)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		ctx, cancel := context.WithCancel(ctx)
		c.cancel = cancel
		err := rl.Wait(ctx)
		if err != nil {
			if errors.Cause(err) == context.Canceled {
				return nil
			}
			return errors.Trace(err)
		}
		err = c.run(ctx)
		// if capture suicided, reset the capture and run again.
		// if the canceled error throw, there are two possible scenarios:
		//   1. the internal context canceled, it means some error happened in
		//      the internal, and the routine is exited, we should restart
		//      the capture.
		//   2. the parent context canceled, it means that the caller of
		//      the capture hope the capture to exit, and this loop will return
		//      in the above `select` block.
		// if there are some **internal** context deadline exceeded (IO/network
		// timeout), reset the capture and run again.
		//
		// TODO: make sure the internal cancel should return the real error
		//       instead of context.Canceled.
		if cerror.ErrCaptureSuicide.Equal(err) ||
			context.Canceled == errors.Cause(err) ||
			context.DeadlineExceeded == errors.Cause(err) {
			log.Info("capture recovered", zap.String("captureID", c.info.ID))
			continue
		}
		return errors.Trace(err)
	}
}

func (c *captureImpl) run(stdCtx context.Context) error {
	err := c.reset(stdCtx)
	if err != nil {
		log.Error("reset capture failed", zap.Error(err))
		return errors.Trace(err)
	}

	err = c.register(stdCtx)
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

	g.Go(func() error {
		// when the campaignOwner returns an error, it means that the coordinator throws
		// an unrecoverable serious errors (recoverable errors are intercepted in the coordinator tick)
		// so we should restart the capture.
		err := c.campaignOwner(stdCtx)
		if err != nil || c.liveness.Load() != model.LivenessCaptureStopping {
			log.Warn("campaign coordinator routine exited, restart the capture",
				zap.String("captureID", c.info.ID), zap.Error(err))
			// Throw ErrCaptureSuicide to restart capture.
			return cerror.ErrCaptureSuicide.FastGenByArgs()
		}
		return nil
	})
	return errors.Trace(g.Wait())
}

// Info gets the capture info
func (c *captureImpl) Info() (model.CaptureInfo, error) {
	c.captureMu.Lock()
	defer c.captureMu.Unlock()
	// when c.reset has not been called yet, c.info is nil.
	if c.info != nil {
		return *c.info, nil
	}
	return model.CaptureInfo{}, cerror.ErrCaptureNotInitialized.GenWithStackByArgs()
}

func (c *captureImpl) campaignOwner(ctx context.Context) error {
	// In most failure cases, we don't return error directly, just run another
	// campaign loop. We treat campaign loop as a special background routine.
	ownerFlushInterval := time.Duration(c.config.OwnerFlushInterval)
	failpoint.Inject("ownerFlushIntervalInject", func(val failpoint.Value) {
		ownerFlushInterval = time.Millisecond * time.Duration(val.(int))
	})
	// Limit the frequency of elections to avoid putting too much pressure on the etcd server
	rl := rate.NewLimiter(rate.Every(time.Second), 1 /* burst */)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		err := rl.Wait(ctx)
		if err != nil {
			if errors.Cause(err) == context.Canceled {
				return nil
			}
			return errors.Trace(err)
		}
		// Before campaign check liveness
		if c.liveness.Load() == model.LivenessCaptureStopping {
			log.Info("do not campaign coordinator, liveness is stopping",
				zap.String("captureID", c.info.ID))
			return nil
		}
		// Campaign to be the coordinator, it blocks until it been elected.
		if err := c.campaign(ctx); err != nil {
			rootErr := errors.Cause(err)
			if rootErr == context.Canceled {
				return nil
			} else if rootErr == mvcc.ErrCompacted || isErrCompacted(rootErr) {
				log.Warn("campaign coordinator failed due to etcd revision "+
					"has been compacted, retry later", zap.Error(err))
				continue
			}
			log.Warn("campaign coordinator failed",
				zap.String("captureID", c.info.ID), zap.Error(err))
			return cerror.ErrCaptureSuicide.GenWithStackByArgs()
		}
		// After campaign check liveness again.
		// It is possible it becomes the coordinator right after receiving SIGTERM.
		if c.liveness.Load() == model.LivenessCaptureStopping {
			// If the capture is stopping, resign actively.
			log.Info("resign coordinator actively, liveness is stopping")
			if resignErr := c.resign(ctx); resignErr != nil {
				log.Warn("resign coordinator actively failed",
					zap.String("captureID", c.info.ID), zap.Error(resignErr))
				return errors.Trace(err)
			}
			return nil
		}

		ownerRev, err := c.EtcdClient.GetOwnerRevision(ctx, c.info.ID)
		if err != nil {
			if errors.Cause(err) == context.Canceled {
				return nil
			}
			return errors.Trace(err)
		}

		log.Info("campaign coordinator successfully",
			zap.String("captureID", c.info.ID),
			zap.Int64("Rev", ownerRev))

		co := coordinator.NewCoordinator(c.info)
		c.setOwner(co)
		err = c.runEtcdWorker(ctx, co.(orchestrator.Reactor),
			orchestrator.NewGlobalState(c.EtcdClient.GetClusterID(), c.config.CaptureSessionTTL),
			ownerFlushInterval, util.RoleOwner.String())
		c.coordinator.AsyncStop()
		c.setOwner(nil)

		if !cerror.ErrNotOwner.Equal(err) {
			// if coordinator exits, resign the coordinator key,
			// use a new context to prevent the context from being cancelled.
			resignCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if resignErr := c.resign(resignCtx); resignErr != nil {
				if errors.Cause(resignErr) != context.DeadlineExceeded {
					log.Info("coordinator resign failed", zap.String("captureID", c.info.ID),
						zap.Error(resignErr), zap.Int64("ownerRev", ownerRev))
					cancel()
					return errors.Trace(resignErr)
				}

				log.Warn("coordinator resign timeout", zap.String("captureID", c.info.ID),
					zap.Error(resignErr), zap.Int64("ownerRev", ownerRev))
			}
			cancel()
		}

		log.Info("coordinator resigned successfully",
			zap.String("captureID", c.info.ID), zap.Int64("ownerRev", ownerRev))
		if err != nil {
			log.Warn("run coordinator exited with error",
				zap.String("captureID", c.info.ID), zap.Int64("ownerRev", ownerRev),
				zap.Error(err))
			// for errors, return error and let capture exits or restart
			return errors.Trace(err)
		}
		// if coordinator exits normally, continue the campaign loop and try to election coordinator again
		log.Info("run coordinator exited normally",
			zap.String("captureID", c.info.ID), zap.Int64("ownerRev", ownerRev))
	}
}

func (c *captureImpl) runEtcdWorker(
	ctx context.Context,
	reactor orchestrator.Reactor,
	reactorState *orchestrator.GlobalReactorState,
	timerInterval time.Duration,
	role string,
) error {
	reactorState.Role = role
	baseKey := etcd.BaseKey(c.EtcdClient.GetClusterID())
	if role == util.RoleProcessor.String() {
		baseKey = baseKey + "/capture"
	}
	etcdWorker, err := orchestrator.NewEtcdWorker(c.EtcdClient,
		etcd.BaseKey(c.EtcdClient.GetClusterID()), reactor, reactorState, &migrate.NoOpMigrator{})
	if err != nil {
		return errors.Trace(err)
	}
	if err := etcdWorker.Run(ctx, c.session, timerInterval, role); err != nil {
		// We check ttl of lease instead of check `session.Done`, because
		// `session.Done` is only notified when etcd client establish a
		// new keepalive request, there could be a time window as long as
		// 1/3 of session ttl that `session.Done` can't be triggered even
		// the lease is already revoked.
		switch {
		case cerror.ErrEtcdSessionDone.Equal(err),
			cerror.ErrLeaseExpired.Equal(err):
			log.Warn("session is disconnected", zap.Error(err))
			return cerror.ErrCaptureSuicide.GenWithStackByArgs()
		}
		lease, inErr := c.EtcdClient.GetEtcdClient().TimeToLive(ctx, c.session.Lease())
		if inErr != nil {
			return cerror.WrapError(cerror.ErrPDEtcdAPIError, inErr)
		}
		if lease.TTL == int64(-1) {
			log.Warn("session is disconnected", zap.Error(err))
			return cerror.ErrCaptureSuicide.GenWithStackByArgs()
		}
		return errors.Trace(err)
	}
	return nil
}

func (c *captureImpl) setOwner(owner coordinator.Coordinator) {
	c.coordinatorMu.Lock()
	defer c.coordinatorMu.Unlock()
	c.coordinator = owner
}

// GetOwner returns coordinator if it is the coordinator.
func (c *captureImpl) GetOwner() (coordinator.Coordinator, error) {
	c.coordinatorMu.Lock()
	defer c.coordinatorMu.Unlock()
	if c.coordinator == nil {
		return nil, cerror.ErrNotOwner.GenWithStackByArgs()
	}
	return c.coordinator, nil
}

// campaign to be an coordinator.
func (c *captureImpl) campaign(ctx context.Context) error {
	// TODO: `Campaign` will get stuck when send SIGSTOP to pd leader.
	// For `Campaign`, when send SIGSTOP to pd leader, cdc maybe call `cancel`
	// (cause by `processor routine` exit). And inside `Campaign`, the routine
	// return from `waitDeletes`(https://github.com/etcd-io/etcd/blob/main/client/v3/concurrency/election.go#L93),
	// then call `Resign`(note: use `client.Ctx`) to etcd server. But the etcd server
	// (the client connects to) has entered the STOP state, which means that
	// the server cannot process the request, but will still maintain the GRPC
	// connection. So `routine` will block 'Resign'.
	return cerror.WrapError(cerror.ErrCaptureCampaignOwner, c.election.campaign(ctx, c.info.ID))
}

// resign lets an coordinator start a new election.
func (c *captureImpl) resign(ctx context.Context) error {
	failpoint.Inject("capture-resign-failed", func() {
		failpoint.Return(errors.New("capture resign failed"))
	})
	if c.election == nil {
		return nil
	}
	return cerror.WrapError(cerror.ErrCaptureResignOwner, c.election.resign(ctx))
}

// register the capture by put the capture's information in etcd
func (c *captureImpl) register(ctx context.Context) error {
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
	o, _ := c.GetOwner()
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

// IsOwner returns whether the capture is an coordinator
func (c *captureImpl) IsOwner() bool {
	c.coordinatorMu.Lock()
	defer c.coordinatorMu.Unlock()
	return c.coordinator != nil
}

func (c *captureImpl) GetPdClient() pd.Client {
	return c.pdClient
}

// GetOwnerCaptureInfo return the controller capture info of current TiCDC cluster
func (c *captureImpl) GetOwnerCaptureInfo(ctx context.Context) (*model.CaptureInfo, error) {
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
