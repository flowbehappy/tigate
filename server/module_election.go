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
	"time"

	"github.com/flowbehappy/tigate/coordinator"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/mvcc"
)

type elector struct {
	election   *concurrency.Election
	serverImpl *serverImpl
}

func NewElector(serverImpl *serverImpl) common.SubModule {
	election := concurrency.NewElection(serverImpl.session,
		etcd.CaptureOwnerKey(serverImpl.EtcdClient.GetClusterID()))
	return &elector{
		serverImpl: serverImpl,
		election:   election,
	}
}

func (e *elector) Run(ctx context.Context) error {
	return e.campaignCoordinator(ctx)
}

func (e *elector) Name() string {
	return "elector"
}

func (e *elector) campaignCoordinator(ctx context.Context) error {
	cfg := config.GetGlobalServerConfig()
	// In most failure cases, we don't return error directly, just run another
	// campaign loop. We treat campaign loop as a special background routine.
	ownerFlushInterval := time.Duration(cfg.OwnerFlushInterval)
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
		if e.serverImpl.liveness.Load() == model.LivenessCaptureStopping {
			log.Info("do not campaign coordinator, liveness is stopping",
				zap.String("captureID", e.serverImpl.info.ID))
			return nil
		}
		// Campaign to be the coordinator, it blocks until it been elected.
		if err := e.election.Campaign(ctx, e.serverImpl.info.ID); err != nil {
			rootErr := errors.Cause(err)
			if rootErr == context.Canceled {
				return nil
			} else if rootErr == mvcc.ErrCompacted || isErrCompacted(rootErr) {
				log.Warn("campaign coordinator failed due to etcd revision "+
					"has been compacted, retry later", zap.Error(err))
				continue
			}
			log.Warn("campaign coordinator failed",
				zap.String("captureID", e.serverImpl.info.ID), zap.Error(err))
			return cerror.ErrCaptureSuicide.GenWithStackByArgs()
		}
		// After campaign check liveness again.
		// It is possible it becomes the coordinator right after receiving SIGTERM.
		if e.serverImpl.liveness.Load() == model.LivenessCaptureStopping {
			// If the server is stopping, resign actively.
			log.Info("resign coordinator actively, liveness is stopping")
			if resignErr := e.resign(ctx); resignErr != nil {
				log.Warn("resign coordinator actively failed",
					zap.String("captureID", e.serverImpl.info.ID), zap.Error(resignErr))
				return errors.Trace(err)
			}
			return nil
		}

		coordinatorVersion, err := e.serverImpl.EtcdClient.GetOwnerRevision(ctx,
			e.serverImpl.info.ID)
		if err != nil {
			return errors.Trace(err)
		}

		log.Info("campaign coordinator successfully",
			zap.String("captureID", e.serverImpl.info.ID),
			zap.Int64("coordinatorVersion", coordinatorVersion))

		co := coordinator.NewCoordinator(e.serverImpl.info,
			e.serverImpl.pdClient, e.serverImpl.PDClock, e.serverImpl.EtcdClient,
			coordinatorVersion)
		e.serverImpl.setCoordinator(co)

		// watcher changefeed changes
		watcher := watcher.NewEtcdWatcher(e.serverImpl.EtcdClient,
			e.serverImpl.session,
			// changefeed info key prefix
			etcd.BaseKey(e.serverImpl.EtcdClient.GetClusterID()),
			"coordinator")

		err = watcher.RunEtcdWorker(ctx, co.(orchestrator.Reactor),
			orchestrator.NewGlobalState(e.serverImpl.EtcdClient.GetClusterID(),
				cfg.CaptureSessionTTL),
			ownerFlushInterval)
		e.serverImpl.coordinator.AsyncStop()
		e.serverImpl.setCoordinator(nil)

		if !cerror.ErrNotOwner.Equal(err) {
			// if coordinator exits, resign the coordinator key,
			// use a new context to prevent the context from being cancelled.
			resignCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if resignErr := e.resign(resignCtx); resignErr != nil {
				if errors.Cause(resignErr) != context.DeadlineExceeded {
					log.Info("coordinator resign failed", zap.String("captureID", e.serverImpl.info.ID),
						zap.Error(resignErr), zap.Int64("coordinatorVersion", coordinatorVersion))
					cancel()
					return errors.Trace(resignErr)
				}

				log.Warn("coordinator resign timeout", zap.String("captureID", e.serverImpl.info.ID),
					zap.Error(resignErr), zap.Int64("coordinatorVersion", coordinatorVersion))
			}
			cancel()
		}

		log.Info("coordinator resigned successfully",
			zap.String("captureID", e.serverImpl.info.ID),
			zap.Int64("coordinatorVersion", coordinatorVersion))
		if err != nil {
			log.Warn("run coordinator exited with error",
				zap.String("captureID", e.serverImpl.info.ID),
				zap.Int64("coordinatorVersion", coordinatorVersion),
				zap.Error(err))
			// for errors, return error and let server exits or restart
			return errors.Trace(err)
		}
		// if coordinator exits normally, continue the campaign loop and try to election coordinator again
		log.Info("run coordinator exited normally",
			zap.String("captureID", e.serverImpl.info.ID),
			zap.Int64("coordinatorVersion", coordinatorVersion))
	}
}

func (e *elector) Close(ctx context.Context) error {
	return nil
}

// resign lets the coordinator start a new election.
func (e *elector) resign(ctx context.Context) error {
	if e.election == nil {
		return nil
	}
	return cerror.WrapError(cerror.ErrCaptureResignOwner,
		e.election.Resign(ctx))
}
