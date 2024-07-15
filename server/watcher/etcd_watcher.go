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

package watcher

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/migrate"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

type EtcdWatcher struct {
	etcdClient etcd.CDCEtcdClient
	baseKey    string
	role       string
	session    *concurrency.Session
}

func NewEtcdWatcher(etcdClient etcd.CDCEtcdClient, session *concurrency.Session,
	baseKey string, role string) *EtcdWatcher {
	return &EtcdWatcher{
		etcdClient: etcdClient,
		baseKey:    baseKey,
		role:       role,
		session:    session,
	}
}

func (w *EtcdWatcher) RunEtcdWorker(
	ctx context.Context,
	reactor orchestrator.Reactor,
	reactorState *orchestrator.GlobalReactorState,
	timerInterval time.Duration,
) error {
	log.Info("start to run etcd worker", zap.String("role", w.role))
	reactorState.Role = w.role
	etcdWorker, err := orchestrator.NewEtcdWorker(w.etcdClient,
		w.baseKey, reactor, reactorState, &migrate.NoOpMigrator{})
	if err != nil {
		return errors.Trace(err)
	}
	if err := etcdWorker.Run(ctx, w.session, timerInterval, w.role); err != nil {
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
		lease, inErr := w.etcdClient.GetEtcdClient().TimeToLive(ctx, w.session.Lease())
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
