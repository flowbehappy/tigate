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

package logpuller

// import (
// 	"context"
// 	"sync"
// 	"sync/atomic"
// 	"time"

// 	"github.com/pingcap/errors"
// 	"github.com/pingcap/log"
// 	"github.com/pingcap/ticdc/heartbeatpb"
// 	"github.com/pingcap/ticdc/pkg/common"
// 	"github.com/pingcap/ticdc/pkg/metrics"
// 	"github.com/pingcap/tiflow/pkg/pdutil"
// 	"github.com/prometheus/client_golang/prometheus"
// 	"github.com/tikv/client-go/v2/oracle"
// 	"go.uber.org/zap"
// 	"golang.org/x/sync/errgroup"
// )

// const (
// 	resolveLockFence        time.Duration = 4 * time.Second
// 	resolveLockTickInterval time.Duration = 2 * time.Second
// )

// type spanProgress struct {
// 	client *SubscriptionClient

// 	span heartbeatpb.TableSpan

// 	subID SubscriptionID

// 	initialized       atomic.Bool
// 	resolvedTsUpdated atomic.Int64
// 	resolvedTs        atomic.Uint64

// 	consume struct {
// 		// This lock is used to prevent the table progress from being
// 		// removed while consuming events.
// 		sync.RWMutex
// 		removed bool
// 		f       func(*common.RawKVEntry, SubscriptionID) error
// 	}
// }

// func (p *spanProgress) resolveLock(currentTime time.Time) {
// 	resolvedTsUpdated := time.Unix(p.resolvedTsUpdated.Load(), 0)
// 	if !p.initialized.Load() || time.Since(resolvedTsUpdated) < resolveLockFence {
// 		return
// 	}
// 	resolvedTs := p.resolvedTs.Load()
// 	resolvedTime := oracle.GetTimeFromTS(resolvedTs)
// 	if currentTime.Sub(resolvedTime) < resolveLockFence {
// 		return
// 	}

// 	targetTs := oracle.GoTimeToTS(resolvedTime.Add(resolveLockFence))
// 	p.client.ResolveLock(p.subID, targetTs)
// }

// type LogPuller struct {
// 	client  *SubscriptionClient
// 	pdClock pdutil.Clock
// 	consume func(*common.RawKVEntry, SubscriptionID) error

// 	subscriptions struct {
// 		sync.RWMutex
// 		// subscriptionID -> spanProgress
// 		spanProgressMap map[SubscriptionID]*spanProgress
// 	}

// 	CounterKv       prometheus.Counter
// 	CounterResolved prometheus.Counter
// }

// func NewLogPuller(
// 	client *SubscriptionClient,
// 	pdClock pdutil.Clock,
// 	consume func(*common.RawKVEntry, SubscriptionID) error,
// ) *LogPuller {
// 	puller := &LogPuller{
// 		client:  client,
// 		pdClock: pdClock,
// 		consume: consume,
// 	}
// 	puller.subscriptions.spanProgressMap = make(map[SubscriptionID]*spanProgress)

// 	return puller
// }

// func (p *LogPuller) updateMetrics(ctx context.Context) error {
// 	ticker := time.NewTicker(10 * time.Second)
// 	defer ticker.Stop()
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return ctx.Err()
// 		case <-ticker.C:
// 			resolvedTsLag := p.client.GetResolvedTsLag()
// 			if resolvedTsLag > 0 {
// 				metrics.LogPullerResolvedTsLag.Set(resolvedTsLag)
// 			}
// 		}
// 	}
// }

// func (p *LogPuller) Run(ctx context.Context) (err error) {
// 	// TODO: distiguish event between event store and schema store
// 	p.CounterKv = metrics.EventStoreReceivedEventCount.WithLabelValues("kv")
// 	p.CounterResolved = metrics.EventStoreReceivedEventCount.WithLabelValues("resolved")
// 	defer func() {
// 		metrics.EventStoreReceivedEventCount.DeleteLabelValues("kv")
// 		metrics.EventStoreReceivedEventCount.DeleteLabelValues("resolved")
// 		log.Info("LogPuller exits", zap.Error(err))
// 	}()

// 	consumeLogEvent := func(e LogEvent) error {
// 		progress := p.getProgress(e.SubscriptionID)
// 		// There is a chance that some stale events are received after
// 		// the subscription is removed. We can just ignore them.
// 		if progress == nil {
// 			log.Info("meet stale event",
// 				zap.Any("subscriptionID", e.SubscriptionID))
// 			return nil
// 		}

// 		if e.Val == nil {
// 			log.Info("meet empty event")
// 			return nil
// 		}

// 		if e.Val.IsResolved() {
// 			p.CounterResolved.Inc()
// 		} else {
// 			p.CounterKv.Inc()
// 		}

// 		if err := progress.consume.f(e.Val, e.SubscriptionID); err != nil {
// 			log.Info("consume error", zap.Error(err))
// 			return errors.Trace(err)
// 		}
// 		return nil
// 	}

// 	eg, ctx := errgroup.WithContext(ctx)

// 	eg.Go(func() error { return p.runResolveLockChecker(ctx) })

// 	eg.Go(func() error { return p.updateMetrics(ctx) })

// 	log.Info("LogPuller starts")
// 	return eg.Wait()
// }

// func (p *LogPuller) Close(ctx context.Context) error {
// 	return p.client.Close(ctx)
// }

// func (p *LogPuller) Subscribe(
// 	span heartbeatpb.TableSpan,
// 	startTs uint64,
// ) SubscriptionID {
// 	p.subscriptions.Lock()

// 	subID := p.client.AllocSubscriptionID()

// 	progress := &spanProgress{
// 		span:  span,
// 		subID: subID,
// 	}

// 	progress.consume.f = func(
// 		raw *common.RawKVEntry,
// 		subID SubscriptionID,
// 	) error {
// 		progress.consume.RLock()
// 		defer progress.consume.RUnlock()
// 		if !progress.consume.removed {
// 			return p.consume(raw, subID)
// 		}
// 		return nil
// 	}

// 	p.subscriptions.spanProgressMap[subID] = progress
// 	p.subscriptions.Unlock()

// 	p.client.Subscribe(subID, span, startTs)
// 	return subID
// }

// func (p *LogPuller) Unsubscribe(subID SubscriptionID) {
// 	p.subscriptions.Lock()
// 	defer p.subscriptions.Unlock()

// 	progress, ok := p.subscriptions.spanProgressMap[subID]
// 	if !ok {
// 		log.Warn("unexist unsubscription", zap.Uint64("subscriptionID", uint64(subID)))
// 		return
// 	}

// 	progress.consume.Lock()
// 	progress.consume.removed = true
// 	progress.consume.Unlock()
// 	delete(p.subscriptions.spanProgressMap, progress.subID)

// 	// TODO: check whether need to unlock before call client.Unsubscribe
// 	p.client.Unsubscribe(progress.subID)
// }

// func (p *LogPuller) getProgress(subID SubscriptionID) *spanProgress {
// 	p.subscriptions.RLock()
// 	defer p.subscriptions.RUnlock()
// 	return p.subscriptions.spanProgressMap[subID]
// }

// func (p *LogPuller) getAllProgresses() map[*spanProgress]struct{} {
// 	p.subscriptions.RLock()
// 	defer p.subscriptions.RUnlock()
// 	hashset := make(map[*spanProgress]struct{}, len(p.subscriptions.spanProgressMap))
// 	for _, value := range p.subscriptions.spanProgressMap {
// 		hashset[value] = struct{}{}
// 	}
// 	return hashset
// }

// func (p *LogPuller) runResolveLockChecker(ctx context.Context) error {
// 	resolveLockTicker := time.NewTicker(resolveLockTickInterval)
// 	defer resolveLockTicker.Stop()
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return ctx.Err()
// 		case <-resolveLockTicker.C:
// 		}
// 		currentTime := p.pdClock.CurrentTime()
// 		for progress := range p.getAllProgresses() {
// 			select {
// 			case <-ctx.Done():
// 				return ctx.Err()
// 			default:
// 				progress.resolveLock(currentTime)
// 			}
// 		}
// 	}
// }
