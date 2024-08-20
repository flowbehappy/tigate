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

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	resolveLockFence        time.Duration = 4 * time.Second
	resolveLockTickInterval time.Duration = 2 * time.Second
)

type tableProgress struct {
	client *SharedClient

	span heartbeatpb.TableSpan

	subID SubscriptionID

	initialized       atomic.Bool
	resolvedTsUpdated atomic.Int64
	resolvedTs        atomic.Uint64

	consume struct {
		// This lock is used to prevent the table progress from being
		// removed while consuming events.
		sync.RWMutex
		removed bool
		f       func(context.Context, *common.RawKVEntry, heartbeatpb.TableSpan) error
	}
}

func (p *tableProgress) resolveLock(currentTime time.Time) {
	resolvedTsUpdated := time.Unix(p.resolvedTsUpdated.Load(), 0)
	if !p.initialized.Load() || time.Since(resolvedTsUpdated) < resolveLockFence {
		return
	}
	resolvedTs := p.resolvedTs.Load()
	resolvedTime := oracle.GetTimeFromTS(resolvedTs)
	if currentTime.Sub(resolvedTime) < resolveLockFence {
		return
	}

	targetTs := oracle.GoTimeToTS(resolvedTime.Add(resolveLockFence))
	p.client.ResolveLock(p.subID, targetTs)
}

type LogPuller struct {
	client  *SharedClient
	pdClock pdutil.Clock
	consume func(context.Context, *common.RawKVEntry, heartbeatpb.TableSpan) error

	// inputChSelector is used to determine which input channel to use for a given span.
	inputChSelector func(span heartbeatpb.TableSpan, workerCount int) int

	// inputChs is used to collect events from client.
	inputChs []chan MultiplexingEvent

	subscriptions struct {
		sync.RWMutex
		// subscriptionID -> tableProgress
		tableProgressMap map[SubscriptionID]*tableProgress
		// span -> subscription
		subscriptionMap *common.SpanHashMap[SubscriptionID]
	}
}

type LogPullerConfig struct {
	// `WorkerCount` specifies how many workers will be spawned to handle events from kv client.
	WorkerCount int
	// `HashSpanFunc` is used to determine which input channel to use for a given span.
	HashSpanFunc func(heartbeatpb.TableSpan, int) int
}

func NewLogPuller(
	client *SharedClient,
	pdClock pdutil.Clock,
	consume func(context.Context, *common.RawKVEntry, heartbeatpb.TableSpan) error,
	config *LogPullerConfig,
) *LogPuller {
	puller := &LogPuller{
		client:          client,
		pdClock:         pdClock,
		consume:         consume,
		inputChSelector: config.HashSpanFunc,
	}
	puller.subscriptions.tableProgressMap = make(map[SubscriptionID]*tableProgress)
	puller.subscriptions.subscriptionMap = common.NewSpanHashMap[SubscriptionID]()

	puller.inputChs = make([]chan MultiplexingEvent, 0, config.WorkerCount)
	for i := 0; i < config.WorkerCount; i++ {
		puller.inputChs = append(puller.inputChs, make(chan MultiplexingEvent, 1024))
	}
	return puller
}

func (p *LogPuller) Run(ctx context.Context) (err error) {
	eg, ctx := errgroup.WithContext(ctx)

	// Start the kv client.
	eg.Go(func() error { return p.client.Run(ctx) })

	// Start workers to handle events received from kv client.
	for i := range p.inputChs {
		inputCh := p.inputChs[i]
		eg.Go(func() error { return p.runEventHandler(ctx, inputCh) })
	}

	eg.Go(func() error { return p.runResolveLockChecker(ctx) })

	log.Info("LogPuller starts",
		zap.Int("workerConcurrent", len(p.inputChs)))
	defer func() {
		log.Info("LogPuller exits", zap.Error(err))
	}()
	return eg.Wait()
}

func (p *LogPuller) Subscribe(
	span heartbeatpb.TableSpan,
	startTs common.Ts,
) {
	p.subscriptions.Lock()
	defer p.subscriptions.Unlock()

	// FIXME: support subscribe the same span multiple times in LogPuller(not sure whether it is supported already)
	if _, exists := p.subscriptions.subscriptionMap.Get(span); exists {
		log.Panic("redundant subscription", zap.String("span", span.String()))
	}

	subID := p.client.AllocSubscriptionID()

	progress := &tableProgress{
		span:  span,
		subID: subID,
	}

	progress.consume.f = func(
		ctx context.Context,
		raw *common.RawKVEntry,
		span heartbeatpb.TableSpan,
	) error {
		progress.consume.RLock()
		defer progress.consume.RUnlock()
		if !progress.consume.removed {
			return p.consume(ctx, raw, span)
		}
		return nil
	}

	p.subscriptions.tableProgressMap[subID] = progress
	p.subscriptions.subscriptionMap.ReplaceOrInsert(span, subID)

	slot := p.inputChSelector(span, len(p.inputChs))
	p.client.Subscribe(subID, span, uint64(startTs), p.inputChs[slot])
}

func (p *LogPuller) Unsubscribe(span heartbeatpb.TableSpan) {
	p.subscriptions.Lock()
	defer p.subscriptions.Unlock()

	subID, ok := p.subscriptions.subscriptionMap.Get(span)
	if !ok {
		log.Panic("unexist unsubscription", zap.String("span", span.String()))
	}
	progress := p.subscriptions.tableProgressMap[subID]

	progress.consume.Lock()
	progress.consume.removed = true
	progress.consume.Unlock()
	p.client.Unsubscribe(progress.subID)
	delete(p.subscriptions.tableProgressMap, progress.subID)
	p.subscriptions.subscriptionMap.Delete(span)
}

func (p *LogPuller) runEventHandler(ctx context.Context, inputCh <-chan MultiplexingEvent) error {
	for {
		var e MultiplexingEvent
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e = <-inputCh:
		}

		progress := p.getProgress(e.SubscriptionID)
		// There is a chance that some stale events are received after
		// the subscription is removed. We can just ignore them.
		if progress == nil {
			log.Info("meet stale event",
				zap.Any("subscriptionID", e.SubscriptionID))
			continue
		}

		if e.Val == nil {
			log.Info("meet empty event")
			continue
		}

		if err := progress.consume.f(ctx, e.Val, progress.span); err != nil {
			log.Info("consume error", zap.Error(err))
			return errors.Trace(err)
		}
	}
}

func (p *LogPuller) getProgress(subID SubscriptionID) *tableProgress {
	p.subscriptions.RLock()
	defer p.subscriptions.RUnlock()
	return p.subscriptions.tableProgressMap[subID]
}

func (p *LogPuller) getAllProgresses() map[*tableProgress]struct{} {
	p.subscriptions.RLock()
	defer p.subscriptions.RUnlock()
	hashset := make(map[*tableProgress]struct{}, len(p.subscriptions.tableProgressMap))
	for _, value := range p.subscriptions.tableProgressMap {
		hashset[value] = struct{}{}
	}
	return hashset
}

func (p *LogPuller) runResolveLockChecker(ctx context.Context) error {
	resolveLockTicker := time.NewTicker(resolveLockTickInterval)
	defer resolveLockTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resolveLockTicker.C:
		}
		currentTime := p.pdClock.CurrentTime()
		for progress := range p.getAllProgresses() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				progress.resolveLock(currentTime)
			}
		}
	}
}
