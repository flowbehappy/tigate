// Copyright 2023 PingCAP, Inc.
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

package puller

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flowbehappy/tigate/logservice/eventsource"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/puller/frontier"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	resolveLockFence        time.Duration = 4 * time.Second
	resolveLockTickInterval time.Duration = 2 * time.Second

	// Suppose there are 50K tables, total size of `resolvedEventsCache`s will be
	// unsafe.SizeOf(eventsource.MultiplexingEvent) * 50K * 256 = 800M.
	tableResolvedTsBufferSize int = 256

	inputChSize                = 1024
	tableProgressAdvanceChSize = 128
)

type tableProgress struct {
	client          *eventsource.SharedClient
	spans           []tablepb.Span
	subscriptionIDs []eventsource.SubscriptionID
	startTs         eventsource.Ts
	tableName       string

	initialized          atomic.Bool
	resolvedTsUpdated    atomic.Int64
	resolvedTs           atomic.Uint64
	maxIngressResolvedTs atomic.Uint64

	resolvedEventsCache chan eventsource.MultiplexingEvent
	tsTracker           frontier.Frontier

	consume struct {
		// This lock is used to prevent the table progress from being
		// removed while consuming events.
		sync.RWMutex
		removed bool
		f       func(context.Context, *common.RawKVEntry, []tablepb.Span) error
	}

	scheduled atomic.Bool
	start     time.Time
}

func (p *tableProgress) handleResolvedSpans(ctx context.Context, e *common.ResolvedSpans) (err error) {
	for _, resolvedSpan := range e.Spans {
		if !spanz.IsSubSpan(resolvedSpan.Span, p.spans...) {
			log.Panic("the resolved span is not in the table spans",
				zap.String("tableName", p.tableName),
				zap.Any("spans", p.spans))
		}
		p.tsTracker.Forward(resolvedSpan.Region, resolvedSpan.Span, e.ResolvedTs)
		if e.ResolvedTs > p.maxIngressResolvedTs.Load() {
			p.maxIngressResolvedTs.Store(e.ResolvedTs)
		}
	}
	resolvedTs := p.tsTracker.Frontier()

	if resolvedTs > 0 && p.initialized.CompareAndSwap(false, true) {
		log.Info("puller is initialized",
			zap.String("tableName", p.tableName),
			zap.Any("tableID", p.spans),
			zap.Uint64("resolvedTs", resolvedTs),
			zap.Duration("duration", time.Since(p.start)),
		)
	}
	if resolvedTs > p.resolvedTs.Load() {
		p.resolvedTs.Store(resolvedTs)
		p.resolvedTsUpdated.Store(time.Now().Unix())
		raw := &common.RawKVEntry{CRTs: resolvedTs, OpType: common.OpTypeResolved}
		err = p.consume.f(ctx, raw, p.spans)
	}

	return
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
	for _, subID := range p.subscriptionIDs {
		p.client.ResolveLock(subID, targetTs)
	}
}

type subscription struct {
	*tableProgress
	subID eventsource.SubscriptionID
}

// MultiplexingPuller works with `eventsource.SharedClient`. All tables share resources.
type MultiplexingPuller struct {
	client  *eventsource.SharedClient
	pdClock pdutil.Clock
	consume func(context.Context, *common.RawKVEntry, []tablepb.Span) error
	// inputChannelIndexer is used to determine which input channel to use for a given span.
	inputChannelIndexer func(span tablepb.Span, workerCount int) int

	// inputChs is used to collect events from client.
	inputChs []chan eventsource.MultiplexingEvent
	// tableProgressAdvanceCh is used to notify the tableAdvancer goroutine
	// to advance the progress of a table.
	tableProgressAdvanceCh chan *tableProgress

	// NOTE: A tableProgress can have multiple subscription, all of them share
	// the same tableProgress. So, we use two maps to store the relationshipxxx
	// between subscription and tableProgress.
	subscriptions struct {
		sync.RWMutex
		// m map subscriptionID -> tableProgress, used to cache and
		// get tableProgress by subscriptionID quickly.
		m map[eventsource.SubscriptionID]*tableProgress
		// n map span -> subscription, used to cache and get subscription by span quickly.
		n *spanz.HashMap[subscription]
	}

	resolvedTsAdvancerCount int
}

// NewMultiplexingPuller creates a MultiplexingPuller.
// `workerCount` specifies how many workers will be spawned to handle events from kv client.
// `frontierCount` specifies how many workers will be spawned to handle resolvedTs event.
func NewMultiplexingPuller(
	client *eventsource.SharedClient,
	pdClock pdutil.Clock,
	consume func(context.Context, *common.RawKVEntry, []tablepb.Span) error,
	workerCount int,
	inputChannelIndexer func(tablepb.Span, int) int,
	resolvedTsAdvancerCount int,
) *MultiplexingPuller {
	mpuller := &MultiplexingPuller{
		client:                  client,
		pdClock:                 pdClock,
		consume:                 consume,
		inputChannelIndexer:     inputChannelIndexer,
		resolvedTsAdvancerCount: resolvedTsAdvancerCount,
		tableProgressAdvanceCh:  make(chan *tableProgress, tableProgressAdvanceChSize),
	}
	mpuller.subscriptions.m = make(map[eventsource.SubscriptionID]*tableProgress)
	mpuller.subscriptions.n = spanz.NewHashMap[subscription]()

	mpuller.inputChs = make([]chan eventsource.MultiplexingEvent, 0, workerCount)
	for i := 0; i < workerCount; i++ {
		mpuller.inputChs = append(mpuller.inputChs, make(chan eventsource.MultiplexingEvent, inputChSize))
	}
	return mpuller
}

// Subscribe some spans. They will share one same resolved timestamp progress.
func (p *MultiplexingPuller) Subscribe(
	spans []tablepb.Span,
	startTs eventsource.Ts,
	tableName string,
) {
	p.subscriptions.Lock()
	defer p.subscriptions.Unlock()
	p.subscribe(spans, startTs, tableName)
}

func (p *MultiplexingPuller) subscribe(
	spans []tablepb.Span,
	startTs eventsource.Ts,
	tableName string,
) {
	for _, span := range spans {
		// Base on the current design, a MultiplexingPuller is only used for one changefeed.
		// So, one span can only be subscribed once.
		if _, exists := p.subscriptions.n.Get(span); exists {
			log.Panic("redundant subscription",
				zap.String("span", span.String()))
		}
	}

	// Create a new table progress for the spans.
	progress := &tableProgress{
		client:          p.client,
		spans:           spans,
		subscriptionIDs: make([]eventsource.SubscriptionID, len(spans)),
		startTs:         startTs,
		tableName:       tableName,

		resolvedEventsCache: make(chan eventsource.MultiplexingEvent, tableResolvedTsBufferSize),
		tsTracker:           frontier.NewFrontier(0, spans...),
		start:               time.Now(),
	}

	progress.consume.f = func(
		ctx context.Context,
		raw *common.RawKVEntry,
		spans []tablepb.Span,
	) error {
		progress.consume.RLock()
		defer progress.consume.RUnlock()
		if !progress.consume.removed {
			return p.consume(ctx, raw, spans)
		}
		return nil
	}

	for i, span := range spans {
		subID := p.client.AllocSubscriptionID()
		progress.subscriptionIDs[i] = subID

		p.subscriptions.m[subID] = progress
		p.subscriptions.n.ReplaceOrInsert(span, subscription{progress, subID})

		slot := p.inputChannelIndexer(span, len(p.inputChs))
		p.client.Subscribe(subID, span, uint64(startTs), p.inputChs[slot])
	}

	progress.initialized.Store(false)
	progress.resolvedTsUpdated.Store(time.Now().Unix())
}

// Unsubscribe some spans, which must be subscribed in one call.
func (p *MultiplexingPuller) Unsubscribe(spans []tablepb.Span) {
	p.subscriptions.Lock()
	defer p.subscriptions.Unlock()
	p.unsubscribe(spans)
}

func (p *MultiplexingPuller) unsubscribe(spans []tablepb.Span) {
	var progress *tableProgress
	for _, span := range spans {
		if prog, exists := p.subscriptions.n.Get(span); exists {
			if prog.tableProgress != progress && progress != nil {
				log.Panic("unsubscribe spans not in one subscription")
			}
			progress = prog.tableProgress
		} else {
			log.Panic("unexist unsubscription",
				zap.String("span", span.String()))
		}
	}
	if len(progress.spans) != len(spans) {
		log.Panic("unsubscribe spans not same with subscription")
	}

	progress.consume.Lock()
	progress.consume.removed = true
	progress.consume.Unlock()
	for i, span := range progress.spans {
		p.client.Unsubscribe(progress.subscriptionIDs[i])
		delete(p.subscriptions.m, progress.subscriptionIDs[i])
		p.subscriptions.n.Delete(span)
	}
}

// Run the puller.
func (p *MultiplexingPuller) Run(ctx context.Context) (err error) {
	return p.run(ctx, true)
}

func (p *MultiplexingPuller) run(ctx context.Context, includeClient bool) error {
	defer func() {
		log.Info("MultiplexingPuller exits")
	}()

	eg, ctx := errgroup.WithContext(ctx)

	// Only !includeClient in tests.
	if includeClient {
		eg.Go(func() error { return p.client.Run(ctx) })
	}

	// Start workers to handle events received from kv client.
	for i := range p.inputChs {
		inputCh := p.inputChs[i]
		eg.Go(func() error { return p.runEventHandler(ctx, inputCh) })
	}

	// Start workers to check and resolve stale locks.
	eg.Go(func() error { return p.runResolveLockChecker(ctx) })

	for i := 0; i < p.resolvedTsAdvancerCount; i++ {
		eg.Go(func() error { return p.runResolvedTsAdvancer(ctx) })
	}

	log.Info("MultiplexingPuller starts",
		zap.Int("workerConcurrent", len(p.inputChs)),
		zap.Int("frontierConcurrent", p.resolvedTsAdvancerCount))
	return eg.Wait()
}

// runEventHandler consumes events from inputCh:
// 1. If the event is a kv event, consume by calling progress.consume.f.
// 2. If the event is a resolved event, send it to the resolvedEventsCache of the corresponding progress.
func (p *MultiplexingPuller) runEventHandler(ctx context.Context, inputCh <-chan eventsource.MultiplexingEvent) error {
	for {
		var e eventsource.MultiplexingEvent
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e = <-inputCh:
		}

		progress := p.getProgress(e.SubscriptionID)
		// There is a chance that some stale events are received after
		// the subscription is removed. We can just ignore them.
		if progress == nil {
			continue
		}

		if e.Val != nil {
			if err := progress.consume.f(ctx, e.Val, progress.spans); err != nil {
				return errors.Trace(err)
			}
		} else if e.Resolved != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case progress.resolvedEventsCache <- e:
				p.schedule(ctx, progress)
			default:
			}
		}
	}
}

func (p *MultiplexingPuller) getProgress(subID eventsource.SubscriptionID) *tableProgress {
	p.subscriptions.RLock()
	defer p.subscriptions.RUnlock()
	return p.subscriptions.m[subID]
}

func (p *MultiplexingPuller) getAllProgresses() map[*tableProgress]struct{} {
	p.subscriptions.RLock()
	defer p.subscriptions.RUnlock()
	hashset := make(map[*tableProgress]struct{}, len(p.subscriptions.m))
	for _, value := range p.subscriptions.m {
		hashset[value] = struct{}{}
	}
	return hashset
}

func (p *MultiplexingPuller) schedule(ctx context.Context, progress *tableProgress) {
	if progress.scheduled.CompareAndSwap(false, true) {
		select {
		case <-ctx.Done():
		case p.tableProgressAdvanceCh <- progress:
		}
	}
}

// runResolvedTsAdvancer receives tableProgress from tableProgressAdvanceCh
// and advances the resolvedTs of the tableProgress.
func (p *MultiplexingPuller) runResolvedTsAdvancer(ctx context.Context) error {
	advanceTableProgress := func(ctx context.Context, progress *tableProgress) error {
		defer func() {
			progress.scheduled.Store(false)
			// Schedule the progress again if there are still events in the cache.
			if len(progress.resolvedEventsCache) > 0 {
				p.schedule(ctx, progress)
			}
		}()

		var event eventsource.MultiplexingEvent
		var spans *common.ResolvedSpans
		for i := 0; i < 128; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case event = <-progress.resolvedEventsCache:
				spans = event.RegionFeedEvent.Resolved
			default:
				return nil
			}
			if err := progress.handleResolvedSpans(ctx, spans); err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}

	var progress *tableProgress
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case progress = <-p.tableProgressAdvanceCh:
			if err := advanceTableProgress(ctx, progress); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (p *MultiplexingPuller) runResolveLockChecker(ctx context.Context) error {
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

// Stats of a puller.
type Stats struct {
	RegionCount         uint64
	CheckpointTsIngress eventsource.Ts
	ResolvedTsIngress   eventsource.Ts
	CheckpointTsEgress  eventsource.Ts
	ResolvedTsEgress    eventsource.Ts
}

// Stats returns Stats.
func (p *MultiplexingPuller) Stats(span tablepb.Span) Stats {
	p.subscriptions.RLock()
	progress := p.subscriptions.n.GetV(span)
	p.subscriptions.RUnlock()
	if progress.tableProgress == nil {
		return Stats{}
	}
	return Stats{
		RegionCount:         p.client.RegionCount(progress.subID),
		ResolvedTsIngress:   eventsource.Ts(progress.maxIngressResolvedTs.Load()),
		CheckpointTsIngress: eventsource.Ts(progress.maxIngressResolvedTs.Load()),
		ResolvedTsEgress:    eventsource.Ts(progress.resolvedTs.Load()),
		CheckpointTsEgress:  eventsource.Ts(progress.resolvedTs.Load()),
	}
}
