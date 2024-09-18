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
	"math"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// LogPullerMultiSpan is a simple wrapper around LogPuller.
// It just maintain the minimum resolve ts of all spans.
// TODO: may be we can implement it in other way(not as a xxxPuller)?
type LogPullerMultiSpan struct {
	innerPuller *LogPuller

	startTs common.Ts

	consume func(context.Context, *common.RawKVEntry) error

	notifyCh chan interface{}

	mu sync.Mutex

	resolvedTsMap map[SubscriptionID]common.Ts

	prevResolvedTs common.Ts

	pendingResolvedTs common.Ts
}

func NewLogPullerMultiSpan(
	client *SubscriptionClient,
	pdClock pdutil.Clock,
	spans []heartbeatpb.TableSpan,
	startTs common.Ts,
	consume func(context.Context, *common.RawKVEntry) error,
) *LogPullerMultiSpan {
	// TODO: remove this when we use a priority queue to maintain the resolved ts.(just check at least one span)
	if len(spans) != 2 {
		log.Panic("not supported")
	}
	pullerWrapper := &LogPullerMultiSpan{
		startTs:           startTs,
		consume:           consume,
		notifyCh:          make(chan interface{}, 4),
		resolvedTsMap:     make(map[SubscriptionID]common.Ts),
		prevResolvedTs:    0,
		pendingResolvedTs: 0,
	}

	// consumeWrapper may be called concurrently
	consumeWrapper := func(ctx context.Context, entry *common.RawKVEntry, subID SubscriptionID) error {
		if entry == nil {
			return nil
		}
		if entry.IsResolved() {
			pullerWrapper.tryUpdatePendingResolvedTs(entry, subID)
			return nil
		}
		return consume(ctx, entry)
	}

	pullerWrapper.innerPuller = NewLogPuller(client, pdClock, consumeWrapper)
	for _, span := range spans {
		subID := pullerWrapper.innerPuller.Subscribe(span, pullerWrapper.startTs)
		pullerWrapper.resolvedTsMap[subID] = 0
	}
	return pullerWrapper
}

func (p *LogPullerMultiSpan) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return p.sendResolvedTsPeriodically(ctx)
	})
	eg.Go(func() error {
		return p.innerPuller.Run(ctx)
	})
	return eg.Wait()
}

func (p *LogPullerMultiSpan) Close(ctx context.Context) error {
	return p.innerPuller.Close(ctx)
}

// return whether the global resolved ts of all spans is updated
func (p *LogPullerMultiSpan) tryUpdatePendingResolvedTs(entry *common.RawKVEntry, subID SubscriptionID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// FIXME: use priority queue to maintain resolved ts
	ts, ok := p.resolvedTsMap[subID]
	if !ok {
		log.Panic("unknown zubscriptionID, should not happen",
			zap.Uint64("subID", uint64(subID)))
	}
	if ts > common.Ts(entry.CRTs) {
		log.Panic("resolved ts should not fallback",
			zap.Uint64("oldResolvedTs", uint64(ts)),
			zap.Uint64("newResolvedTs", entry.CRTs))
	}
	p.resolvedTsMap[subID] = common.Ts(entry.CRTs)

	currentResolvedTs := common.Ts(math.MaxUint64)
	for _, v := range p.resolvedTsMap {
		if v < currentResolvedTs {
			currentResolvedTs = v
		}
	}
	if currentResolvedTs == math.MaxUint64 {
		log.Panic("should not happen")
	}
	p.pendingResolvedTs = currentResolvedTs
	select {
	case p.notifyCh <- struct{}{}:
	default:
	}
}

func (p *LogPullerMultiSpan) sendResolvedTsPeriodically(ctx context.Context) error {
	trySendResolvedTs := func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.pendingResolvedTs > p.prevResolvedTs {
			p.consume(ctx, &common.RawKVEntry{
				OpType: common.OpTypeResolved,
				CRTs:   common.Ts(p.pendingResolvedTs),
			})
			p.prevResolvedTs = p.pendingResolvedTs
		}
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			trySendResolvedTs()
		case <-p.notifyCh:
			trySendResolvedTs()
		}
	}
}
