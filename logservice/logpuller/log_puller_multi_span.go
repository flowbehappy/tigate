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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/utils/heap"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// LogPullerMultiSpan is a simple wrapper around LogPuller.
// It just maintain the minimum resolve ts of all spans.
type LogPullerMultiSpan struct {
	innerPuller *LogPuller

	consume func(context.Context, *common.RawKVEntry) error

	// used to notify the min resolved ts of all spans is updated
	notifyCh chan interface{}

	// following fields are protected by this mutex
	mu sync.Mutex

	resolvedTsMap map[SubscriptionID]*resolvedTsItem

	resolvedTsHeap *heap.Heap[*resolvedTsItem]

	// the resolved ts that have been consumed
	prevResolvedTs uint64

	// the resolved ts pending to be consumed if it is larger than `prevResolvedTs`
	pendingResolvedTs uint64
}

func NewLogPullerMultiSpan(
	client *SubscriptionClient,
	pdClock pdutil.Clock,
	spans []heartbeatpb.TableSpan,
	startTs uint64,
	consume func(context.Context, *common.RawKVEntry) error,
) *LogPullerMultiSpan {
	if len(spans) <= 1 {
		log.Panic("spans should have more than 1 element")
	}
	pullerWrapper := &LogPullerMultiSpan{
		consume:           consume,
		notifyCh:          make(chan interface{}, 4),
		resolvedTsMap:     make(map[SubscriptionID]*resolvedTsItem),
		resolvedTsHeap:    heap.NewHeap[*resolvedTsItem](),
		prevResolvedTs:    0,
		pendingResolvedTs: 0,
	}

	// consumeWrapper may be called concurrently
	consumeWrapper := func(ctx context.Context, entry *common.RawKVEntry, subID SubscriptionID) error {
		if entry.IsResolved() {
			pullerWrapper.tryUpdatePendingResolvedTs(subID, entry.CRTs)
			return nil
		}
		return consume(ctx, entry)
	}

	pullerWrapper.innerPuller = NewLogPuller(client, pdClock, consumeWrapper)
	for _, span := range spans {
		subID := pullerWrapper.innerPuller.Subscribe(span, startTs)
		item := &resolvedTsItem{
			resolvedTs: 0,
		}
		pullerWrapper.resolvedTsMap[subID] = item
		pullerWrapper.resolvedTsHeap.AddOrUpdate(item)
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
func (p *LogPullerMultiSpan) tryUpdatePendingResolvedTs(subID SubscriptionID, newResolvedTs uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	item, ok := p.resolvedTsMap[subID]
	if !ok {
		log.Panic("unknown zubscriptionID, should not happen",
			zap.Uint64("subID", uint64(subID)))
	}
	log.Info("schema store update pending resolved ts",
		zap.Uint64("subID", uint64(subID)),
		zap.Uint64("newResolvedTs", newResolvedTs))
	if newResolvedTs < item.resolvedTs {
		log.Panic("resolved ts should not fallback",
			zap.Uint64("newResolvedTs", newResolvedTs),
			zap.Uint64("oldResolvedTs", item.resolvedTs))
	}
	item.resolvedTs = newResolvedTs
	p.resolvedTsHeap.AddOrUpdate(item)

	minResolvedTsItem, ok := p.resolvedTsHeap.PeekTop()
	if !ok || minResolvedTsItem.resolvedTs == math.MaxUint64 {
		log.Panic("should not happen")
	}
	p.pendingResolvedTs = minResolvedTsItem.resolvedTs
	if p.pendingResolvedTs > p.prevResolvedTs {
		select {
		case p.notifyCh <- struct{}{}:
		default:
		}
	}
}

func (p *LogPullerMultiSpan) sendResolvedTsPeriodically(ctx context.Context) error {
	trySendResolvedTs := func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		// Note: pendingResolvedTs may be 0(which means not all spans have received resolved ts) and it won't be send here
		if p.pendingResolvedTs > p.prevResolvedTs {
			p.consume(ctx, &common.RawKVEntry{
				OpType: common.OpTypeResolved,
				CRTs:   p.pendingResolvedTs,
			})
			p.prevResolvedTs = p.pendingResolvedTs
		}
	}
	ticker := time.NewTicker(100 * time.Millisecond)
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

type resolvedTsItem struct {
	resolvedTs uint64
	heapIndex  int
}

func (m *resolvedTsItem) SetHeapIndex(index int) { m.heapIndex = index }

func (m *resolvedTsItem) GetHeapIndex() int { return m.heapIndex }

func (m *resolvedTsItem) LessThan(other *resolvedTsItem) bool {
	return m.resolvedTs < other.resolvedTs
}
