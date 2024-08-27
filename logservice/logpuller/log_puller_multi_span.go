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

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/pdutil"
)

// LogPullerMultiSpan is a simple wrapper around LogPuller.
// It just maintain the minimum resolve ts of all spans.
// TODO: may be we can implement it in other way(not as a xxxPuller)?
type LogPullerMultiSpan struct {
	innerPuller *LogPuller

	mu                sync.Mutex
	spanResolvedTsMap *common.SpanHashMap[common.Ts]
	// resolvedTs is the minimum resolved ts of all spans.
	resolvedTs common.Ts
}

func NewLogPullerMultiSpan(
	client *SubscriptionClient,
	pdClock pdutil.Clock,
	spans []common.TableSpan,
	startTs common.Ts,
	consume func(context.Context, *common.RawKVEntry) error,
) *LogPullerMultiSpan {
	// TODO: remove this when we use a priority queue to maintain the resolved ts.(just check at least one span)
	if len(spans) != 2 {
		log.Panic("not supported")
	}
	spanResolvedTsMap := common.NewSpanHashMap[common.Ts]()
	for _, span := range spans {
		spanResolvedTsMap.ReplaceOrInsert(*span.TableSpan, 0)
	}
	pullerWrapper := &LogPullerMultiSpan{
		spanResolvedTsMap: spanResolvedTsMap,
		resolvedTs:        common.Ts(startTs),
	}

	// consumeWrapper may be called concurrently
	consumeWrapper := func(ctx context.Context, entry *common.RawKVEntry, span heartbeatpb.TableSpan) error {
		if entry == nil {
			return nil
		}
		if entry.IsResolved() {
			if ts := pullerWrapper.tryUpdateGlobalResolvedTs(entry, span); ts != 0 {
				return consume(ctx, &common.RawKVEntry{
					OpType: common.OpTypeResolved,
					CRTs:   uint64(ts),
				})
			}
			return nil
		}
		return consume(ctx, entry)
	}

	pullerWrapper.innerPuller = NewLogPuller(client, pdClock, consumeWrapper)
	pullerWrapper.spanResolvedTsMap.Range(func(span heartbeatpb.TableSpan, ts common.Ts) bool {
		pullerWrapper.innerPuller.Subscribe(span, pullerWrapper.resolvedTs)
		return true
	})
	return pullerWrapper
}

func (p *LogPullerMultiSpan) Run(ctx context.Context) error {
	return p.innerPuller.Run(ctx)
}

func (p *LogPullerMultiSpan) Close(ctx context.Context) error {
	return p.innerPuller.Close(ctx)
}

// return whether the global resolved ts of all spans is updated
func (p *LogPullerMultiSpan) tryUpdateGlobalResolvedTs(entry *common.RawKVEntry, span heartbeatpb.TableSpan) common.Ts {
	p.mu.Lock()
	defer p.mu.Unlock()
	// FIXME: use priority queue to maintain resolved ts
	ts, ok := p.spanResolvedTsMap.Get(span)
	if !ok {
		log.Panic("unknown span, should not happen")
	}
	if ts > common.Ts(entry.CRTs) {
		log.Panic("resolved ts should not fallback")
	}
	p.spanResolvedTsMap.ReplaceOrInsert(span, common.Ts(entry.CRTs))

	currentResolvedTs := common.Ts(0)
	p.spanResolvedTsMap.Range(func(_ heartbeatpb.TableSpan, v common.Ts) bool {
		if currentResolvedTs == 0 || v < currentResolvedTs {
			currentResolvedTs = v
		}
		return true
	})
	// currentResolvedTs may be 0 if some span have not received resolved ts yet
	if p.resolvedTs < currentResolvedTs {
		p.resolvedTs = currentResolvedTs
		return p.resolvedTs
	}
	return 0
}
