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
	"golang.org/x/sync/errgroup"
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
	client *SharedClient,
	spans []common.TableSpan,
	startTs common.Ts,
	consume func(context.Context, *common.RawKVEntry) error,
	config *LogPullerConfig,
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

	consumeWrapper := func(ctx context.Context, entry *common.RawKVEntry, span heartbeatpb.TableSpan) error {
		if entry == nil {
			return nil
		}
		if entry.IsResolved() {
			if pullerWrapper.tryUpdateGlobalResolvedTs(entry, span) {
				return consume(ctx, entry)
			}
			return nil
		}
		return consume(ctx, entry)
	}

	pullerWrapper.innerPuller = NewLogPuller(client, consumeWrapper, config)
	return pullerWrapper
}

func (p *LogPullerMultiSpan) Run(ctx context.Context) error {
	p.mu.Lock()
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return p.innerPuller.Run(ctx) })
	p.spanResolvedTsMap.Range(func(span heartbeatpb.TableSpan, ts common.Ts) bool {
		p.innerPuller.Subscribe(span, p.resolvedTs)
		return true
	})
	p.mu.Unlock()
	return eg.Wait()
}

// return whether the global resolved ts of all spans is updated
func (p *LogPullerMultiSpan) tryUpdateGlobalResolvedTs(entry *common.RawKVEntry, span heartbeatpb.TableSpan) bool {
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
		return true
	}
	return false
}
