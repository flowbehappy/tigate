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
	"testing"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
)

func newLogPullerMultiSpanForTest(spans []common.TableSpan, outputCh chan<- *common.RawKVEntry) *LogPullerMultiSpan {
	clientConfig := &SubscriptionClientConfig{
		RegionRequestWorkerPerStore:   1,
		ChangeEventProcessorNum:       2,
		AdvanceResolvedTsIntervalInMs: 1,
	}
	client := NewSubscriptionClient(clientConfig, nil, nil, nil, nil, &security.Credential{})
	consume := func(ctx context.Context, e *common.RawKVEntry) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case outputCh <- e:
			return nil
		}
	}
	pullerConfig := &LogPullerConfig{
		WorkerCount:  1,
		HashSpanFunc: func(heartbeatpb.TableSpan, int) int { return 0 },
	}

	return NewLogPullerMultiSpan(
		client,
		pdutil.NewClock4Test(),
		spans,
		1, // start ts
		consume,
		pullerConfig,
	)
}

func TestMultiplexingPullerResolvedForward(t *testing.T) {
	ctx := context.Background()
	outputCh := make(chan *common.RawKVEntry, 16)
	rawSpan1 := common.ToSpan([]byte("t_a"), []byte("t_e"))
	rawSpan1.TableID = 100
	span1 := common.TableSpan{
		TableSpan: &rawSpan1,
	}
	rawSpan2 := common.ToSpan([]byte("t_f"), []byte("t_z"))
	rawSpan2.TableID = 101
	span2 := common.TableSpan{
		TableSpan: &rawSpan2,
	}
	puller := newLogPullerMultiSpanForTest([]common.TableSpan{span1, span2}, outputCh)
	defer puller.Close(ctx)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		puller.Run(ctx)
	}()

	allProgress := puller.innerPuller.getAllProgresses()
	subIDs := make([]subscriptionID, 0, len(allProgress))
	for p := range allProgress {
		subIDs = append(subIDs, p.subID)
	}
	require.Equal(t, 2, len(subIDs))
	for i, subID := range subIDs {
		puller.innerPuller.inputChs[0] <- LogEvent{
			regionFeedEvent: regionFeedEvent{
				Val: &common.RawKVEntry{
					OpType: common.OpTypeResolved,
					CRTs:   uint64(1000 + i),
				},
			},
			subscriptionID: subID,
		}
	}

	select {
	case ev := <-outputCh:
		require.Equal(t, common.OpTypeResolved, ev.OpType)
		require.Equal(t, uint64(1000), ev.CRTs)
	case <-time.NewTimer(100 * time.Millisecond).C:
		require.True(t, false, "must get an event")
	}
	cancel()
	wg.Wait()
}
