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

package schemastore

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/utils/heap"
	"github.com/stretchr/testify/require"
)

func TestAdvanceSchemaStoreResolvedTs(t *testing.T) {
	tsCh := make(chan uint64, 100)
	advanceResolvedTs := func(resolvedTS uint64) {
		tsCh <- resolvedTS
	}
	ddlJobFetcher := &ddlJobFetcher{
		advanceResolvedTs: advanceResolvedTs,
	}
	ddlJobFetcher.resolvedTsTracker.resolvedTsItemMap = make(map[logpuller.SubscriptionID]*resolvedTsItem)
	ddlJobFetcher.resolvedTsTracker.resolvedTsHeap = heap.NewHeap[*resolvedTsItem]()

	addSubscription := func(subID logpuller.SubscriptionID) {
		item := &resolvedTsItem{
			resolvedTs: 0,
		}
		ddlJobFetcher.resolvedTsTracker.resolvedTsItemMap[subID] = item
		ddlJobFetcher.resolvedTsTracker.resolvedTsHeap.AddOrUpdate(item)
	}
	subID1 := logpuller.SubscriptionID(100)
	addSubscription(subID1)
	subID2 := logpuller.SubscriptionID(101)
	addSubscription(subID2)

	{
		ddlJobFetcher.tryAdvanceResolvedTs(subID1, 100)
		ddlJobFetcher.tryAdvanceResolvedTs(subID1, 200)

		select {
		case ts := <-tsCh:
			require.Equal(t, uint64(0), ts)
		case <-time.NewTimer(100 * time.Millisecond).C:
			require.True(t, false, "must get an event")
		}
		select {
		case ts := <-tsCh:
			require.Equal(t, uint64(0), ts)
		case <-time.NewTimer(100 * time.Millisecond).C:
			require.True(t, false, "must get an event")
		}
	}

	{
		ddlJobFetcher.tryAdvanceResolvedTs(subID2, 100)

		select {
		case ts := <-tsCh:
			require.Equal(t, uint64(100), ts)
		case <-time.NewTimer(100 * time.Millisecond).C:
			require.True(t, false, "must get an event")
		}
	}

	{
		ddlJobFetcher.tryAdvanceResolvedTs(subID2, 300)

		select {
		case ts := <-tsCh:
			require.Equal(t, uint64(200), ts)
		case <-time.NewTimer(100 * time.Millisecond).C:
			require.True(t, false, "must get an event")
		}
	}

	{
		ddlJobFetcher.tryAdvanceResolvedTs(subID2, 400)

		select {
		case ts := <-tsCh:
			require.Equal(t, uint64(200), ts)
		case <-time.NewTimer(100 * time.Millisecond).C:
			require.True(t, false, "must get an event")
		}
	}

	{
		ddlJobFetcher.tryAdvanceResolvedTs(subID1, 300)

		select {
		case ts := <-tsCh:
			require.Equal(t, uint64(300), ts)
		case <-time.NewTimer(100 * time.Millisecond).C:
			require.True(t, false, "must get an event")
		}
	}
}
