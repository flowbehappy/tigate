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

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/txnutil"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tidb/pkg/store/mockstore/mockcopr"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"

	"github.com/tikv/client-go/v2/tikv"
)

// func TestGenerateResolveLockTask(t *testing.T) {
// 	client := &SubscriptionClient{
// 		resolveLockTaskCh: make(chan resolveLockTask, 10),
// 	}
// 	rawSpan := heartbeatpb.TableSpan{
// 		TableID:  1,
// 		StartKey: []byte{'a'},
// 		EndKey:   []byte{'z'},
// 	}
// 	span := client.newSubscribedSpan(SubscriptionID(1), rawSpan, 100)
// 	client.totalSpans.spanMap = make(map[SubscriptionID]*subscribedSpan)
// 	client.totalSpans.spanMap[SubscriptionID(1)] = span
// 	client.pdClock = pdutil.NewClock4Test()

// 	// Lock a range, and then ResolveLock will trigger a task for it.
// 	res := span.rangeLock.LockRange(context.Background(), []byte{'b'}, []byte{'c'}, 1, 100)
// 	require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
// 	res.LockedRangeState.Initialized.Store(true)
// 	client.ResolveLock(SubscriptionID(1), 200)
// 	select {
// 	case task := <-client.resolveLockTaskCh:
// 		require.Equal(t, uint64(1), task.regionID)
// 		require.Equal(t, uint64(200), task.targetTs)
// 	case <-time.After(100 * time.Millisecond):
// 		require.True(t, false, "must get a resolve lock task")
// 	}

// 	// Lock another range, no task will be triggered before initialized.
// 	res = span.rangeLock.LockRange(context.Background(), []byte{'c'}, []byte{'d'}, 2, 100)
// 	require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
// 	state := newRegionFeedState(regionInfo{lockedRangeState: res.LockedRangeState, subscribedSpan: span}, 1)
// 	client.ResolveLock(SubscriptionID(1), 200)
// 	select {
// 	case task := <-client.resolveLockTaskCh:
// 		require.Equal(t, uint64(1), task.regionID)
// 	case <-time.After(100 * time.Millisecond):
// 	}
// 	select {
// 	case <-client.resolveLockTaskCh:
// 		require.True(t, false, "shouldn't get a resolve lock task")
// 	case <-time.After(100 * time.Millisecond):
// 	}

// 	// Task will be triggered after initialized.
// 	state.setInitialized()
// 	client.ResolveLock(SubscriptionID(1), 200)
// 	select {
// 	case <-client.resolveLockTaskCh:
// 	case <-time.After(100 * time.Millisecond):
// 		require.True(t, false, "must get a resolve lock task")
// 	}
// 	select {
// 	case <-client.resolveLockTaskCh:
// 	case <-time.After(100 * time.Millisecond):
// 		require.True(t, false, "must get a resolve lock task")
// 	}
// 	require.Equal(t, 0, len(client.resolveLockTaskCh))

// 	close(client.resolveLockTaskCh)
// }

func TestSubscriptionWithFailedTiKV(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	eventsCh1 := make(chan *cdcpb.ChangeDataEvent, 10)
	eventsCh2 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataServer(eventsCh1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)
	srv2 := newMockChangeDataServer(eventsCh2)
	server2, addr2 := newMockService(ctx, t, srv2, wg)

	rpcClient, cluster, pdClient, _ := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())

	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	regionCache := tikv.NewRegionCache(pdClient)
	pdClock := pdutil.NewClock4Test()
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	lockResolver := txnutil.NewLockerResolver(kvStorage)

	invalidStore := "localhost:1"
	cluster.AddStore(1, addr1)
	cluster.AddStore(2, addr2)
	cluster.AddStore(3, invalidStore)
	// bootstrap cluster with a region which leader is in invalid store.
	cluster.Bootstrap(11, []uint64{1, 2, 3}, []uint64{4, 5, 6}, 6)

	clientConfig := &SubscriptionClientConfig{
		RegionRequestWorkerPerStore: 2,
	}
	client := NewSubscriptionClient(
		clientConfig,
		pdClient,
		regionCache,
		pdClock,
		lockResolver,
		&security.Credential{},
	)

	defer func() {
		cancel()
		client.Close(ctx)
		_ = kvStorage.Close()
		regionCache.Close()
		pdClient.Close()
		srv1.wg.Wait()
		srv2.wg.Wait()
		server1.Stop()
		server2.Stop()
		wg.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := client.Run(ctx)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	subID := client.AllocSubscriptionID()
	span := heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("b")}
	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool {
		// should not reach here
		require.True(t, false)
		return false
	}
	tsCh := make(chan uint64, 10)
	advanceResolvedTs := func(ts uint64) {
		select {
		case <-ctx.Done():
		case tsCh <- ts:
		}
	}
	client.Subscribe(subID, span, 1, consumeKVEvents, advanceResolvedTs, 0)

	eventsCh1 <- mockInitializedEvent(11, uint64(subID))
	targetTs := oracle.GoTimeToTS(pdClock.CurrentTime())
	eventsCh1 <- mockTsEventBatch(11, targetTs, uint64(subID))
	// After trying to receive something from the invalid store,
	// it should auto switch to other stores and fetch events finally.
	select {
	case resolvedTs := <-tsCh:
		require.Equal(t, targetTs, resolvedTs)
	case <-time.After(5 * time.Second):
		require.True(t, false, "reconnection not succeed in 5 second")
	}

	// Stop server1 and the client needs to handle it.
	server1.Stop()

	eventsCh2 <- mockInitializedEvent(11, uint64(subID))
	targetTs = oracle.GoTimeToTS(pdClock.CurrentTime())
	eventsCh2 <- mockTsEvent(11, targetTs, uint64(subID))
	// After trying to receive something from a failed store,
	// it should auto switch to other stores and fetch events finally.
	select {
	case resolvedTs := <-tsCh:
		require.Equal(t, targetTs, resolvedTs)
	case <-time.After(5 * time.Second):
		require.True(t, false, "reconnection not succeed in 5 second")
	}
}
