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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/logservice/txnutil"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/store/mockstore/mockcopr"
	"github.com/pingcap/tiflow/cdc/kv/regionlock"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	pdClient "github.com/tikv/pd/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func newMockService(
	ctx context.Context,
	t *testing.T,
	server cdcpb.ChangeDataServer,
	wg *sync.WaitGroup,
) (grpcServer *grpc.Server, addr string) {
	return newMockServiceSpecificAddr(ctx, t, server, "127.0.0.1:0", wg)
}

func newMockServiceSpecificAddr(
	ctx context.Context,
	t *testing.T,
	srv cdcpb.ChangeDataServer,
	listenAddr string,
	wg *sync.WaitGroup,
) (grpcServer *grpc.Server, addr string) {
	lc := &net.ListenConfig{}
	lis, err := lc.Listen(ctx, "tcp", listenAddr)
	require.Nil(t, err)
	addr = lis.Addr().String()
	kaep := keepalive.EnforcementPolicy{
		// force minimum ping interval
		MinTime:             3 * time.Second,
		PermitWithoutStream: true,
	}
	// Some tests rely on connect timeout and ping test, so we use a smaller num
	kasp := keepalive.ServerParameters{
		MaxConnectionIdle:     10 * time.Second, // If a client is idle for 20 seconds, send a GOAWAY
		MaxConnectionAge:      10 * time.Second, // If any connection is alive for more than 20 seconds, send a GOAWAY
		MaxConnectionAgeGrace: 5 * time.Second,  // Allow 5 seconds for pending RPCs to complete before forcibly closing connections
		Time:                  3 * time.Second,  // Ping the client if it is idle for 5 seconds to ensure the connection is still active
		Timeout:               1 * time.Second,  // Wait 1 second for the ping ack before assuming the connection is dead
	}
	grpcServer = grpc.NewServer(grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kasp))
	// grpcServer is the server, srv is the service
	cdcpb.RegisterChangeDataServer(grpcServer, srv)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := grpcServer.Serve(lis)
		require.Nil(t, err)
	}()
	return
}

func TestGenerateResolveLockTask(t *testing.T) {
	client := &SubscriptionClient{
		resolveLockTaskCh: make(chan resolveLockTask, 10),
	}
	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}
	span := client.newSubscribedSpan(subscriptionID(1), rawSpan, 100)
	client.totalSpans.spanMap = make(map[subscriptionID]*subscribedSpan)
	client.totalSpans.spanMap[subscriptionID(1)] = span
	client.pdClock = pdutil.NewClock4Test()

	// Lock a range, and then ResolveLock will trigger a task for it.
	res := span.rangeLock.LockRange(context.Background(), []byte{'b'}, []byte{'c'}, 1, 100)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
	res.LockedRangeState.Initialized.Store(true)
	client.ResolveLock(subscriptionID(1), 200)
	select {
	case task := <-client.resolveLockTaskCh:
		require.Equal(t, uint64(1), task.regionID)
		require.Equal(t, uint64(200), task.targetTs)
	case <-time.After(100 * time.Millisecond):
		require.True(t, false, "must get a resolve lock task")
	}

	// Lock another range, no task will be triggered before initialized.
	res = span.rangeLock.LockRange(context.Background(), []byte{'c'}, []byte{'d'}, 2, 100)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
	state := newRegionFeedState(regionInfo{lockedRangeState: res.LockedRangeState, subscribedSpan: span}, 1)
	client.ResolveLock(subscriptionID(1), 200)
	select {
	case task := <-client.resolveLockTaskCh:
		require.Equal(t, uint64(1), task.regionID)
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-client.resolveLockTaskCh:
		require.True(t, false, "shouldn't get a resolve lock task")
	case <-time.After(100 * time.Millisecond):
	}

	// Task will be triggered after initialized.
	state.setInitialized()
	client.ResolveLock(subscriptionID(1), 200)
	select {
	case <-client.resolveLockTaskCh:
	case <-time.After(100 * time.Millisecond):
		require.True(t, false, "must get a resolve lock task")
	}
	select {
	case <-client.resolveLockTaskCh:
	case <-time.After(100 * time.Millisecond):
		require.True(t, false, "must get a resolve lock task")
	}
	require.Equal(t, 0, len(client.resolveLockTaskCh))

	close(client.resolveLockTaskCh)
}

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
		RegionRequestWorkerPerStore:        1,
		ChangeEventProcessorNum:            2,
		AdvanceResolvedTsIntervalInMs:      1, // must be small to pass the test
		RegionIncrementalScanLimitPerStore: 100,
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
	consumeCh := make(chan LogEvent, 50)
	consumeLogEvent := func(ctx context.Context, e LogEvent) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case consumeCh <- e:
			return nil
		}
	}
	go func() {
		defer wg.Done()
		err := client.Run(ctx, consumeLogEvent)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// hack to wait SubscriptionClient.consume is set
	for {
		if client.consume != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	subID := client.AllocSubscriptionID()
	span := heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("b")}
	client.Subscribe(subID, span, 1)

	eventsCh1 <- mockInitializedEvent(11, uint64(subID))
	ts := oracle.GoTimeToTS(pdClock.CurrentTime())
	eventsCh1 <- mockTsEventBatch(11, ts, uint64(subID))
	// After trying to receive something from the invalid store,
	// it should auto switch to other stores and fetch events finally.
	select {
	case event := <-consumeCh:
		require.Equal(t, common.OpTypeResolved, event.regionFeedEvent.Val.OpType)
		require.Equal(t, ts, event.regionFeedEvent.Val.CRTs)
	case <-time.After(5 * time.Second):
		require.True(t, false, "reconnection not succeed in 5 second")
	}

	// Stop server1 and the client needs to handle it.
	server1.Stop()

	eventsCh2 <- mockInitializedEvent(11, uint64(subID))
	ts = oracle.GoTimeToTS(pdClock.CurrentTime())
	eventsCh2 <- mockTsEvent(11, ts, uint64(subID))
	// After trying to receive something from a failed store,
	// it should auto switch to other stores and fetch events finally.
	select {
	case event := <-consumeCh:
		require.Equal(t, common.OpTypeResolved, event.regionFeedEvent.Val.OpType)
		require.Equal(t, ts, event.regionFeedEvent.Val.CRTs)
	case <-time.After(5 * time.Second):
		require.True(t, false, "reconnection not succeed in 5 second")
	}
}

type mockPDClient struct {
	pdClient.Client
	versionGen func() string
}

var _ pdClient.Client = &mockPDClient{}

func (m *mockPDClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	s, err := m.Client.GetStore(ctx, storeID)
	if err != nil {
		return nil, err
	}
	s.Version = m.versionGen()
	return s, nil
}

var defaultVersionGen = func() string {
	return version.MinTiKVVersion.String()
}

func mockInitializedEvent(regionID, requestID uint64) *cdcpb.ChangeDataEvent {
	initialized := &cdcpb.ChangeDataEvent{
		Events: []*cdcpb.Event{
			{
				RegionId:  regionID,
				RequestId: requestID,
				Event: &cdcpb.Event_Entries_{
					Entries: &cdcpb.Event_Entries{
						Entries: []*cdcpb.Event_Row{
							{
								Type: cdcpb.Event_INITIALIZED,
							},
						},
					},
				},
			},
		},
	}
	return initialized
}

func mockTsEvent(regionID, ts, requestID uint64) *cdcpb.ChangeDataEvent {
	return &cdcpb.ChangeDataEvent{
		Events: []*cdcpb.Event{
			{
				RegionId:  regionID,
				RequestId: requestID,
				Event:     &cdcpb.Event_ResolvedTs{ResolvedTs: ts},
			},
		},
	}
}

// TODO: add test for batch ts event
func mockTsEventBatch(regionID, ts, requestID uint64) *cdcpb.ChangeDataEvent {
	return &cdcpb.ChangeDataEvent{
		ResolvedTs: &cdcpb.ResolvedTs{
			Regions:   []uint64{regionID},
			Ts:        ts,
			RequestId: requestID,
		},
	}
}

type mockChangeDataServer struct {
	ch chan *cdcpb.ChangeDataEvent
	wg sync.WaitGroup
}

func newMockChangeDataServer(ch chan *cdcpb.ChangeDataEvent) *mockChangeDataServer {
	return &mockChangeDataServer{ch: ch}
}

func (m *mockChangeDataServer) EventFeed(s cdcpb.ChangeData_EventFeedServer) error {
	closed := make(chan struct{})
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer close(closed)
		for {
			if _, err := s.Recv(); err != nil {
				return
			}
		}
	}()
	m.wg.Add(1)
	defer m.wg.Done()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-closed:
			return nil
		case <-ticker.C:
		}
		select {
		case event := <-m.ch:
			if err := s.Send(event); err != nil {
				return err
			}
		default:
		}
	}
}

func (m *mockChangeDataServer) EventFeedV2(s cdcpb.ChangeData_EventFeedV2Server) error {
	return m.EventFeed(s)
}
