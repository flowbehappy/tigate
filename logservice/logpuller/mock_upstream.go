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

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/stretchr/testify/require"
	pdClient "github.com/tikv/pd/client"
	"go.uber.org/zap"
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
	server cdcpb.ChangeDataServer,
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
	cdcpb.RegisterChangeDataServer(grpcServer, server)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := grpcServer.Serve(lis)
		require.Nil(t, err)
	}()
	return
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
			log.Info("mock server send event", zap.Any("event", event))
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
