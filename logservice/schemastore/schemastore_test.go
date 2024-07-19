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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/flowbehappy/tigate/logservice/upstream"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

// TODO: this is not a really unit test, more like a usage example.
// 1. deploy a tidb cluster
// 2. set global tidb_gc_life_time="100h";
// 3. begin; select @@tidb_current_ts; and set it to gcTs
// 4. create some tables;
// 3. begin; select @@tidb_current_ts; and set it to snapTs;
func TestBasicDDLJob(t *testing.T) {
	ctx := context.Background()
	upstreamManager := upstream.NewManager(ctx)
	pdEndpoints := []string{"http://127.0.0.1:2379"}
	pdClient, err := pd.NewClientWithContext(
		ctx, pdEndpoints, pd.SecurityOption{},
		// the default `timeout` is 3s, maybe too small if the pd is busy,
		// set to 10s to avoid frequent timeout.
		pd.WithCustomTimeoutOption(10*time.Second),
		pd.WithGRPCDialOptions(
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
		))
	require.Nil(t, err)

	etcdCli, err := upstream.CreateRawEtcdClient(&security.Credential{}, grpc.WithTransportCredentials(insecure.NewCredentials()), pdEndpoints...)
	require.Nil(t, err)

	upstream, err := upstreamManager.AddDefaultUpstream(pdEndpoints, &security.Credential{}, pdClient, etcdCli)
	require.Nil(t, err)

	schemaStore, err := NewSchemaStore(ctx, "/tmp/cdc", upstream.PDClient, upstream.RegionCache, upstream.PDClock, upstream.KVStorage)
	require.Nil(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go schemaStore.Run(ctx)
	time.Sleep(3 * time.Second)
	phy, logic, err := upstream.PDClient.GetTS(ctx)
	require.Nil(t, err)
	snapTs := oracle.ComposeTS(phy, logic)
	tables, err := schemaStore.GetAllPhysicalTables(common.Ts(snapTs))
	require.Nil(t, err)
	log.Info("schema store get all tables", zap.Any("tables", tables))
	fmt.Printf("all tables")
}
