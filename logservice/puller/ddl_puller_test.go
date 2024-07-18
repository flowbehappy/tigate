package puller

import (
	"context"
	"testing"
	"time"

	"github.com/flowbehappy/tigate/logservice/schemastore"
	"github.com/flowbehappy/tigate/logservice/upstream"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

// TODO: this is not a really unit test, more like a usage example.
// 1. deploy a tidb cluster
// 2. set global tidb_gc_life_time="100h";
// 3. begin; select @@tidb_current_ts; and set it to gcTS and checkpointTS;
func TestBasic(t *testing.T) {
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

	gcTS := uint64(451108303014723585) // FIXME every time run
	schemaStore, _, err := schemastore.NewSchemaStore("/tmp/cdc", upstream.KVStorage, schemastore.Timestamp(gcTS))
	require.Nil(t, err)
	_ = NewDDLJobPuller(ctx, upstream, gcTS, schemaStore)
}
