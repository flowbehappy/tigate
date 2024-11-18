package logpuller

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/tiflow/pkg/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
)

// StatusIsEOF checks whether status is caused by client send closing.
func StatusIsEOF(status *grpcstatus.Status) bool {
	return status == nil ||
		status.Code() == grpccodes.Canceled ||
		(status.Code() == grpccodes.Unknown && status.Message() == io.EOF.Error())
}

const (
	grpcInitialWindowSize     = (1 << 16) - 1
	grpcInitialConnWindowSize = 1 << 23
	grpcMaxCallRecvMsgSize    = 1 << 28

	rpcMetaFeaturesKey string = "features"
	rpcMetaFeaturesSep string = ","

	// this feature supports these interactions with TiKV sides:
	// 1. in one GRPC stream, TiKV will merge resolved timestamps into several buckets based on
	//    `RequestId`s. For example, region 100 and 101 have been subscribed twice with `RequestId`
	//    1 and 2, TiKV will sends a ResolvedTs message
	//    [{"RequestId": 1, "regions": [100, 101]}, {"RequestId": 2, "regions": [100, 101]}]
	//    to the TiCDC client.
	// 2. TiCDC can deregister all regions with a same request ID by specifying the `RequestId`.
	rpcMetaFeatureStreamMultiplexing string = "stream-multiplexing"
)

// `createGRPCConn` return a grpc connection to `target` but no IO is performed.
// Use of the `ClientConn` for RPCs will automatically cause it to connect.
func createGRPCConn(ctx context.Context, credential *security.Credential, target string) (*grpc.ClientConn, error) {
	grpcTLSOption, err := credential.ToGRPCDialOption()
	if err != nil {
		return nil, err
	}

	dialOptions := []grpc.DialOption{
		grpcTLSOption,
		grpc.WithInitialWindowSize(grpcInitialWindowSize),
		grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(grpcMaxCallRecvMsgSize)),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  time.Second,
				Multiplier: 1.1,
				Jitter:     0.1,
				MaxDelay:   3 * time.Second,
			},
			MinConnectTimeout: 3 * time.Second,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
		//experimental.WithRecvBufferPool(grpc.NewSharedBufferPool()),
	}

	grpcMetrics := metrics.GetGlobalGrpcMetrics()
	if grpcMetrics != nil {
		dialOptions = append(dialOptions, grpc.WithUnaryInterceptor(grpcMetrics.UnaryClientInterceptor()))
		dialOptions = append(dialOptions, grpc.WithStreamInterceptor(grpcMetrics.StreamClientInterceptor()))
	}

	return grpc.DialContext(ctx, target, dialOptions...)

	//return grpc.NewClient(target, dialOptions...)
}

func getContextFromFeatures(ctx context.Context, features []string) context.Context {
	return metadata.NewOutgoingContext(
		ctx,
		metadata.New(map[string]string{
			rpcMetaFeaturesKey: strings.Join(features, rpcMetaFeaturesSep),
		}),
	)
}

type ConnAndClient struct {
	Conn   *grpc.ClientConn
	Client cdcpb.ChangeData_EventFeedV2Client
}

// `Connect` returns a connection and client to remote store.
func Connect(ctx context.Context, credential *security.Credential, target string) (*ConnAndClient, error) {
	clientConn, err := createGRPCConn(ctx, credential, target)
	if err != nil {
		return nil, err
	}

	rpc := cdcpb.NewChangeDataClient(clientConn)
	ctx = getContextFromFeatures(ctx, []string{rpcMetaFeatureStreamMultiplexing})
	client, err := rpc.EventFeedV2(ctx)
	return &ConnAndClient{
		Conn:   clientConn,
		Client: client,
	}, err
}
