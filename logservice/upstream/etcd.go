package upstream

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/tikv/pd/pkg/errs"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientV3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

// The following code is mainly copied from:
// https://github.com/tikv/pd/blob/master/pkg/utils/etcdutil/etcdutil.go
const (
	// defaultEtcdClientTimeout is the default timeout for etcd client.
	defaultEtcdClientTimeout = 5 * time.Second
	// defaultDialKeepAliveTime is the time after which client pings the server to see if transport is alive.
	defaultDialKeepAliveTime = 10 * time.Second
	// defaultDialKeepAliveTimeout is the time that the client waits for a response for the
	// keep-alive probe. If the response is not received in this time, the connection is closed.
	defaultDialKeepAliveTimeout = 3 * time.Second
	// etcdServerOfflineTimeout is the timeout for an unhealthy etcd endpoint to be offline from healthy checker.
	etcdServerOfflineTimeout = 30 * time.Minute
	// etcdServerDisconnectedTimeout is the timeout for an unhealthy etcd endpoint to be disconnected from healthy checker.
	etcdServerDisconnectedTimeout = 1 * time.Minute

	etcdClientTimeoutDuration = 30 * time.Second
	// healthyPath is the path to check etcd health.
	healthyPath = "health"
)

func newClient(tlsConfig *tls.Config, grpcDialOption grpc.DialOption, endpoints ...string) (*clientV3.Client, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("empty endpoints")
	}
	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)

	lgc := zap.NewProductionConfig()
	lgc.Encoding = log.ZapEncodingName
	client, err := clientV3.New(clientV3.Config{
		Endpoints:            endpoints,
		TLS:                  tlsConfig,
		LogConfig:            &logConfig,
		DialTimeout:          defaultEtcdClientTimeout,
		DialKeepAliveTime:    defaultDialKeepAliveTime,
		DialKeepAliveTimeout: defaultDialKeepAliveTimeout,
		DialOptions: []grpc.DialOption{
			grpcDialOption,
			grpc.WithBlock(),
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
				Time:    10 * time.Second,
				Timeout: 20 * time.Second,
			}),
		},
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return client, nil
}

// CreateRawEtcdClient creates etcd v3 client with detecting endpoints.
// It will check the health of endpoints periodically, and update endpoints if needed.
func CreateRawEtcdClient(securityConf *security.Credential, grpcDialOption grpc.DialOption, endpoints ...string) (*clientV3.Client, error) {
	log.Info("create etcdCli", zap.Strings("endpoints", endpoints))

	tlsConfig, err := securityConf.ToTLSConfig()
	if err != nil {
		return nil, err
	}

	client, err := newClient(tlsConfig, grpcDialOption, endpoints...)
	if err != nil {
		return nil, err
	}

	tickerInterval := defaultDialKeepAliveTime

	checker := &healthyChecker{
		tlsConfig:      tlsConfig,
		grpcDialOption: grpcDialOption,
	}
	eps := syncUrls(client)
	checker.update(eps)

	// Create a goroutine to check the health of etcd endpoints periodically.
	go func(client *clientV3.Client) {
		ticker := time.NewTicker(tickerInterval)
		defer ticker.Stop()
		lastAvailable := time.Now()
		for {
			select {
			case <-client.Ctx().Done():
				log.Info("etcd client is closed, exit health check goroutine")
				checker.Range(func(key, value interface{}) bool {
					client := value.(*healthyClient)
					client.Close()
					return true
				})
				return
			case <-ticker.C:
				usedEps := client.Endpoints()
				healthyEps := checker.patrol(client.Ctx())
				if len(healthyEps) == 0 {
					// when all endpoints are unhealthy, try to reset endpoints to update connect
					// rather than delete them to avoid there is no any endpoint in client.
					// Note: reset endpoints will trigger subconn closed, and then trigger reconnect.
					// otherwise, the subconn will be retrying in grpc layer and use exponential backoff,
					// and it cannot recover as soon as possible.
					if time.Since(lastAvailable) > etcdServerDisconnectedTimeout {
						log.Info("no available endpoint, try to reset endpoints", zap.Strings("lastEndpoints", usedEps))
						client.SetEndpoints([]string{}...)
						client.SetEndpoints(usedEps...)
					}
				} else {
					if !util.AreStringSlicesEquivalent(healthyEps, usedEps) {
						client.SetEndpoints(healthyEps...)
						change := fmt.Sprintf("%d->%d", len(usedEps), len(healthyEps))
						log.Info("update endpoints", zap.String("numChange", change),
							zap.Strings("lastEndpoints", usedEps), zap.Strings("endpoints", client.Endpoints()))
					}
					lastAvailable = time.Now()
				}
			}
		}
	}(client)

	// Notes: use another goroutine to update endpoints to avoid blocking health check in the first goroutine.
	go func(client *clientV3.Client) {
		ticker := time.NewTicker(tickerInterval)
		defer ticker.Stop()
		for {
			select {
			case <-client.Ctx().Done():
				log.Info("etcd client is closed, exit update endpoint goroutine")
				return
			case <-ticker.C:
				eps := syncUrls(client)
				checker.update(eps)
			}
		}
	}(client)

	return client, nil
}

type healthyClient struct {
	*clientV3.Client
	lastHealth time.Time
}

type healthyChecker struct {
	sync.Map       // map[string]*healthyClient
	tlsConfig      *tls.Config
	grpcDialOption grpc.DialOption
}

func (checker *healthyChecker) patrol(ctx context.Context) []string {
	// See https://github.com/etcd-io/etcd/blob/85b640cee793e25f3837c47200089d14a8392dc7/etcdctl/ctlv3/command/ep_command.go#L105-L145
	var wg sync.WaitGroup
	count := 0
	checker.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	hch := make(chan string, count)
	healthyList := make([]string, 0, count)
	checker.Range(func(key, value interface{}) bool {
		wg.Add(1)
		go func(key, value interface{}) {
			defer wg.Done()
			ep := key.(string)
			client := value.(*healthyClient)
			if IsHealthy(ctx, client.Client) {
				hch <- ep
				checker.Store(ep, &healthyClient{
					Client:     client.Client,
					lastHealth: time.Now(),
				})
				return
			}
		}(key, value)
		return true
	})
	wg.Wait()
	close(hch)
	for h := range hch {
		healthyList = append(healthyList, h)
	}
	return healthyList
}

func (checker *healthyChecker) update(eps []string) {
	for _, ep := range eps {
		// check if client exists, if not, create one, if exists, check if it's offline or disconnected.
		if client, ok := checker.Load(ep); ok {
			lastHealthy := client.(*healthyClient).lastHealth
			if time.Since(lastHealthy) > etcdServerOfflineTimeout {
				log.Info("some etcd server maybe offline", zap.String("endpoint", ep))
				checker.Delete(ep)
			}
			if time.Since(lastHealthy) > etcdServerDisconnectedTimeout {
				// try to reset client endpoint to trigger reconnect
				client.(*healthyClient).Client.SetEndpoints([]string{}...)
				client.(*healthyClient).Client.SetEndpoints(ep)
			}
			continue
		}
		checker.addClient(ep, time.Now())
	}
}

func (checker *healthyChecker) addClient(ep string, lastHealth time.Time) {
	client, err := newClient(checker.tlsConfig, checker.grpcDialOption, ep)
	if err != nil {
		log.Error("failed to create etcd healthy client", zap.Error(err))
		return
	}
	checker.Store(ep, &healthyClient{
		Client:     client,
		lastHealth: lastHealth,
	})
}

func syncUrls(client *clientV3.Client) []string {
	// See https://github.com/etcd-io/etcd/blob/85b640cee793e25f3837c47200089d14a8392dc7/clientv3/client.go#L170-L183
	ctx, cancel := context.WithTimeout(clientV3.WithRequireLeader(client.Ctx()),
		etcdClientTimeoutDuration)
	defer cancel()
	mresp, err := client.MemberList(ctx)
	if err != nil {
		log.Error("failed to list members", errs.ZapError(err))
		return []string{}
	}
	var eps []string
	for _, m := range mresp.Members {
		if len(m.Name) != 0 && !m.IsLearner {
			eps = append(eps, m.ClientURLs...)
		}
	}
	return eps
}

// IsHealthy checks if the etcd is healthy.
func IsHealthy(ctx context.Context, client *clientV3.Client) bool {
	timeout := etcdClientTimeoutDuration
	ctx, cancel := context.WithTimeout(clientV3.WithRequireLeader(ctx), timeout)
	defer cancel()
	_, err := client.Get(ctx, healthyPath)
	// permission denied is OK since proposal goes through consensus to get it
	// See: https://github.com/etcd-io/etcd/blob/85b640cee793e25f3837c47200089d14a8392dc7/etcdctl/ctlv3/command/ep_command.go#L124
	return err == nil || err == rpctypes.ErrPermissionDenied
}
