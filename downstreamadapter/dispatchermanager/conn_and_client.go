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

package dispatchermanager

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"new_arch/eventpb"

	"github.com/ngaut/log"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

const (
	grpcInitialWindowSize     = (1 << 16) - 1
	grpcInitialConnWindowSize = 1 << 23
	grpcMaxCallRecvMsgSize    = 1 << 28

	rpcMetaFeaturesKey string = "features"
	rpcMetaFeaturesSep string = ","
)

// 这个 grpc 相关的代码最后全部要重新整改过，先糊起来

type ConnAndClientPool struct {
	credential        *security.Credential
	maxStreamsPerConn int

	sync.Mutex
	stores map[string]*connArray // 不同 ip 对应不同的 connArray
}

type ConnAndTableAddrClient struct {
	conn   *grpc.ClientConn
	client eventpb.TableAddr_TableAddrClient
}

func (c *ConnAndTableAddrClient) Release() {
	c.conn.Close()
}

func (c *ConnAndClientPool) newConnAndTableAddrClient(addr string) (*ConnAndTableAddrClient, error) {
	for {
		clientConn, err := c.connect(addr)
		if err != nil {
			log.Error("create new grpc connect failed with addr ", addr, ", err: ", err)
			// for backoff
			if err = util.Hang(context.Background(), time.Second); err != nil {
				return nil, err
			}
		}

		rpc := eventpb.NewTableAddrClient(clientConn)
		ctx := context.Background()
		client, err := rpc.TableAddr(ctx)
		if err != nil {
			log.Error("get rpc table addr error", err)
			return nil, err
		}

		return &ConnAndTableAddrClient{
			conn:   clientConn,
			client: client,
		}, nil
	}

}

// 开一个新的 grpc conn 出来
func (c *ConnAndClientPool) connect(target string) (*grpc.ClientConn, error) {
	grpcTLSOption, err := c.credential.ToGRPCDialOption()
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
	}

	return grpc.NewClient(target, dialOptions...)
}

type connArray struct {
	pool         *ConnAndClientPool
	addr         string
	inConnecting atomic.Bool

	sync.Mutex
	conns []*Conn
}

func (c *connArray) push(conn *Conn) {
	c.Lock()
	defer c.Unlock()

	c.conns = append(c.conns, conn)
	c.sort()
}
func (c *connArray) sort() {
	c.Lock()
	defer c.Unlock()
	sort.Slice(c.conns, func(i, j int) bool {
		return c.conns[i].streams < c.conns[j].streams
	})
}

func (c *connArray) release(conn *Conn) {
	conn.streams -= 1
	if conn.streams == 0 {
		for i := range c.conns {
			if c.conns[i] == conn {
				c.conns[i] = c.conns[len(c.conns)-1]
				c.conns = c.conns[:len(c.conns)-1]
				break
			}
		}
		if len(c.conns) == 0 {
			c.pool.Lock()
			delete(c.pool.stores, c.addr)
			c.pool.Unlock()
		}
		_ = conn.ClientConn.Close()
	}
	c.sort()
}

func (c *connArray) connect() (conn *Conn, err error) {
	if c.inConnecting.CompareAndSwap(false, true) {
		defer c.inConnecting.Store(false) // why?
		var clientConn *grpc.ClientConn
		if clientConn, err = c.pool.connect(c.addr); err != nil {
			return
		}

		rpc := eventpb.NewEventsClient(clientConn)
		ctx := context.Background()
		if _, err = rpc.EventFeed(ctx); err != nil {
			log.Error("get rpc event feed error", err)
			return
		}

		conn = new(Conn)
		conn.ClientConn = clientConn

	}
	return
}

// 这个是最基础的 connection
type Conn struct {
	*grpc.ClientConn
	streams int
}

// ConnAndClient indicates a connection and a eventfeed client.
type ConnAndClient struct {
	conn   *Conn
	array  *connArray
	client eventpb.Events_EventFeedClient
	closed atomic.Bool
}

func (c *ConnAndClient) Release() {
	if c.client != nil && !c.closed.Load() {
		_ = c.client.CloseSend()
		c.closed.Store(true)
	}
	if c.conn != nil && c.array != nil {
		c.array.release(c.conn)
		c.conn = nil
		c.array = nil
	}
}

func newConnAndClientPool(
	credential *security.Credential,
	maxStreamsPerConn int,
) *ConnAndClientPool {
	stores := make(map[string]*connArray, 64) // ？
	return &ConnAndClientPool{
		credential:        credential,
		maxStreamsPerConn: maxStreamsPerConn,
		stores:            stores,
	}
}

func (c *ConnAndClientPool) Connect(addr string) (cc *ConnAndClient, err error) {
	var conns *connArray
	c.Lock()
	if conns = c.stores[addr]; conns == nil {
		conns = &connArray{pool: c, addr: addr}
		c.stores[addr] = conns
	}
	c.Unlock()

	for {
		conns.Lock()
		if len(conns.conns) > 0 && conns.conns[0].streams < c.maxStreamsPerConn {
			// conns 里有 conn，并且还没超过最大开的 stream 数目，就复用
			break
		}
		conns.Unlock()

		// 开一个新的 conn
		var conn *Conn
		if conn, err = conns.connect(); err != nil {
			return
		}
		if conn != nil {
			conns.Lock()
			conns.push(conn) // 插入并且排序，让 stream 最少的 conn 拍最前面
			break
		}
		// for backoff
		if err = util.Hang(context.Background(), time.Second); err != nil {
			return
		}
	}

	cc = &ConnAndClient{conn: conns.conns[0], array: conns}
	cc.conn.streams += 1
	defer func() {
		conns.Unlock()
		if err != nil && cc != nil {
			cc.Release()
			cc = nil
		}
	}()

	rpc := eventpb.NewEventsClient(cc.conn.ClientConn)
	cc.client, err = rpc.EventFeed(context.Background())
	return
}
