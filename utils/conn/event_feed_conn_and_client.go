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

package conn

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flowbehappy/tigate/eventpb"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util"
	"google.golang.org/grpc"
)

type EventFeedConnArray struct {
	pool         *EventFeedConnAndClientPool
	addr         string
	inConnecting atomic.Bool

	sync.Mutex
	conns []*Conn
}

func (c *EventFeedConnArray) push(conn *Conn) {
	c.Lock()
	defer c.Unlock()

	c.conns = append(c.conns, conn)
	c.sort()
}
func (c *EventFeedConnArray) sort() {
	c.Lock()
	defer c.Unlock()
	sort.Slice(c.conns, func(i, j int) bool {
		return c.conns[i].streams < c.conns[j].streams
	})
}

func (c *EventFeedConnArray) release(conn *Conn) {
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

func (c *EventFeedConnArray) connect() (conn *Conn, err error) {
	if c.inConnecting.CompareAndSwap(false, true) {
		defer c.inConnecting.Store(false) // why?
		var clientConn *grpc.ClientConn
		if clientConn, err = Connect(c.addr, c.pool.credential); err != nil {
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

type EventFeedConnAndClientPool struct {
	credential        *security.Credential
	maxStreamsPerConn int

	sync.Mutex
	stores map[string]*EventFeedConnArray // 不同 ip 对应不同的 connArray
}

func NewEventFeedConnAndClientPool(
	credential *security.Credential,
	maxStreamsPerConn int,
) *EventFeedConnAndClientPool {
	stores := make(map[string]*EventFeedConnArray, 64) // ？
	return &EventFeedConnAndClientPool{
		credential:        credential,
		maxStreamsPerConn: maxStreamsPerConn,
		stores:            stores,
	}
}

func (c *EventFeedConnAndClientPool) Connect(addr string) (cc *EventFeedConnAndClient, err error) {
	var conns *EventFeedConnArray
	c.Lock()
	if conns = c.stores[addr]; conns == nil {
		conns = &EventFeedConnArray{pool: c, addr: addr}
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

	cc = &EventFeedConnAndClient{conn: conns.conns[0], array: conns}
	cc.conn.streams += 1
	defer func() {
		conns.Unlock()
		if err != nil && cc != nil {
			cc.Release()
			cc = nil
		}
	}()

	rpc := eventpb.NewEventsClient(cc.conn.ClientConn)
	cc.Client, err = rpc.EventFeed(context.Background())
	return
}

// ConnAndClient indicates a connection and a eventfeed client.
type EventFeedConnAndClient struct {
	conn   *Conn
	array  *EventFeedConnArray
	Client eventpb.Events_EventFeedClient
	closed atomic.Bool
}

func (c *EventFeedConnAndClient) Release() {
	if c.Client != nil && !c.closed.Load() {
		_ = c.Client.CloseSend()
		c.closed.Store(true)
	}
	if c.conn != nil && c.array != nil {
		c.array.release(c.conn)
		c.conn = nil
		c.array = nil
	}
}
