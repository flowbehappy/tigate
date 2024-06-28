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
	"time"

	"github.com/flowbehappy/tigate/eventpb"

	"github.com/ngaut/log"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util"
	"google.golang.org/grpc"
)

type TableAddrConnAndClient struct {
	conn   *grpc.ClientConn
	Client eventpb.TableAddr_TableAddrClient
}

func (c *TableAddrConnAndClient) Release() {
	c.conn.Close()
}

func NewConnAndTableAddrClient(addr string, credential *security.Credential) (*TableAddrConnAndClient, error) {
	for {
		clientConn, err := connect(addr, credential)
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

		return &TableAddrConnAndClient{
			conn:   clientConn,
			Client: client,
		}, nil
	}

}
