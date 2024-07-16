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

package server

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/etcd"
	pd "github.com/tikv/pd/client"
)

// Server represents a server, it monitors the changefeed
// information in etcd and schedules Task on it.
type Server interface {
	Run(ctx context.Context) error
	Close(ctx context.Context)

	SelfInfo() (*model.CaptureInfo, error)
	Liveness() model.Liveness

	GetCoordinator() (Coordinator, error)
	IsCoordinator() bool

	// GetCoordinatorInfo returns the coordinator server， it will be used when forward api request
	GetCoordinatorInfo(ctx context.Context) (*model.CaptureInfo, error)

	GetPdClient() pd.Client
	GetEtcdClient() etcd.CDCEtcdClient
}
