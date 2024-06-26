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

package maintainer

import (
	"github.com/flowbehappy/tigate/rpc"
	"github.com/flowbehappy/tigate/scheduler"
)

// Manager is the manager of all changefeed maintainer in a ticdc node, each ticdc node will
// start a Manager when the node is startup. the Manager should:
// 1. handle bootstrap command from coordinator and return all changefeed maintainer status
// 2. handle dispatcher command from coordinator: add or remove changefeed maintainer
// 3. check maintainer liveness
type Manager struct {
	rpcClient  rpc.RpcClient
	supervisor *scheduler.Supervisor
	scheduler  scheduler.Scheduler
}

// NewManager create a changefeed maintainer instance
func NewManager(rpcClient rpc.RpcClient) *Manager {
	return &Manager{
		rpcClient: rpcClient,
	}
}
