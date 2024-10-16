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

package scheduler

import (
	"testing"

	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestCheckBalanceStatus(t *testing.T) {
	// 4 tasks assigned 2 servers,and an empty server
	require.Equal(t, 0, checkBalanceStatus(map[node.ID]int{
		"node1": 2,
		"node2": 2,
	}, map[node.ID]*node.Info{
		"node1": {ID: "node1"},
		"node2": {ID: "node2"},
		"node3": {ID: "node3"},
	}))

	require.Equal(t, 1, checkBalanceStatus(map[node.ID]int{
		"node1": 2,
		"node2": 0,
	}, map[node.ID]*node.Info{
		"node1": {ID: "node1"},
		"node2": {ID: "node2"},
	}))
}
