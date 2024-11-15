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
	"fmt"
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestDispatcherResolvedTsHeap(t *testing.T) {
	dMap := newDispatcherMap()
	dispatchers := []*dispatcher.Dispatcher{
		{
			ResolvedTs: dispatcher.NewTsItem(1),
		},
		{
			ResolvedTs: dispatcher.NewTsItem(2),
		},
		{
			ResolvedTs: dispatcher.NewTsItem(3),
		},
		{
			ResolvedTs: dispatcher.NewTsItem(4),
		},
		{
			ResolvedTs: dispatcher.NewTsItem(5),
		},
	}
	for _, d := range dispatchers {
		dMap.Set(common.NewDispatcherID(), d)
	}

	for idx, d := range dispatchers {
		fmt.Println(idx, ":", d.ResolvedTs)
	}

	require.Equal(t, 5, dMap.Len())
	require.Equal(t, uint64(1), dMap.GetMinWatermark().ResolvedTs)
	d := dispatchers[0]

	d.ResolvedTs.Set(2)
	require.Equal(t, uint64(2), dMap.GetMinWatermark().ResolvedTs)

	d.ResolvedTs.Set(3)
	require.Equal(t, uint64(2), dMap.GetMinWatermark().ResolvedTs)
}
