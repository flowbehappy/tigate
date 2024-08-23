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

package logpuller

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegionStatesOperation(t *testing.T) {
	worker := &regionRequestWorker{}
	worker.requestedRegions.subscriptions = make(map[subscriptionID]regionFeedStates)

	require.Nil(t, worker.getRegionState(1, 2))
	require.Nil(t, worker.takeRegionState(1, 2))

	worker.addRegionState(1, 2, &regionFeedState{})
	require.NotNil(t, worker.getRegionState(1, 2))
	require.NotNil(t, worker.takeRegionState(1, 2))
	require.Nil(t, worker.getRegionState(1, 2))
	require.Equal(t, 0, len(worker.requestedRegions.subscriptions))

	worker.addRegionState(1, 2, &regionFeedState{})
	require.NotNil(t, worker.getRegionState(1, 2))
	require.NotNil(t, worker.takeRegionState(1, 2))
	require.Nil(t, worker.getRegionState(1, 2))
	require.Equal(t, 0, len(worker.requestedRegions.subscriptions))
}
