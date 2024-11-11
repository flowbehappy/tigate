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

package changefeed

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestShouldFailWhenRetry(t *testing.T) {
	backoff := NewBackoff(common.NewChangeFeedIDWithName("test"), time.Minute*30, 1)
	require.True(t, backoff.ShouldRun())
	changefeed, state, err := backoff.CheckStatus(&heartbeatpb.MaintainerStatus{
		CheckpointTs: 1,
		Err: []*heartbeatpb.RunningError{
			{Message: "test"}},
	})
	require.True(t, changefeed)
	require.Equal(t, model.StateWarning, state)
	require.NotNil(t, err)
	require.False(t, backoff.ShouldRun())
	require.True(t, backoff.isRestarting.Load())

	// advance checkpointTs
	changefeed, state, err = backoff.CheckStatus(&heartbeatpb.MaintainerStatus{
		CheckpointTs: 2,
	})

	require.True(t, changefeed)
	require.Equal(t, model.StateNormal, state)
	require.Nil(t, err)
	require.True(t, backoff.ShouldRun())
	require.False(t, backoff.isRestarting.Load())

	//fail
	changefeed, state, err = backoff.CheckStatus(&heartbeatpb.MaintainerStatus{
		CheckpointTs: 1,
		Err: []*heartbeatpb.RunningError{
			{Message: "test"},
			{Code: "CDC:ErrSnapshotLostByGC", Message: "snapshot lost by gc"}},
	})

	require.True(t, changefeed)
	require.Equal(t, model.StateFailed, state)
	require.NotNil(t, err)
	require.False(t, backoff.ShouldRun())
	require.True(t, backoff.isRestarting.Load())
}
