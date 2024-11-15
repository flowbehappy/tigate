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

	"github.com/benbjohnson/clock"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestRetry(t *testing.T) {
	backoff := NewBackoff(common.NewChangeFeedIDWithName("test"), time.Minute*30, 1)
	require.True(t, backoff.ShouldRun())

	// stop the backoff
	mc := clock.NewMock()
	mc.Set(time.Now())
	backoff.errBackoff.Clock = mc
	mc.Add(backoff.errBackoff.MaxElapsedTime + 1)

	changefeed, state, err := backoff.CheckStatus(&heartbeatpb.MaintainerStatus{
		CheckpointTs: 1,
		Err: []*heartbeatpb.RunningError{
			{Message: "test"}},
	})
	require.True(t, changefeed)
	require.Equal(t, model.StateWarning, state)
	require.NotNil(t, err)
	require.False(t, backoff.ShouldRun())
	require.True(t, backoff.retrying.Load())

	// advance checkpointTs
	changefeed, state, err = backoff.CheckStatus(&heartbeatpb.MaintainerStatus{
		CheckpointTs: 2,
	})

	require.True(t, changefeed)
	require.Equal(t, model.StateNormal, state)
	require.Nil(t, err)
	require.True(t, backoff.ShouldRun())
	require.False(t, backoff.retrying.Load())

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
	require.False(t, backoff.retrying.Load())

	// failed changefeed
	changefeed, state, err = backoff.CheckStatus(&heartbeatpb.MaintainerStatus{
		CheckpointTs: 3,
	})
	require.False(t, changefeed)
	require.Equal(t, model.StateFailed, state)
	require.Equal(t, uint64(2), backoff.checkpointTs)

	backoff.resetErrRetry()
	require.True(t, backoff.ShouldRun())
	require.False(t, backoff.retrying.Load())

	// not advance checkpointTs
	changefeed, state, err = backoff.CheckStatus(&heartbeatpb.MaintainerStatus{
		CheckpointTs: 2,
	})

	require.False(t, changefeed)
	require.Equal(t, model.StateNormal, state)
	require.Nil(t, err)
	require.True(t, backoff.ShouldRun())
	require.False(t, backoff.retrying.Load())
}

func TestErrorReportedWhenRetrying(t *testing.T) {
	backoff := NewBackoff(common.NewChangeFeedIDWithName("test"), time.Minute*30, 1)
	require.True(t, backoff.ShouldRun())

	changefeed, state, err := backoff.CheckStatus(&heartbeatpb.MaintainerStatus{
		CheckpointTs: 1,
		Err: []*heartbeatpb.RunningError{
			{Message: "test"}},
	})
	require.NotNil(t, err)
	require.True(t, changefeed)
	require.Equal(t, model.StateWarning, state)
	require.True(t, backoff.retrying.Load())
	require.True(t, backoff.isRestarting.Load())
	backoffInterval := backoff.backoffInterval

	// report error again, the backoff interval is changed
	changefeed, state, err = backoff.CheckStatus(&heartbeatpb.MaintainerStatus{
		CheckpointTs: 1,
		Err: []*heartbeatpb.RunningError{
			{Message: "test"}},
	})
	require.NotNil(t, err)
	require.True(t, changefeed)
	require.Equal(t, model.StateWarning, state)
	require.True(t, backoff.retrying.Load())
	require.True(t, backoff.isRestarting.Load())
	// the interval is increased, todo: maybe we should ignore the error when retrying
	require.True(t, backoffInterval < backoff.backoffInterval)
}

func TestFailedWhenRetry(t *testing.T) {
	backoff := NewBackoff(common.NewChangeFeedIDWithName("test"), time.Second*30, 1)
	require.True(t, backoff.ShouldRun())

	mc := clock.NewMock()
	mc.Set(time.Now())
	backoff.errBackoff.Clock = mc

	changefeed, state, err := backoff.CheckStatus(&heartbeatpb.MaintainerStatus{
		CheckpointTs: 1,
		Err: []*heartbeatpb.RunningError{
			{Message: "test"}},
	})
	require.NotNil(t, err)
	require.True(t, changefeed)
	require.Equal(t, model.StateWarning, state)
	require.True(t, backoff.retrying.Load())
	require.True(t, backoff.isRestarting.Load())

	mc.Set(time.Now().Add(time.Minute))
	// failed
	changefeed, state, err = backoff.CheckStatus(&heartbeatpb.MaintainerStatus{
		CheckpointTs: 1,
		Err: []*heartbeatpb.RunningError{
			{Message: "test"}},
	})
	require.NotNil(t, err)
	require.True(t, changefeed)
	require.Equal(t, model.StateFailed, state)
	require.True(t, backoff.retrying.Load())
	require.True(t, backoff.isRestarting.Load())
	backoff.StartFinished()
	require.False(t, backoff.isRestarting.Load())
}

func TestNormal(t *testing.T) {
	backoff := NewBackoff(common.NewChangeFeedIDWithName("test"), time.Second*10, 1)
	require.True(t, backoff.ShouldRun())

	changefeed, state, err := backoff.CheckStatus(&heartbeatpb.MaintainerStatus{
		CheckpointTs: 2,
	})
	require.Nil(t, err)
	require.False(t, changefeed)
	require.Equal(t, model.StateNormal, state)
	require.False(t, backoff.retrying.Load())
	require.False(t, backoff.isRestarting.Load())
}
