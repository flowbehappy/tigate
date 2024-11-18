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

package operator

import (
	"testing"

	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/stretchr/testify/require"
)

func TestAddMaintainerOperator_OnNodeRemove(t *testing.T) {
	op := NewAddMaintainerOperator(nil, &changefeed.Changefeed{}, "n1")
	op.OnNodeRemove("n2")
	require.False(t, op.canceled.Load())
	require.False(t, op.finished.Load())

	op.OnNodeRemove("n1")
	require.True(t, op.canceled.Load())
	require.True(t, op.finished.Load())

	require.Nil(t, op.Schedule())
}

func TestAddMaintainerOperator_OnTaskRemoved(t *testing.T) {
	op := NewAddMaintainerOperator(nil, &changefeed.Changefeed{}, "n1")

	op.OnTaskRemoved()
	require.True(t, op.canceled.Load())
	require.True(t, op.finished.Load())

	require.Nil(t, op.Schedule())
}
