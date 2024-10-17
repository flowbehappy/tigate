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

package range_checker

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewTableIDRangeChecker(t *testing.T) {
	rc := NewTableCountChecker(3)
	require.NotNil(t, rc)
	require.Len(t, rc.reportedMap, 0)
	require.Equal(t, 3, rc.needCount)
}

func TestAddSubRange(t *testing.T) {
	rc := NewTableCountChecker(3)
	require.Len(t, rc.reportedMap, 0)
	rc.AddSubRange(1, nil, nil)
	require.Len(t, rc.reportedMap, 1)
	_, ok := rc.reportedMap[1]
	require.True(t, ok)
	require.False(t, rc.IsFullyCovered())
}

func TestIsFullyCovered(t *testing.T) {
	rc := NewTableCountChecker(3)
	require.False(t, rc.IsFullyCovered())
	rc.AddSubRange(1, nil, nil)
	require.False(t, rc.IsFullyCovered())
	rc.AddSubRange(2, nil, nil)
	rc.AddSubRange(3, nil, nil)
	require.True(t, rc.IsFullyCovered())
}

func TestReset(t *testing.T) {
	rc := NewTableCountChecker(3)
	rc.AddSubRange(1, nil, nil)
	rc.AddSubRange(2, nil, nil)
	rc.AddSubRange(3, nil, nil)
	require.True(t, rc.IsFullyCovered())
	rc.Reset()
	require.Len(t, rc.reportedMap, 0)
	require.False(t, rc.IsFullyCovered())
}
