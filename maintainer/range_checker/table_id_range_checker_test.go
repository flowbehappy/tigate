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
	// Test if NewTableIDRangeChecker initializes the object correctly with provided table IDs
	tables := []int64{1, 2, 3}
	rc := NewTableIDRangeChecker(tables)
	require.NotNil(t, rc)
	require.Len(t, rc.tables, 3)

	// Check that the provided table IDs are correctly set
	for _, tableID := range tables {
		require.Contains(t, rc.tables, tableID)
	}
}

func TestAddSubRange(t *testing.T) {
	// Test the AddSubRange function
	rc := NewTableIDRangeChecker([]int64{1, 2, 3})
	require.Len(t, rc.tables, 3)
	// Add a table ID that exists in the tables map
	rc.AddSubRange(1, nil, nil)
	require.True(t, rc.tables[1])
	require.False(t, rc.tables[2])
	// Initially, none of the table IDs are reported, should return false
	require.False(t, rc.IsFullyCovered())
}

func TestIsFullyCovered(t *testing.T) {
	// Test the IsFullyCovered function
	rc := NewTableIDRangeChecker([]int64{1, 2, 3})

	// Initially, none of the table IDs are reported, should return false
	require.False(t, rc.IsFullyCovered())

	// Report one table ID, should still return false
	rc.AddSubRange(1, nil, nil)
	require.False(t, rc.IsFullyCovered())

	// Report all table IDs, should return true
	rc.AddSubRange(2, nil, nil)
	rc.AddSubRange(3, nil, nil)
	require.True(t, rc.IsFullyCovered())
}

func TestReset(t *testing.T) {
	// Test the Reset function
	rc := NewTableIDRangeChecker([]int64{1, 2, 3})

	// Add a table ID to reportedTables
	rc.AddSubRange(1, nil, nil)
	rc.AddSubRange(2, nil, nil)
	rc.AddSubRange(3, nil, nil)
	require.True(t, rc.IsFullyCovered())

	// Call Reset, should clear reportedTables
	rc.Reset()

	// Ensure tables map remains unaffected
	require.Len(t, rc.tables, 3)
	require.False(t, rc.IsFullyCovered())
}
