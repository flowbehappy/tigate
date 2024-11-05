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
	"bytes"
	"testing"

	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
)

func TestNewTableSpanRangeChecker(t *testing.T) {
	// Test if NewTableSpanRangeChecker initializes the object correctly with provided table IDs
	tables := []int64{1, 2, 3}
	rc := NewTableSpanRangeChecker(tables)
	require.NotNil(t, rc)
	require.Len(t, rc.tableSpans, 3)

	// Check that the table spans are correctly set up
	for _, tableID := range tables {
		require.NotNil(t, rc.tableSpans[tableID])
	}
}

func TestTableSpanRangeChecker_AddSubRange(t *testing.T) {
	// Test the AddSubRange function
	rc := NewTableSpanRangeChecker([]int64{1})
	start := []byte{0x00}
	end := []byte{0xFF}

	// Add sub-range that is within the span
	rc.AddSubRange(1, start, end)
	spanChecker := rc.tableSpans[1]
	require.NotNil(t, spanChecker)

	// Verify that the sub-range was added to the span checker
	require.False(t, spanChecker.IsFullyCovered())
}

func TestTableSpanRangeChecker_IsFullyCovered(t *testing.T) {
	// Test the IsFullyCovered function for TableSpanRangeChecker
	tables := []int64{0, 1}
	rc := NewTableSpanRangeChecker(tables)

	span := spanz.TableIDToComparableSpan(0)
	rc.AddSubRange(0, span.StartKey, span.EndKey)

	span = spanz.TableIDToComparableSpan(1)
	rc.AddSubRange(1, span.StartKey, appendNew(span.StartKey, 'a'))
	rc.AddSubRange(1, appendNew(span.StartKey, 'a'), appendNew(span.StartKey, 'b'))
	rc.AddSubRange(1, appendNew(span.StartKey, 'b'), span.EndKey)

	// Should return true since the range is fully covered
	require.True(t, rc.IsFullyCovered())

	// Reset and re-add to cover only part of the range
	rc.Reset()
	require.False(t, rc.IsFullyCovered())
	span = spanz.TableIDToComparableSpan(0)
	rc.AddSubRange(0, span.StartKey, span.EndKey)
	span = spanz.TableIDToComparableSpan(1)
	rc.AddSubRange(1, span.StartKey, span.EndKey)
	require.True(t, rc.IsFullyCovered())
}

func TestTableSpanRangeChecker_Reset(t *testing.T) {
	// Test the Reset function
	rc := NewTableSpanRangeChecker([]int64{1})
	start := []byte{0x00}
	end := []byte{0xFF}

	// Add sub-ranges and verify they exist
	rc.AddSubRange(1, start, end)
	require.False(t, rc.tableSpans[1].IsFullyCovered())

	// Call Reset, which should clear all reported spans
	rc.Reset()
	require.False(t, rc.tableSpans[1].IsFullyCovered())
}

func TestSpanCoverageChecker_AddSubRange(t *testing.T) {
	// Test the AddSubRange function for SpanCoverageChecker
	start := []byte{0x00}
	end := []byte{0xFF}
	rc := NewTableSpanCoverageChecker(start, end)

	// Add non-overlapping sub-ranges
	rc.AddSubRange([]byte{0x00}, []byte{0x10})
	rc.AddSubRange([]byte{0x10}, []byte{0x20})

	// Verify the ranges are added correctly
	require.False(t, rc.IsFullyCovered())

	// Add overlapping sub-ranges and check they are merged
	rc.AddSubRange([]byte{0x00}, []byte{0x20})
	require.False(t, rc.IsFullyCovered())

	// Add final range to cover everything
	rc.AddSubRange([]byte{0x20}, end)
	require.True(t, rc.IsFullyCovered())
}

func TestSpanCoverageChecker_IsFullyCovered(t *testing.T) {
	// Test the IsFullyCovered function
	start := []byte{0x00}
	end := []byte{0xFF}
	rc := NewTableSpanCoverageChecker(start, end)

	// Initially, the range is not covered
	require.False(t, rc.IsFullyCovered())

	// Add sub-ranges that fully cover the range
	rc.AddSubRange([]byte{0x00}, []byte{0x7F})
	rc.AddSubRange([]byte{0x7F}, []byte{0xFF})
	require.True(t, rc.IsFullyCovered())

	// Add non-continuous range, should not be fully covered
	rc.Reset()
	rc.AddSubRange([]byte{0x00}, []byte{0x50})
	require.False(t, rc.IsFullyCovered())
}

func TestSpanCoverageChecker_Reset(t *testing.T) {
	// Test the Reset function for SpanCoverageChecker
	start := []byte{0x00}
	end := []byte{0xFF}
	rc := NewTableSpanCoverageChecker(start, end)

	// Add some sub-ranges
	rc.AddSubRange([]byte{0x00}, []byte{0x50})
	rc.AddSubRange([]byte{0x50}, []byte{0xFF})
	require.True(t, rc.IsFullyCovered())

	// Reset the checker, it should be empty
	rc.Reset()
	require.False(t, rc.IsFullyCovered())
	require.Equal(t, 0, rc.tree.Len())
}

func appendNew(origin []byte, c byte) []byte {
	nb := bytes.Clone(origin)
	return append(nb, c)
}
