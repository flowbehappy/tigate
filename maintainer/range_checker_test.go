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
	"testing"

	"github.com/stretchr/testify/require"
)

// Test fully covered range
func TestRangeChecker_FullyCovered(t *testing.T) {
	start := []byte{0}
	end := []byte{100}

	rc := NewRangeChecker(start, end)

	rc.AddSubRange([]byte{0}, []byte{30})
	rc.AddSubRange([]byte{30}, []byte{60})
	rc.AddSubRange([]byte{60}, []byte{100})

	require.True(t, rc.IsFullyCovered(), "Expected the range to be fully covered.")
}

// Test partially covered range
func TestRangeChecker_PartiallyCovered(t *testing.T) {
	start := []byte{0}
	end := []byte{100}

	rc := NewRangeChecker(start, end)

	rc.AddSubRange([]byte{0}, []byte{30})
	rc.AddSubRange([]byte{40}, []byte{100}) // Missing part from 30 to 40

	require.False(t, rc.IsFullyCovered(), "Expected the range to be partially covered.")
}

// Test single range covering the full range
func TestRangeChecker_SingleFullRange(t *testing.T) {
	start := []byte{0}
	end := []byte{100}

	rc := NewRangeChecker(start, end)

	rc.AddSubRange([]byte{0}, []byte{100})

	require.True(t, rc.IsFullyCovered(), "Expected the range to be fully covered by a single sub-range.")
}

// Test adding overlapping ranges
func TestRangeChecker_OverlappingRanges(t *testing.T) {
	start := []byte{0}
	end := []byte{100}

	rc := NewRangeChecker(start, end)

	rc.AddSubRange([]byte{0}, []byte{50})
	rc.AddSubRange([]byte{25}, []byte{100}) // Overlapping ranges

	require.True(t, rc.IsFullyCovered(), "Expected the range to be fully covered with overlapping ranges.")
}

// Test adding non-overlapping ranges that don't cover the full range
func TestRangeChecker_NonOverlappingRanges(t *testing.T) {
	start := []byte{0}
	end := []byte{100}

	rc := NewRangeChecker(start, end)

	rc.AddSubRange([]byte{0}, []byte{30})
	rc.AddSubRange([]byte{50}, []byte{100}) // Missing part from 30 to 50

	require.False(t, rc.IsFullyCovered(), "Expected the range to be partially covered.")
}

// Test edge case with empty ranges
func TestRangeChecker_EmptyRanges(t *testing.T) {
	start := []byte{0}
	end := []byte{100}

	rc := NewRangeChecker(start, end)

	// No sub-ranges added, should not be fully covered
	require.False(t, rc.IsFullyCovered(), "Expected the range to be not covered as no sub-ranges were added.")
}

// Test with different lengths of start and end byte slices
func TestRangeChecker_DifferentLengths(t *testing.T) {
	start := []byte{0, 0}
	end := []byte{255, 255, 255, 255}

	rc := NewRangeChecker(start, end)

	// Adding ranges that only cover part of the full range
	rc.AddSubRange([]byte{0}, []byte{0, 0, 1})                   // Covers part of the range
	rc.AddSubRange([]byte{0, 0, 1}, []byte{128, 128, 128})       // Covers part of the range
	rc.AddSubRange([]byte{128, 128, 128}, []byte{255, 255, 255}) // Still not fully covered

	require.False(t, rc.IsFullyCovered(), "Expected the range to be not covered due to different lengths.")
}

// Test fully covered range
func TestRangeChecker_Reset(t *testing.T) {
	start := []byte{0}
	end := []byte{100}

	rc := NewRangeChecker(start, end)

	rc.AddSubRange([]byte{0}, []byte{30})
	rc.AddSubRange([]byte{30}, []byte{60})
	rc.AddSubRange([]byte{60}, []byte{100})

	require.True(t, rc.IsFullyCovered(), "Expected the range to be fully covered.")
	rc.Reset()
	require.False(t, rc.IsFullyCovered(), "Expected the range to be not covered after reset.")
}
