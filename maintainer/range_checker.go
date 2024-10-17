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
	"bytes"

	"github.com/google/btree"
)

// RangeChecker is used to check if all ranges cover the start and end byte slices.
type RangeChecker struct {
	start, end []byte
	tree       *btree.BTreeG[*RangeNode]
}

// NewRangeChecker creates a new RangeChecker with given start and end.
func NewRangeChecker(start, end []byte) *RangeChecker {
	return &RangeChecker{
		start: start,
		end:   end,
		tree:  btree.NewG[*RangeNode](4, rangeLockEntryLess),
	}
}

// AddSubRange adds a sub-range to the range checker.
func (rc *RangeChecker) AddSubRange(newStart, newEnd []byte) {
	// Iterate through the B-tree to find overlapping or adjacent ranges
	var toDelete []*RangeNode

	// Find ranges that overlap or touch the new range
	rc.tree.AscendGreaterOrEqual(&RangeNode{start: newStart}, func(node *RangeNode) bool {
		if bytes.Compare(node.end, newStart) < 0 {
			// No more overlapping ranges
			return true
		}
		if bytes.Compare(node.start, newEnd) > 0 {
			// If the start of the current node is beyond the end of the new range, stop
			return false
		}
		// Mark this node for deletion and keep it in the list
		toDelete = append(toDelete, node)
		return true
	})

	// If there are overlapping ranges, merge them
	if len(toDelete) > 0 {
		mergedStart := newStart
		mergedEnd := newEnd

		for _, node := range toDelete {
			// Update merged start and end
			if bytes.Compare(node.start, mergedStart) < 0 {
				mergedStart = node.start
			}
			if bytes.Compare(node.end, mergedEnd) > 0 {
				mergedEnd = node.end
			}
		}

		// Remove all the overlapping nodes from the tree
		for _, node := range toDelete {
			rc.tree.Delete(node)
		}

		// Insert the merged range
		rc.tree.ReplaceOrInsert(&RangeNode{start: mergedStart, end: mergedEnd})
	} else {
		// No overlap, simply insert the new range
		rc.tree.ReplaceOrInsert(&RangeNode{start: newStart, end: newEnd})
	}
}

// IsFullyCovered checks if the entire range from start to end is covered.
func (rc *RangeChecker) IsFullyCovered() bool {
	if rc.tree.Len() == 0 {
		return false
	}

	currentStart := rc.start

	// Use Ascend method to iterate over the nodes
	rc.tree.Ascend(func(node *RangeNode) bool {
		if bytes.Compare(currentStart, node.start) < 0 {
			// There is a gap, not fully covered
			return false
		}
		// Move to the next position
		currentStart = node.end
		return bytes.Compare(currentStart, rc.end) < 0 // Continue until we've covered the whole range
	})

	return bytes.Equal(currentStart, rc.end)
}

func (rc *RangeChecker) Reset() {
	rc.tree = btree.NewG[*RangeNode](4, rangeLockEntryLess)
}

// RangeNode represents a node in the BTree.
type RangeNode struct {
	start, end []byte
}

// rangeLockEntryLess compares two RangeNode based on their start and end values.
func rangeLockEntryLess(a, b *RangeNode) bool {
	if bytes.Equal(a.start, b.start) {
		return bytes.Compare(a.end, b.end) < 0
	}
	return bytes.Compare(a.start, b.end) < 0
}
