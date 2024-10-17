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

// TableIDRangeChecker is used to check if all table IDs are covered.
type TableIDRangeChecker struct {
	tables map[int64]bool
}

// NewTableIDRangeChecker creates a new TableIDRangeChecker.
func NewTableIDRangeChecker(tables []int64) *TableIDRangeChecker {
	tc := &TableIDRangeChecker{
		tables: make(map[int64]bool, len(tables)),
	}
	for _, tableID := range tables {
		tc.tables[tableID] = false
	}
	return tc
}

// AddSubRange adds table id to the range checker.
func (rc *TableIDRangeChecker) AddSubRange(tableID int64, _, _ []byte) {
	rc.tables[tableID] = true
}

// IsFullyCovered checks if all table IDs are covered.
func (rc *TableIDRangeChecker) IsFullyCovered() bool {
	for _, covered := range rc.tables {
		if !covered {
			return false
		}
	}
	return true
}

// Reset resets the reported tables.
func (rc *TableIDRangeChecker) Reset() {
	for key, _ := range rc.tables {
		rc.tables[key] = false
	}
}
