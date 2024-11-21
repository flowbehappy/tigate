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

// RangeChecker is an interface for checking if a range is fully covered
type RangeChecker interface {
	// AddSubRange adds a sub table pan to the range checker.
	AddSubRange(tableID int64, start, end []byte)
	// IsFullyCovered checks if the entire range from start to end is covered.
	IsFullyCovered() bool
	// Reset resets the range checker reported sub spans
	Reset()
	// Detail returns the detail status of the range checker, it used for debugging.
	Detail() string
}
