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

// BoolRangeChecker is a range checker that always returns the same value.
type BoolRangeChecker struct {
	covered bool
}

func NewBoolRangeChecker(covered bool) *BoolRangeChecker {
	return &BoolRangeChecker{
		covered: covered,
	}
}

// AddSubRange adds a sub table pan to the range checker.
func (f *BoolRangeChecker) AddSubRange(_ int64, _, _ []byte) {

}

// IsFullyCovered checks if the entire range from start to end is covered.
func (f *BoolRangeChecker) IsFullyCovered() bool {
	return f.covered
}

// Reset resets the range checker reported sub spans
func (f *BoolRangeChecker) Reset() {

}
