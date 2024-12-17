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

package replica

// type OpType int

// const (
// 	OpSplit         OpType = iota // Split one span to multiple subspans
// 	OpMerge                       // merge multiple spans to one span
// 	OpMergeAndSplit               // remove old spans and split to multiple subspans
// )

// type CheckResult[R Replication] struct {
// 	OpType       OpType
// 	Replications []R
// }

// type Checker[R Replication, S any] interface {
// 	UpdateStatus(replication R, status S)
// 	Check() []CheckResult[R]
// }

// define the check strategy
// soft/hard threadhold
// split/merge/mergeAndSplit result
