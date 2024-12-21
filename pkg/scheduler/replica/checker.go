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

import "github.com/pingcap/ticdc/pkg/node"

type OpType int

const (
	OpSplit         OpType = iota // Split one span to multiple subspans
	OpMerge                       // merge multiple spans to one span
	OpMergeAndSplit               // remove old spans and split to multiple subspans
)

type CheckResult[T ReplicationID, R Replication[T]] struct {
	OpType       OpType
	Replications []R
}

type Checker[T ReplicationID, R Replication[T], S any] interface {
	UpdateStatus(replication R, status S)
	Check() []CheckResult[T, R]
}

type GroupCheckResult any
type ReplicationStatus any

type StatusChecker[T ReplicationID, R Replication[T], S ReplicationStatus, C GroupCheckResult] interface {
	AddReplica(replication R)
	RemoveReplica(replication R)
	UpdateStatus(replication R, status S)
	Check(nodes map[node.ID]*node.Info) []C
}

// implement a empty status checker
type EmptyStatusChecker[T ReplicationID, R Replication[T], S ReplicationStatus] struct{}

func (c *EmptyStatusChecker[T, R, S]) AddReplica(replication R) {}

func (c *EmptyStatusChecker[T, R, S]) RemoveReplica(replication R) {}

func (c *EmptyStatusChecker[T, R, S]) UpdateStatus(replication R, status S) {}

func (c *EmptyStatusChecker[T, R, S]) Check(nodes map[node.ID]*node.Info) []GroupCheckResult {
	return nil
}
