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

package scheduler

import (
	"math/rand"

	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/utils/heap"
)

type priorityQueue[T replica.Replication] struct {
	h    *heap.Heap[*Item[T]]
	less func(a, b int) bool

	rand *rand.Rand
}

func (q *priorityQueue[T]) InitItem(node node.ID, load int, tasks []T) {
	q.AddOrUpdate(&Item[T]{
		Node:  node,
		tasks: tasks,
		load:  load,
		less:  q.less,
	})
}

func (q *priorityQueue[T]) AddOrUpdate(item *Item[T]) {
	item.randomizeWorkload = randomizeWorkload(q.rand, item.load)
	q.h.AddOrUpdate(item)
}

func (q *priorityQueue[T]) PeekTop() (*Item[T], bool) {
	return q.h.PeekTop()
}

// Item is an item in the priority queue, use the Load field as the priority
type Item[T replica.Replication] struct {
	Node  node.ID
	tasks []T
	load  int

	// for heap adjustment
	index             int
	less              func(a, b int) bool
	randomizeWorkload int
}

func (i *Item[T]) SetHeapIndex(idx int) {
	i.index = idx
}

func (i *Item[T]) GetHeapIndex() int {
	return i.index
}

func (i *Item[T]) LessThan(t *Item[T]) bool {
	return i.less(i.randomizeWorkload, t.randomizeWorkload)
}

const (
	randomPartBitSize = 8
	randomPartMask    = (1 << randomPartBitSize) - 1
)

// randomizeWorkload injects small randomness into the workload, so that
// when two captures tied in competing for the minimum workload, the result
// will not always be the same.
// The bitwise layout of the return value is:
// 63                8                0
// |----- input -----|-- random val --|
func randomizeWorkload(random *rand.Rand, input int) int {
	if random == nil {
		return input
	}
	randomPart := int(random.Uint32() & randomPartMask)
	// randomPart is a small random value that only affects the
	// result of comparison of workloads when two workloads are equal.
	return (input << randomPartBitSize) | randomPart
}
