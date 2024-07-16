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
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/rpc"
	"github.com/google/btree"
	"github.com/pingcap/tiflow/cdc/model"
)

type ItemIterator[Key interface{}, Value any] func(key Key, value Value) bool

type Map[Key interface{}, Value any] interface {
	Len() int
	Has(Key) bool
	Get(Key) (Value, bool)
	Delete(Key) (Value, bool)
	ReplaceOrInsert(Key, Value) (Value, bool)
	Ascend(iterator ItemIterator[Key, Value])
}

// Item is a btree item that wraps a  (key) and an item (value).
type Item[Key InferiorID, T any] struct {
	Key   Key
	Value T
}

// lessItem compares two Spans, defines the order between spans.
func lessItem[Key InferiorID, T any](a, b Item[Key, T]) bool {
	return a.Key.Less(b.Key)
}

// BtreeMap is a specialized btree map that map a Span to a value.
type BtreeMap[Key InferiorID, T any] struct {
	tree *btree.BTreeG[Item[Key, T]]
}

// NewBtreeMap returns a new BtreeMap.
func NewBtreeMap[Key InferiorID, T any]() *BtreeMap[Key, T] {
	const defaultDegree = 16
	return NewBtreeMapWithDegree[Key, T](defaultDegree)
}

// NewBtreeMapWithDegree returns a new BtreeMap with the given degree.
func NewBtreeMapWithDegree[Key InferiorID, T any](degree int) *BtreeMap[Key, T] {
	return &BtreeMap[Key, T]{
		tree: btree.NewG(degree, lessItem[Key, T]),
	}
}

// Len returns the number of items currently in the tree.
func (m *BtreeMap[Key, T]) Len() int {
	return m.tree.Len()
}

// Has returns true if the given key is in the tree.
func (m *BtreeMap[Key, T]) Has(key Key) bool {
	return m.tree.Has(Item[Key, T]{Key: key})
}

// Get looks for the key item in the tree, returning it.
// It returns (zeroValue, false) if unable to find that item.
func (m *BtreeMap[Key, T]) Get(key Key) (T, bool) {
	item, ok := m.tree.Get(Item[Key, T]{Key: key})
	return item.Value, ok
}

// Delete removes an item equal to the passed in item from the tree, returning
// it.  If no such item exists, returns (zeroValue, false).
func (m *BtreeMap[Key, T]) Delete(key Key) (T, bool) {
	item, ok := m.tree.Delete(Item[Key, T]{Key: key})
	return item.Value, ok
}

// ReplaceOrInsert adds the given item to the tree.  If an item in the tree
// already equals the given one, it is removed from the tree and returned,
// and the second return value is true.  Otherwise, (zeroValue, false)
//
// nil cannot be added to the tree (will panic).
func (m *BtreeMap[Key, T]) ReplaceOrInsert(key Key, value T) (T, bool) {
	old, ok := m.tree.ReplaceOrInsert(Item[Key, T]{Key: key, Value: value})
	return old.Value, ok
}

// Ascend calls the iterator for every value in the tree within the range
// [first, last], until iterator returns false.
func (m *BtreeMap[Key, T]) Ascend(iterator ItemIterator[Key, T]) {
	m.tree.Ascend(func(item Item[Key, T]) bool {
		return iterator(item.Key, item.Value)
	})
}

type Inferior interface {
	GetID() InferiorID
	UpdateStatus(InferiorStatus)
	IsAlive() bool
	NewInferiorStatus(heartbeatpb.ComponentState) InferiorStatus
	NewAddInferiorMessage(model.CaptureID, bool) rpc.Message
	NewRemoveInferiorMessage(model.CaptureID) rpc.Message
}

type InferiorID interface {
	Equal(InferiorID) bool
	String() string
	Less(InferiorID) bool
}

type InferiorStatus interface {
	GetInferiorID() InferiorID
	GetInferiorState() heartbeatpb.ComponentState
}
