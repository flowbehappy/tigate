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

package utils

import (
	"github.com/google/btree"
)

const defaultDegree = 16

// ItemIterator iterates the map, return false to stop the iteration
type ItemIterator[KeyT, ValueT any] func(KeyT, ValueT) bool

// Map is the general interface of a map
type Map[KeyT, ValueT any] interface {
	Len() int
	Has(KeyT) bool
	Get(KeyT) (ValueT, bool)
	Delete(KeyT) (ValueT, bool)
	ReplaceOrInsert(KeyT, ValueT) (ValueT, bool)
	Ascend(iterator ItemIterator[KeyT, ValueT])
}

// Item is a btree item that wraps a (key) and an item (value).
type Item[KeyT, ValueT any] struct {
	k KeyT
	v ValueT
}

// BtreeMap is a specialized btree map that map a Span to a value.
type BtreeMap[KeyT, ValueT any] struct {
	tree *btree.BTreeG[Item[KeyT, ValueT]]
}

// NewBtreeMap returns a new BtreeMap.
func NewBtreeMap[KeyT, ValueT any](lessKeyF func(KeyT, KeyT) bool) *BtreeMap[KeyT, ValueT] {
	return NewBtreeMapWithDegree[KeyT, ValueT](defaultDegree, nil)
}

// NewBtreeMapWithDegree returns a new BtreeMap with the given degree.
func NewBtreeMapWithDegree[KeyT, ValueT any](degree int, lessKey func(KeyT, KeyT) bool) *BtreeMap[KeyT, ValueT] {
	return &BtreeMap[KeyT, ValueT]{
		tree: btree.NewG(degree, func(a, b Item[KeyT, ValueT]) bool {
			return lessKey(a.k, b.k)
		}),
	}
}

// Len returns the number of items currently in the tree.
func (m *BtreeMap[KeyT, ValueT]) Len() int {
	return m.tree.Len()
}

// Has returns true if the given key is in the tree.
func (m *BtreeMap[KeyT, ValueT]) Has(key KeyT) bool {
	return m.tree.Has(Item[KeyT, ValueT]{k: key})
}

// Get looks for the key item in the tree, returning it.
// It returns (zeroValue, false) if unable to find that item.
func (m *BtreeMap[KeyT, ValueT]) Get(key KeyT) (ValueT, bool) {
	item, ok := m.tree.Get(Item[KeyT, ValueT]{k: key})
	return item.v, ok
}

// Delete removes an item equal to the passed in item from the tree, returning
// it.  If no such item exists, returns (zeroValue, false).
func (m *BtreeMap[KeyT, ValueT]) Delete(key KeyT) (ValueT, bool) {
	item, ok := m.tree.Delete(Item[KeyT, ValueT]{k: key})
	return item.v, ok
}

// ReplaceOrInsert adds the given item to the tree.  If an item in the tree
// already equals the given one, it is removed from the tree and returned,
// and the second return value is true.  Otherwise, (zeroValue, false)
//
// nil cannot be added to the tree (will panic).
func (m *BtreeMap[KeyT, ValueT]) ReplaceOrInsert(key KeyT, value ValueT) (ValueT, bool) {
	old, ok := m.tree.ReplaceOrInsert(Item[KeyT, ValueT]{k: key, v: value})
	return old.v, ok
}

// Ascend calls the iterator for every value in the tree within the range
// [first, last], until iterator returns false.
func (m *BtreeMap[KeyT, ValueT]) Ascend(iterator ItemIterator[KeyT, ValueT]) {
	m.tree.Ascend(func(item Item[KeyT, ValueT]) bool {
		return iterator(item.k, item.v)
	})
}
