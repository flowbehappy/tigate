// Copyright 2022 PingCAP, Inc.
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

package common

import (
	"encoding/binary"
	"reflect"
	"unsafe"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/util/seahash"
	"go.uber.org/zap"
)

// HashMapSpan is a specialized hash map that map a TableSpan to a value.
type SpanHashMap[T any] struct {
	hashMap map[hashableSpan]T
}

// NewHashMap returns a new HashMap.
func NewSpanHashMap[T any]() *SpanHashMap[T] {
	return &SpanHashMap[T]{
		hashMap: make(map[hashableSpan]T),
	}
}

// Len returns the number of items currently in the map.
func (m *SpanHashMap[T]) Len() int {
	return len(m.hashMap)
}

// Has returns true if the given key is in the map.
func (m *SpanHashMap[T]) Has(span heartbeatpb.TableSpan) bool {
	_, ok := m.hashMap[toHashableSpan(span)]
	return ok
}

// Get looks for the key item in the map, returning it.
// It returns (zeroValue, false) if unable to find that item.
func (m *SpanHashMap[T]) Get(span heartbeatpb.TableSpan) (T, bool) {
	item, ok := m.hashMap[toHashableSpan(span)]
	return item, ok
}

// GetV looks for the key item in the map, returning it.
// It returns zeroValue if unable to find that item.
func (m *SpanHashMap[T]) GetV(span heartbeatpb.TableSpan) T {
	item := m.hashMap[toHashableSpan(span)]
	return item
}

// Delete removes an item whose key equals to the span.
func (m *SpanHashMap[T]) Delete(span heartbeatpb.TableSpan) {
	delete(m.hashMap, toHashableSpan(span))
}

// ReplaceOrInsert adds the given item to the map.
func (m *SpanHashMap[T]) ReplaceOrInsert(span heartbeatpb.TableSpan, value T) {
	m.hashMap[toHashableSpan(span)] = value
}

type ItemIterator[T any] func(span heartbeatpb.TableSpan, value T) bool

// Range calls the iterator for every value in the map until iterator returns
// false.
func (m *SpanHashMap[T]) Range(iterator ItemIterator[T]) {
	for k, v := range m.hashMap {
		ok := iterator(k.toSpan(), v)
		if !ok {
			break
		}
	}
}

// HashTableSpan hashes the given span to a slot offset.
func HashTableSpan(span heartbeatpb.TableSpan, slots int) int {
	b := make([]byte, 8+len(span.StartKey))
	binary.LittleEndian.PutUint64(b[0:8], uint64(span.TableID))
	copy(b[8:], span.StartKey)
	return int(seahash.Sum64(b) % uint64(slots))
}

// hashableSpan is a hashable span, which can be used as a map key.
type hashableSpan struct {
	TableID  uint64
	StartKey string
	EndKey   string
}

// toHashableSpan converts a Span to a hashable span.
func toHashableSpan(span heartbeatpb.TableSpan) hashableSpan {
	return hashableSpan{
		TableID:  span.TableID,
		StartKey: unsafeBytesToString(span.StartKey),
		EndKey:   unsafeBytesToString(span.EndKey),
	}
}

// toSpan converts to Span.
func (h hashableSpan) toSpan() heartbeatpb.TableSpan {
	return heartbeatpb.TableSpan{
		TableID:  h.TableID,
		StartKey: unsafeStringToBytes(h.StartKey),
		EndKey:   unsafeStringToBytes(h.EndKey),
	}
}

// unsafeStringToBytes converts string to byte without memory allocation.
// The []byte must not be mutated.
// See: https://cs.opensource.google/go/go/+/refs/tags/go1.19.4:src/strings/builder.go;l=48
func unsafeBytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// unsafeStringToBytes converts string to byte without memory allocation.
// The returned []byte must not be mutated.
// See: https://groups.google.com/g/golang-nuts/c/Zsfk-VMd_fU/m/O1ru4fO-BgAJ
func unsafeStringToBytes(s string) []byte {
	if len(s) == 0 {
		return []byte{}
	}
	const maxCap = 0x7fff0000
	if len(s) > maxCap {
		log.Panic("string is too large", zap.Int("len", len(s)))
	}
	return (*[maxCap]byte)(unsafe.Pointer(
		(*reflect.StringHeader)(unsafe.Pointer(&s)).Data),
	)[:len(s):len(s)]
}
