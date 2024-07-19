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
	"testing"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/stretchr/testify/require"
)

func TestHashMap(t *testing.T) {
	t.Parallel()

	m := NewSpanHashMap[int]()

	// Insert then get.
	m.ReplaceOrInsert(TableSpan{&heartbeatpb.TableSpan{TableID: 1}}, 1)
	v, ok := m.Get(TableSpan{&heartbeatpb.TableSpan{TableID: 1}})
	require.Equal(t, v, 1)
	require.True(t, ok)
	require.Equal(t, 1, m.Len())
	require.True(t, m.Has(TableSpan{&heartbeatpb.TableSpan{TableID: 1}}))

	// Insert then get again.
	m.ReplaceOrInsert(TableSpan{&heartbeatpb.TableSpan{TableID: 1, StartKey: []byte{1}}}, 2)
	require.Equal(t, 2, m.Len())
	v, ok = m.Get(TableSpan{&heartbeatpb.TableSpan{TableID: 1, StartKey: []byte{1}}})
	require.Equal(t, v, 2)
	require.True(t, ok)

	// Overwrite then get.
	m.ReplaceOrInsert(
		TableSpan{&heartbeatpb.TableSpan{TableID: 1, StartKey: []byte{1}}}, 3)
	require.Equal(t, 2, m.Len())
	require.True(t, m.Has(TableSpan{&heartbeatpb.TableSpan{TableID: 1, StartKey: []byte{1}}}))
	v, ok = m.Get(TableSpan{&heartbeatpb.TableSpan{TableID: 1, StartKey: []byte{1}}})
	require.Equal(t, v, 3)
	require.True(t, ok)

	// get value
	v = m.GetV(TableSpan{&heartbeatpb.TableSpan{TableID: 1, StartKey: []byte{1}}})
	require.Equal(t, v, 3)

	// Delete than get value
	m.Delete(TableSpan{&heartbeatpb.TableSpan{TableID: 1, StartKey: []byte{1}}})
	require.Equal(t, 1, m.Len())
	require.False(t, m.Has(TableSpan{&heartbeatpb.TableSpan{TableID: 1, StartKey: []byte{1}}}))
	v = m.GetV(TableSpan{&heartbeatpb.TableSpan{TableID: 1, StartKey: []byte{1}}})
	require.Equal(t, v, 0)

	// Pointer value
	mp := NewSpanHashMap[*int]()
	vp := &v
	mp.ReplaceOrInsert(TableSpan{&heartbeatpb.TableSpan{TableID: 1}}, vp)
	vp1, ok := mp.Get(TableSpan{&heartbeatpb.TableSpan{TableID: 1}})
	require.Equal(t, vp, vp1)
	require.True(t, ok)
	require.Equal(t, 1, m.Len())
}

func TestHashMapIter(t *testing.T) {
	t.Parallel()

	m := NewSpanHashMap[int]()
	for i := 0; i < 4; i++ {
		m.ReplaceOrInsert(TableSpan{&heartbeatpb.TableSpan{TableID: uint64(i)}}, i)
	}

	j := 0
	m.Range(func(span TableSpan, value int) bool {
		_, ok := m.Get(span)
		require.True(t, ok)
		j++
		return true
	})
	require.Equal(t, 4, j)

	j = 0
	m.Range(func(span TableSpan, value int) bool {
		ok := m.Has(span)
		require.True(t, ok)
		j++
		return false
	})
	require.Equal(t, 1, j)
}
