// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestIteratorOnSel(t *testing.T) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	chk := New(fields, 32, 1024)
	sel := make([]int, 0, 1024)
	for i := 0; i < 1024; i++ {
		chk.AppendInt64(0, int64(i))
		if i%2 == 0 {
			sel = append(sel, i)
		}
	}
	chk.SetSel(sel)
	it := NewIterator4Chunk(chk)
	cnt := 0
	for row := it.Begin(); row != it.End(); row = it.Next() {
		require.Equal(t, int64(0), row.GetInt64(0)%2)
		cnt++
	}
	require.Equal(t, 1024/2, cnt)
}

func checkEqual(it Iterator, exp []int64, t *testing.T) {
	require.Len(t, exp, it.Len())
	for row, i := it.Begin(), 0; row != it.End(); row, i = it.Next(), i+1 {
		require.Equal(t, exp[i], row.GetInt64(0))
	}
}

func TestIterator(t *testing.T) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	chk := New(fields, 32, 1024)
	n := 10
	var expected []int64
	for i := 0; i < n; i++ {
		chk.AppendInt64(0, int64(i))
		expected = append(expected, int64(i))
	}
	var rows []Row
	var it Iterator
	it = NewIterator4Slice(rows)
	checkEqual(it, expected, t)
	it.Begin()
	for i := 0; i < 5; i++ {
		require.Equal(t, rows[i], it.Current())
		it.Next()
	}
	it.ReachEnd()
	require.Equal(t, it.End(), it.Current())
	require.Equal(t, rows[0], it.Begin())

	it = NewIterator4Chunk(chk)
	checkEqual(it, expected, t)
	it.Begin()
	for i := 0; i < 5; i++ {
		require.Equal(t, chk.GetRow(i), it.Current())
		it.Next()
	}
	it.ReachEnd()
	require.Equal(t, it.End(), it.Current())
	require.Equal(t, chk.GetRow(0), it.Begin())

	it = NewIterator4Slice(nil)
	require.Equal(t, it.End(), it.Begin())
	it = NewIterator4Chunk(new(Chunk))
	require.Equal(t, it.End(), it.Begin())

}
