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

package mysql

import (
	"testing"
)

// BenchmarkOldBuildInsert-10: 1000000	      1101 ns/op	    7544 B/op	     103 allocs/op
// BenchmarkBuildInsert-10:2492268	       530.9 ns/op	    2472 B/op	      87 allocs/op ( 2.49x )
func BenchmarkBuildInsert(b *testing.B) {
	insert, _, _, tableInfo := getRowForTest(b)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = buildInsert(tableInfo, insert, false)
		}
	})
}

// BenchmarkBuildDelete-10: 6709940	       204.4 ns/op	    1672 B/op	       7 allocs/op
func BenchmarkBuildDelete(b *testing.B) {
	_, delete, _, tableInfo := getRowForTest(b)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = buildDelete(tableInfo, delete)
		}
	})
}

// BenchmarkOldBuildUpdate-10: 918433	      1526 ns/op	    9722 B/op	     101 allocs/op
// BenchmarkBuildUpdate-10: 1381165	       878.6 ns/op	    6440 B/op	      91 allocs/op ( 1.50x )
func BenchmarkBuildUpdate(b *testing.B) {
	_, _, update, tableInfo := getRowForTest(b)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = buildUpdate(tableInfo, update)
		}
	})
}
