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

import "golang.org/x/sys/cpu"

var (
	_ Iterator = (*Iterator4Chunk)(nil)
	_ Iterator = (*Iterator4Slice)(nil)
)

// Iterator is used to iterate a number of rows.
//
//	for row := it.Begin(); row != it.End(); row = it.Next() {
//	    ...
//	}
type Iterator interface {
	// Begin resets the cursor of the iterator and returns the first Row.
	Begin() Row

	// Next returns the next Row.
	Next() Row

	// End returns the invalid end Row.
	End() Row

	// Len returns the length.
	Len() int

	// Current returns the current Row.
	Current() Row

	// ReachEnd reaches the end of iterator.
	ReachEnd()

	// Error returns none-nil error if anything wrong happens during the iteration.
	Error() error
}

// NewIterator4Slice returns a *Iterator4Slice for Row slice.
func NewIterator4Slice(rows []Row) *Iterator4Slice {
	return &Iterator4Slice{
		rows:   rows,
		cursor: 0,
	}
}

// Iterator4Slice is used to iterate rows inside a slice.
type Iterator4Slice struct {
	_      cpu.CacheLinePad
	rows   []Row
	cursor int
	_      cpu.CacheLinePad
}

// Begin implements the Iterator interface.
func (it *Iterator4Slice) Begin() Row {
	if it.Len() == 0 {
		return it.End()
	}
	it.cursor = 1
	return it.rows[0]
}

// Next implements the Iterator interface.
func (it *Iterator4Slice) Next() Row {
	if l := it.Len(); it.cursor >= l {
		it.cursor = l + 1
		return it.End()
	}
	row := it.rows[it.cursor]
	it.cursor++
	return row
}

// Current implements the Iterator interface.
func (it *Iterator4Slice) Current() Row {
	if it.cursor == 0 || it.cursor > it.Len() {
		return it.End()
	}
	return it.rows[it.cursor-1]
}

// End implements the Iterator interface.
func (*Iterator4Slice) End() Row {
	return Row{}
}

// ReachEnd implements the Iterator interface.
func (it *Iterator4Slice) ReachEnd() {
	it.cursor = it.Len() + 1
}

// Len implements the Iterator interface.
func (it *Iterator4Slice) Len() int {
	return len(it.rows)
}

// Reset iterator.rows and cursor.
func (it *Iterator4Slice) Reset(rows []Row) {
	it.rows = rows
	it.cursor = 0
}

// Error returns none-nil error if anything wrong happens during the iteration.
func (*Iterator4Slice) Error() error {
	return nil
}

// NewIterator4Chunk returns a iterator for Chunk.
func NewIterator4Chunk(chk *Chunk) *Iterator4Chunk {
	return &Iterator4Chunk{chk: chk}
}

// Iterator4Chunk is used to iterate rows inside a chunk.
type Iterator4Chunk struct {
	chk     *Chunk
	cursor  int32
	numRows int32
}

// Begin implements the Iterator interface.
func (it *Iterator4Chunk) Begin() Row {
	it.numRows = int32(it.chk.NumRows())
	if it.numRows == 0 {
		return it.End()
	}
	it.cursor = 1
	return it.chk.GetRow(0)
}

// Next implements the Iterator interface.
func (it *Iterator4Chunk) Next() Row {
	if it.cursor >= it.numRows {
		it.cursor = it.numRows + 1
		return it.End()
	}
	row := it.chk.GetRow(int(it.cursor))
	it.cursor++
	return row
}

// Current implements the Iterator interface.
func (it *Iterator4Chunk) Current() Row {
	if it.cursor == 0 || int(it.cursor) > it.Len() {
		return it.End()
	}
	return it.chk.GetRow(int(it.cursor) - 1)
}

// End implements the Iterator interface.
func (*Iterator4Chunk) End() Row {
	return Row{}
}

// ReachEnd implements the Iterator interface.
func (it *Iterator4Chunk) ReachEnd() {
	it.cursor = int32(it.Len() + 1)
}

// Len implements the Iterator interface
func (it *Iterator4Chunk) Len() int {
	return it.chk.NumRows()
}

// GetChunk returns the chunk stored in the Iterator4Chunk
func (it *Iterator4Chunk) GetChunk() *Chunk {
	return it.chk
}

// ResetChunk reset the chunk this iter holds
func (it *Iterator4Chunk) ResetChunk(chk *Chunk) {
	it.chk = chk
}

// Error returns none-nil error if anything wrong happens during the iteration.
func (*Iterator4Chunk) Error() error {
	return nil
}
