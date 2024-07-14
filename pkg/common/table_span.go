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

package common

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/scheduler"
)

// TableSpan implement the InferiorID interface, it is the replicate unit,
// it can be a partial table region is the split table feature is enable for kafka sink
type TableSpan struct {
	*heartbeatpb.TableSpan
}

// DDLSpan is the special span for Table Trigger Event Dispatcher
var DDLSpan = TableSpan{TableSpan: &heartbeatpb.TableSpan{TableID: 0, StartKey: nil, EndKey: nil}}

// Less compares two Spans, defines the order between spans.
func (s *TableSpan) Less(inferior scheduler.InferiorID) bool {
	tbl := inferior.(*TableSpan)
	if s.TableID < tbl.TableID {
		return true
	}
	if bytes.Compare(s.StartKey, tbl.StartKey) < 0 {
		return true
	}
	return false
}

func (s *TableSpan) String() string {
	return fmt.Sprintf("id: %d, startKey: %s, endKey: %s",
		s.TableID, hex.EncodeToString(s.StartKey),
		hex.EncodeToString(s.EndKey))
}

func (s *TableSpan) Equal(inferior scheduler.InferiorID) bool {
	tbl := inferior.(*TableSpan)
	return s.TableID == tbl.TableID &&
		bytes.Equal(s.StartKey, tbl.StartKey) &&
		bytes.Equal(s.EndKey, tbl.EndKey)
}

type DataRange struct {
	Span    *TableSpan
	startTs uint64
	endTs   uint64
}

func NewDataRange(span *TableSpan, startTs, endTs uint64) *DataRange {
	return &DataRange{
		Span:    span,
		startTs: startTs,
		endTs:   endTs,
	}
}

func (d *DataRange) StartTs() uint64 {
	return d.startTs
}

func (d *DataRange) EndTs() uint64 {
	return d.endTs
}

func (d *DataRange) String() string {
	return fmt.Sprintf("span: %s, startTs: %d, endTs: %d", d.Span.String(), d.startTs, d.endTs)
}

func (d *DataRange) Equal(other *DataRange) bool {
	return d.Span.Equal(other.Span) && d.startTs == other.startTs && d.endTs == other.endTs
}

// Merge merges two DataRange, if the two DataRange have different Span, return nil.
// Otherwise, return the merged DataRange.
// The merged DataRange has the same Span, and the startTs is the minimum of the two DataRange,
// and the endTs is the maximum of the two DataRange.
func (d *DataRange) Merge(other *DataRange) *DataRange {
	if !d.Span.Equal(other.Span) {
		return nil
	}
	if d.startTs > other.startTs {
		d.startTs = other.startTs
	}
	if d.endTs < other.endTs {
		d.endTs = other.endTs
	}
	return d
}
