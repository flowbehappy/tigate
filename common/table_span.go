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
	"github.com/flowbehappy/tigate/scheduler"
)

// TableSpan implement the InferiorID interface, it is the replicate unit,
// it can be a partial table region is the split table feature is enable for kafka sink
type TableSpan struct {
	TableID  uint64
	StartKey []byte
	EndKey   []byte
}

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
