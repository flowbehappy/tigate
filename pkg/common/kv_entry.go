// Copyright 2020 PingCAP, Inc.
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

//go:generate msgp

package common

import (
	"fmt"
)

// OpType for the kv, delete or put
type OpType int

// OpType for kv
const (
	OpTypeUnknown OpType = iota
	OpTypePut
	OpTypeDelete
	OpTypeResolved
)

type CompressType int32

const (
	CompressTypeNone = iota
	CompressTypeZstd
)

// RawKVEntry represents a kv change or a resolved ts event
// TODO: use a different struct
type RawKVEntry struct {
	OpType OpType `msg:"op_type"`
	Key    []byte `msg:"key"`
	// nil for delete type
	Value []byte `msg:"value"`
	// nil for insert type
	OldValue []byte `msg:"old_value"`
	StartTs  uint64 `msg:"start_ts"`
	// Commit or resolved TS
	CRTs uint64 `msg:"crts"`

	// Additional debug info
	RegionID     uint64       `msg:"region_id"`
	CompressType CompressType `msg:"compress_type"`
}

func (v *RawKVEntry) IsResolved() bool {
	return v.OpType == OpTypeResolved
}

// IsUpdate checks if the event is an update event.
func (v *RawKVEntry) IsUpdate() bool {
	return v.OpType == OpTypePut && v.OldValue != nil && v.Value != nil
}

func (v *RawKVEntry) String() string {
	// TODO: redact values.
	return fmt.Sprintf(
		"OpType: %v, Key: %s, Value: %s, OldValue: %s, StartTs: %d, CRTs: %d, RegionID: %d",
		v.OpType, string(v.Key), string(v.Value), string(v.OldValue), v.StartTs, v.CRTs, v.RegionID)
}

// ApproximateDataSize calculate the approximate size of protobuf binary
// representation of this event.
func (v *RawKVEntry) ApproximateDataSize() int64 {
	return int64(len(v.Key) + len(v.Value) + len(v.OldValue))
}
