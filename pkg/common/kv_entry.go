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
	"encoding/binary"
	"fmt"
)

// OpType for the kv, delete or put
type OpType uint32

// OpType for kv
const (
	OpTypeUnknown OpType = iota
	OpTypePut
	OpTypeDelete
	OpTypeResolved
)

type CompressType uint32

const (
	CompressTypeNone = iota
	CompressTypeZstd
)

// RawKVEntry represents a kv change or a resolved ts event
// TODO: use a different struct
type RawKVEntry struct {
	OpType  OpType `msg:"op_type"`  // offset 0 bytes
	CRTs    uint64 `msg:"crts"`     // offset 4 bytes
	StartTs uint64 `msg:"start_ts"` // offset 12 bytes
	// Commit or resolved TS
	// Additional debug info
	RegionID uint64 `msg:"region_id"` // offset 20 bytes

	KeyLen      uint32 `msg:"key_len"`       // offset 28 bytes
	ValueLen    uint32 `msg:"value_len"`     // offset 32 bytes
	OldValueLen uint32 `msg:"old_value_len"` // offset 36 bytes

	Key []byte `msg:"key"` // offset 40 bytes
	// nil for delete type
	Value []byte `msg:"value"`
	// nil for insert type
	OldValue []byte `msg:"old_value"`
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

// Encode serializes the RawKVEntry into a byte slice
func (v *RawKVEntry) Encode() []byte {
	// Calculate total size
	totalSize := 4*5 + 8*3 + len(v.Key) + len(v.Value) + len(v.OldValue)
	buf := make([]byte, 0, totalSize)
	// Use binary.LittleEndian.PutUint32/64 to write directly to the buffer
	buf = binary.LittleEndian.AppendUint32(buf, uint32(v.OpType))
	buf = binary.LittleEndian.AppendUint64(buf, v.CRTs)
	buf = binary.LittleEndian.AppendUint64(buf, v.StartTs)
	buf = binary.LittleEndian.AppendUint64(buf, v.RegionID)

	v.KeyLen = uint32(len(v.Key))
	v.ValueLen = uint32(len(v.Value))
	v.OldValueLen = uint32(len(v.OldValue))

	buf = binary.LittleEndian.AppendUint32(buf, v.KeyLen)
	buf = binary.LittleEndian.AppendUint32(buf, v.ValueLen)
	buf = binary.LittleEndian.AppendUint32(buf, v.OldValueLen)

	buf = append(buf, v.Key...)
	buf = append(buf, v.Value...)
	buf = append(buf, v.OldValue...)

	return buf
}

// Decode deserializes a byte slice into a RawKVEntry
func (v *RawKVEntry) Decode(data []byte) error {
	if len(data) < 36 { // Minimum size for fixed-length fields
		return fmt.Errorf("insufficient data length")
	}

	offset := 0
	v.OpType = OpType(binary.LittleEndian.Uint32(data[offset : offset+4]))
	offset += 4
	v.CRTs = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	v.StartTs = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	v.RegionID = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	v.KeyLen = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4
	v.ValueLen = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4
	v.OldValueLen = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	totalLen := int(v.KeyLen + v.ValueLen + v.OldValueLen)
	if len(data[offset:]) < totalLen {
		return fmt.Errorf("insufficient data for variable-length fields")
	}

	v.Key = data[offset : offset+int(v.KeyLen)]
	offset += int(v.KeyLen)

	v.Value = data[offset : offset+int(v.ValueLen)]
	offset += int(v.ValueLen)

	v.OldValue = data[offset : offset+int(v.OldValueLen)]

	return nil
}
