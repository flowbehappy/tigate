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

package encoder

import (
	"bytes"
	"context"

	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	ticommon "github.com/pingcap/tiflow/pkg/sink/codec/common"
)

const (
	// BatchVersion1 represents the version of batch format
	BatchVersion1 uint64 = 1

	// MemBufShrinkThreshold represents the threshold of shrinking the buffer.
	MemBufShrinkThreshold = 1024 * 1024
)

// EventEncoder is an abstraction for events encoder
type EventEncoder interface {
	// EncodeCheckpointEvent appends a checkpoint event into the batch.
	// This event will be broadcast to all partitions to signal a global checkpoint.
	EncodeCheckpointEvent(ts uint64) (*ticommon.Message, error)
	// EncodeDDLEvent appends a DDL event into the batch
	EncodeDDLEvent(e *commonEvent.DDLEvent) (*ticommon.Message, error)
	// AppendRowChangedEvent appends a row changed event into the batch or buffer.
	AppendRowChangedEvent(context.Context, string, *commonEvent.RowEvent) error
	// Build builds the batch messages from AppendRowChangedEvent and returns the messages.
	Build() []*ticommon.Message
	// clean the resources
	Clean()
}

// IsColumnValueEqual checks whether the preValue and updatedValue are equal.
func IsColumnValueEqual(preValue, updatedValue interface{}) bool {
	if preValue == nil || updatedValue == nil {
		return preValue == updatedValue
	}

	preValueBytes, ok1 := preValue.([]byte)
	updatedValueBytes, ok2 := updatedValue.([]byte)
	if ok1 && ok2 {
		return bytes.Equal(preValueBytes, updatedValueBytes)
	}
	// mounter use the same table info to parse the value,
	// the value type should be the same
	return preValue == updatedValue
}
