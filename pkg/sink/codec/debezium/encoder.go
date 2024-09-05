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

package debezium

import (
	"bytes"
	"context"
	"time"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/sink/codec/encoder"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	ticommon "github.com/pingcap/tiflow/pkg/sink/codec/common"
)

// BatchEncoder encodes message into Debezium format.
type BatchEncoder struct {
	messages []*ticommon.Message

	config *ticommon.Config
	codec  *dbzCodec
}

// EncodeCheckpointEvent implements the RowEventEncoder interface
func (d *BatchEncoder) EncodeCheckpointEvent(ts uint64) (*ticommon.Message, error) {
	// Currently ignored. Debezium MySQL Connector does not emit such event.
	return nil, nil
}

// AppendRowChangedEvent implements the RowEventEncoder interface
func (d *BatchEncoder) AppendRowChangedEvent(
	_ context.Context,
	_ string,
	e *common.RowChangedEvent,
	callback func(),
) error {
	valueBuf := bytes.Buffer{}
	err := d.codec.EncodeRowChangedEvent(e, &valueBuf)
	if err != nil {
		return errors.Trace(err)
	}
	// TODO: Use a streaming compression is better.
	value, err := ticommon.Compress(
		d.config.ChangefeedID,
		d.config.LargeMessageHandle.LargeMessageHandleCompression,
		valueBuf.Bytes(),
	)
	if err != nil {
		return errors.Trace(err)
	}
	m := &ticommon.Message{
		Key:      nil,
		Value:    value,
		Ts:       e.CommitTs,
		Schema:   e.TableInfo.GetSchemaNamePtr(),
		Table:    e.TableInfo.GetTableNamePtr(),
		Type:     model.MessageTypeRow,
		Protocol: config.ProtocolDebezium,
		Callback: callback,
	}
	m.IncRowsCount()

	d.messages = append(d.messages, m)
	return nil
}

// EncodeDDLEvent implements the RowEventEncoder interface
// DDL message unresolved tso
func (d *BatchEncoder) EncodeDDLEvent(e *model.DDLEvent) (*ticommon.Message, error) {
	// Schema Change Events are currently not supported.
	return nil, nil
}

// Build implements the RowEventEncoder interface
func (d *BatchEncoder) Build() []*ticommon.Message {
	if len(d.messages) == 0 {
		return nil
	}

	result := d.messages
	d.messages = nil
	return result
}

// newBatchEncoder creates a new Debezium BatchEncoder.
func NewBatchEncoder(c *ticommon.Config, clusterID string) encoder.RowEventEncoder {
	batch := &BatchEncoder{
		messages: nil,
		config:   c,
		codec: &dbzCodec{
			config:    c,
			clusterID: clusterID,
			nowFunc:   time.Now,
		},
	}
	return batch
}

func (d *BatchEncoder) Clean() {}
