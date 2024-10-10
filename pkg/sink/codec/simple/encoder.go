// Copyright 2023 PingCAP, Inc.
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

package simple

import (
	"context"

	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/pkg/sink/codec/encoder"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	ticommon "github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/kafka/claimcheck"
	"go.uber.org/zap"
)

type Encoder struct {
	messages   []*ticommon.Message
	config     *ticommon.Config
	claimCheck *claimcheck.ClaimCheck
	marshaller marshaller
}

func NewEncoder(ctx context.Context, config *ticommon.Config) (encoder.EventEncoder, error) {
	claimCheck, err := claimcheck.New(ctx, config.LargeMessageHandle, config.ChangefeedID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	marshaller, err := newMarshaller(config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Encoder{
		messages:   make([]*ticommon.Message, 0, 1),
		config:     config,
		claimCheck: claimCheck,
		marshaller: marshaller,
	}, nil
}

// AppendRowChangedEvent implement the RowEventEncoder interface
func (e *Encoder) AppendRowChangedEvent(ctx context.Context, _ string, event *commonEvent.RowChangedEvent, callback func()) error {
	value, err := e.marshaller.MarshalRowChangedEvent(event, false, "")
	if err != nil {
		return err
	}

	value, err = ticommon.Compress(e.config.ChangefeedID, e.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	if err != nil {
		return err
	}

	result := &ticommon.Message{
		Value:    value,
		Ts:       event.CommitTs,
		Schema:   event.TableInfo.GetSchemaNamePtr(),
		Table:    event.TableInfo.GetTableNamePtr(),
		Type:     model.MessageTypeRow,
		Protocol: config.ProtocolSimple,
		Callback: callback,
	}

	result.IncRowsCount()
	length := result.Length()
	if length <= e.config.MaxMessageBytes {
		e.messages = append(e.messages, result)
		return nil
	}

	if e.config.LargeMessageHandle.Disabled() {
		log.Error("Single message is too large for simple",
			zap.Int("maxMessageBytes", e.config.MaxMessageBytes),
			zap.Int("length", length),
			zap.Any("table", event.TableInfo.TableName))
		return cerror.ErrMessageTooLarge.GenWithStackByArgs()
	}

	var claimCheckLocation string
	if e.config.LargeMessageHandle.EnableClaimCheck() {
		fileName := claimcheck.NewFileName()
		claimCheckLocation = e.claimCheck.FileNameWithPrefix(fileName)
		if err = e.claimCheck.WriteMessage(ctx, result.Key, result.Value, fileName); err != nil {
			return errors.Trace(err)
		}
	}

	value, err = e.marshaller.MarshalRowChangedEvent(event, true, claimCheckLocation)
	if err != nil {
		return err
	}
	value, err = ticommon.Compress(e.config.ChangefeedID, e.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	if err != nil {
		return err
	}
	result.Value = value

	if result.Length() <= e.config.MaxMessageBytes {
		log.Warn("Single message is too large for simple, only encode handle key columns",
			zap.Int("maxMessageBytes", e.config.MaxMessageBytes),
			zap.Int("originLength", length),
			zap.Int("length", result.Length()),
			zap.Any("table", event.TableInfo.TableName))
		e.messages = append(e.messages, result)
		return nil
	}

	log.Error("Single message is still too large for simple after only encode handle key columns",
		zap.Int("maxMessageBytes", e.config.MaxMessageBytes),
		zap.Int("length", result.Length()),
		zap.Any("table", event.TableInfo.TableName))
	return cerror.ErrMessageTooLarge.GenWithStackByArgs()
}

// Build implement the RowEventEncoder interface
func (e *Encoder) Build() []*ticommon.Message {
	var result []*ticommon.Message
	if len(e.messages) != 0 {
		result = e.messages
		e.messages = nil
	}
	return result
}

// EncodeCheckpointEvent implement the DDLEventBatchEncoder interface
func (e *Encoder) EncodeCheckpointEvent(ts uint64) (*ticommon.Message, error) {
	value, err := e.marshaller.MarshalCheckpoint(ts)
	if err != nil {
		return nil, err
	}

	value, err = ticommon.Compress(e.config.ChangefeedID,
		e.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	return ticommon.NewResolvedMsg(config.ProtocolSimple, nil, value, ts), err
}

// EncodeDDLEvent implement the DDLEventBatchEncoder interface
func (e *Encoder) EncodeDDLEvent(event *commonEvent.DDLEvent) (*ticommon.Message, error) {
	// value, err := e.marshaller.MarshalDDLEvent(event)
	// if err != nil {
	// 	return nil, err
	// }

	// value, err = ticommon.Compress(e.config.ChangefeedID,
	// 	e.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	// if err != nil {
	// 	return nil, err
	// }
	// result := ticommon.NewDDLMsg(config.ProtocolSimple, nil, value, event)

	// if result.Length() > e.config.MaxMessageBytes {
	// 	log.Error("DDL message is too large for simple",
	// 		zap.Int("maxMessageBytes", e.config.MaxMessageBytes),
	// 		zap.Int("length", result.Length()),
	// 		zap.Any("table", event.TableInfo.TableName))
	// 	return nil, cerror.ErrMessageTooLarge.GenWithStackByArgs()
	// }
	// return result, nil
	return nil, nil
}

// CleanMetrics implement the RowEventEncoderBuilder interface
func (e *Encoder) Clean() {
	if e.claimCheck != nil {
		e.claimCheck.CleanMetrics()
	}
}
