package open

import (
	"context"
	"encoding/binary"

	"github.com/flowbehappy/tigate/pkg/common"
	newcommon "github.com/flowbehappy/tigate/pkg/sink/codec/common"
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

// BatchEncoder for open protocol will batch multiple row changed events into a single message.
// One message can contain at most MaxBatchSize events, and the total size of the message cannot exceed MaxMessageBytes.
type BatchEncoder struct {
	messages []*ticommon.Message
	// buff the callback of the latest message
	callbackBuff []func()

	claimCheck *claimcheck.ClaimCheck

	config *newcommon.Config
}

// AppendRowChangedEvent implements the RowEventEncoder interface
func (d *BatchEncoder) AppendRowChangedEvent(
	ctx context.Context,
	_ string,
	e *common.RowEvent,
) error {
	key, value, length, err := encodeRowChangedEvent(e, d.config, false, "")
	if err != nil {
		return errors.Trace(err)
	}

	if length > d.config.MaxMessageBytes {
		// message len is larger than max-message-bytes
		if d.config.LargeMessageHandle.Disabled() {
			log.Warn("Single message is too large for open-protocol",
				zap.Int("maxMessageBytes", d.config.MaxMessageBytes),
				zap.Int("length", length),
				zap.Any("table", e.TableInfo.TableName),
				zap.Any("key", key))
			return cerror.ErrMessageTooLarge.GenWithStackByArgs()
		}

		if d.config.LargeMessageHandle.EnableClaimCheck() {
			// send the large message to the external storage first, then
			// create a new message contains the reference of the large message.
			claimCheckFileName := claimcheck.NewFileName()
			keyOutput, valueOutput := enhancedKeyValue(key, value)
			err = d.claimCheck.WriteMessage(ctx, keyOutput, valueOutput, claimCheckFileName)
			if err != nil {
				return errors.Trace(err)
			}

			key, value, length, err = encodeRowChangedEvent(e, d.config, true, d.claimCheck.FileNameWithPrefix(claimCheckFileName))
			if err != nil {
				return errors.Trace(err)
			}

			if length > d.config.MaxMessageBytes {
				log.Warn("Single message is too large for open-protocol, "+
					"when create the claim-check location message",
					zap.Int("maxMessageBytes", d.config.MaxMessageBytes),
					zap.Int("length", length),
					zap.Any("key", key))
				return cerror.ErrMessageTooLarge.GenWithStackByArgs()
			}
		}

		if d.config.LargeMessageHandle.HandleKeyOnly() {
			// it's must that `LargeMessageHandle == LargeMessageHandleOnlyHandleKeyColumns` here.
			key, value, length, err = encodeRowChangedEvent(e, d.config, true, "")
			if err != nil {
				return errors.Trace(err)
			}

			if length > d.config.MaxMessageBytes {
				log.Warn("Single message is too large for open-protocol even only encode handle key columns",
					zap.Int("maxMessageBytes", d.config.MaxMessageBytes),
					zap.Int("length", length),
					zap.Any("table", e.TableInfo.TableName),
					zap.Any("key", key))
				return cerror.ErrMessageTooLarge.GenWithStackByArgs()
			}
		}
	}

	d.pushMessage(key, value, e.Callback)
	return nil
}

// Build implements the RowEventEncoder interface
func (d *BatchEncoder) Build() (messages []*ticommon.Message) {
	if len(d.messages) == 0 {
		return nil
	}
	d.finalizeCallback()
	result := d.messages
	d.messages = nil
	return result
}

func (d *BatchEncoder) pushMessage(key, value []byte, callback func()) {
	length := len(key) + len(value) + 16

	var (
		keyLenByte   [8]byte
		valueLenByte [8]byte
	)
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	binary.BigEndian.PutUint64(valueLenByte[:], uint64(len(value)))

	if len(d.messages) == 0 || d.messages[len(d.messages)-1].Length()+length > d.config.MaxMessageBytes || d.messages[len(d.messages)-1].GetRowsCount() >= d.config.MaxBatchSize {
		d.finalizeCallback()
		// create a new message
		versionHead := make([]byte, 8)
		binary.BigEndian.PutUint64(versionHead, encoder.BatchVersion1)

		message := ticommon.Message{
			Key:      versionHead,
			Value:    valueLenByte[:],
			Type:     model.MessageTypeRow,
			Protocol: config.ProtocolOpen,
		}
		message.Key = append(message.Key, keyLenByte[:]...)
		message.Key = append(message.Key, key...)
		message.Value = append(message.Value, value...)
		message.IncRowsCount()
		d.callbackBuff = append(d.callbackBuff, callback)
		d.messages = append(d.messages, &message)
		return
	}

	// append to the latest message
	latestMessage := d.messages[len(d.messages)-1]
	latestMessage.Key = append(latestMessage.Key, keyLenByte[:]...)
	latestMessage.Key = append(latestMessage.Key, key...)
	latestMessage.Value = append(latestMessage.Value, valueLenByte[:]...)
	latestMessage.Value = append(latestMessage.Value, value...)
	d.callbackBuff = append(d.callbackBuff, callback)
	latestMessage.IncRowsCount()

}

func (d *BatchEncoder) finalizeCallback() {
	if len(d.callbackBuff) == 0 || len(d.messages) == 0 {
		return
	}

	lastMsg := d.messages[len(d.messages)-1]
	callbacks := d.callbackBuff
	lastMsg.Callback = func() {
		for _, cb := range callbacks {
			cb()
		}
	}
	d.callbackBuff = make([]func(), 0)
}

func enhancedKeyValue(key, value []byte) ([]byte, []byte) {
	var (
		keyLenByte   [8]byte
		valueLenByte [8]byte
		versionHead  [8]byte
	)
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	binary.BigEndian.PutUint64(valueLenByte[:], uint64(len(value)))
	binary.BigEndian.PutUint64(versionHead[:], encoder.BatchVersion1)

	keyOutput := versionHead[:]
	keyOutput = append(keyOutput, keyLenByte[:]...)
	keyOutput = append(keyOutput, key...)
	valueOutput := valueLenByte[:]
	valueOutput = append(valueOutput, value...)
	return keyOutput, valueOutput
}

// NewBatchEncoder creates a new BatchEncoder.
func NewBatchEncoder(ctx context.Context, config *newcommon.Config) (encoder.RowEventEncoder, error) {
	claimCheck, err := claimcheck.New(ctx, config.LargeMessageHandle, config.ChangefeedID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &BatchEncoder{
		config:     config,
		claimCheck: claimCheck,
	}, nil
}

func (d *BatchEncoder) Clean() {
	if d.claimCheck != nil {
		d.claimCheck.CleanMetrics()
	}
}

func (d *BatchEncoder) EncodeDDLEvent(e *common.DDLEvent) (*ticommon.Message, error) {
	key, value, err := encodeDDLEvent(e, d.config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &ticommon.Message{
		Key:      key,
		Value:    value,
		Type:     model.MessageTypeDDL,
		Protocol: config.ProtocolOpen,
	}, nil
}

// EncodeCheckpointEvent implements the RowEventEncoder interface
func (d *BatchEncoder) EncodeCheckpointEvent(ts uint64) (*ticommon.Message, error) {
	key, value, err := encodeResolvedTs(ts)

	if err != nil {
		return nil, errors.Trace(err)
	}

	return &ticommon.Message{
		Key:      key,
		Value:    value,
		Type:     model.MessageTypeResolved,
		Protocol: config.ProtocolOpen,
	}, nil
}
