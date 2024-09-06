package open

import (
	"bytes"
	"encoding/binary"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/sink/codec/encoder"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	ticommon "github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/util"
)

func encodeRowChangedEvent(e *common.RowChangedEvent, config *ticommon.Config, largeMessageOnlyHandleKeyColumns bool, claimCheckLocationName string) ([]byte, []byte, int, error) {
	keyBuf := &bytes.Buffer{}
	valueBuf := &bytes.Buffer{}
	keyWriter := util.BorrowJSONWriter(keyBuf)
	valueWriter := util.BorrowJSONWriter(valueBuf)

	keyWriter.WriteObject(func() {
		keyWriter.WriteUint64Field("ts", e.CommitTs)
		keyWriter.WriteStringField("scm", e.TableInfo.GetSchemaName())
		keyWriter.WriteStringField("tbl", e.TableInfo.GetTableName())
		keyWriter.WriteIntField("t", int(model.MessageTypeRow))

		if claimCheckLocationName != "" {
			keyWriter.WriteBoolField("ohk", false) // 不知道啥用
			keyWriter.WriteStringField("ccl", claimCheckLocationName)
		}
	})
	var err error
	if e.IsDelete() {
		onlyHandleKeyColumns := config.DeleteOnlyHandleKeyColumns || largeMessageOnlyHandleKeyColumns

		valueWriter.WriteArrayField("d", func() {
			err = writeColumnFieldValues(valueWriter, e.GetPreColumns(), onlyHandleKeyColumns)
		})
		if err != nil {
			return nil, nil, 0, err
		}
	} else if e.IsInsert() {
		valueWriter.WriteArrayField("u", func() {
			err = writeColumnFieldValues(valueWriter, e.GetColumns(), largeMessageOnlyHandleKeyColumns)
		})
		if err != nil {
			return nil, nil, 0, err
		}
	} else if e.IsUpdate() {
		valueWriter.WriteArrayField("u", func() {
			err = writeColumnFieldValues(valueWriter, e.GetColumns(), largeMessageOnlyHandleKeyColumns)
		})
		if err != nil {
			return nil, nil, 0, err
		}
		valueWriter.WriteArrayField("p", func() {
			err = writeColumnFieldValues(valueWriter, e.GetPreColumns(), largeMessageOnlyHandleKeyColumns)
		})
		if err != nil {
			return nil, nil, 0, err
		}
	}

	util.ReturnJSONWriter(keyWriter)
	util.ReturnJSONWriter(valueWriter)

	value, err := ticommon.Compress(
		config.ChangefeedID, config.LargeMessageHandle.LargeMessageHandleCompression, valueBuf.Bytes(),
	)

	if err != nil {
		return nil, nil, 0, err
	}

	key := keyBuf.Bytes()

	// for single message that is longer than max-message-bytes
	// 16 is the length of `keyLenByte` and `valueLenByte`, 8 is the length of `versionHead`
	length := len(key) + len(value) + ticommon.MaxRecordOverhead + 16 + 8

	return key, value, length, nil
}

func encodeDDLEvent(e *common.DDLEvent, config *ticommon.Config) ([]byte, []byte, error) {
	keyBuf := &bytes.Buffer{}
	valueBuf := &bytes.Buffer{}
	keyWriter := util.BorrowJSONWriter(keyBuf)
	valueWriter := util.BorrowJSONWriter(valueBuf)

	keyWriter.WriteObject(func() {
		keyWriter.WriteUint64Field("ts", e.CommitTS)
		keyWriter.WriteStringField("scm", e.Job.SchemaName)
		keyWriter.WriteStringField("tbl", e.Job.TableName)
		keyWriter.WriteIntField("t", int(model.MessageTypeDDL))
	})

	valueWriter.WriteObject(func() {
		valueWriter.WriteStringField("q", e.Job.Query)
		valueWriter.WriteStringField("t", string(e.Job.Type))
	})

	util.ReturnJSONWriter(keyWriter)
	util.ReturnJSONWriter(valueWriter)

	value, err := ticommon.Compress(
		config.ChangefeedID, config.LargeMessageHandle.LargeMessageHandleCompression, valueBuf.Bytes(),
	)
	if err != nil {
		return nil, nil, err
	}

	key := keyBuf.Bytes()

	var keyLenByte [8]byte
	var valueLenByte [8]byte
	var versionByte [8]byte

	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	binary.BigEndian.PutUint64(valueLenByte[:], uint64(len(value)))
	binary.BigEndian.PutUint64(versionByte[:], codec.BatchVersion1)

	keyOutput := new(bytes.Buffer)
	keyOutput.Write(versionByte[:])
	keyOutput.Write(keyLenByte[:])
	keyOutput.Write(key)

	valueOutput := new(bytes.Buffer)
	valueOutput.Write(versionByte[:])
	valueOutput.Write(value)

	return keyOutput.Bytes(), valueOutput.Bytes(), nil
}

func encodeResolvedTs(ts uint64) ([]byte, []byte, error) {
	keyBuf := &bytes.Buffer{}
	keyWriter := util.BorrowJSONWriter(keyBuf)

	keyWriter.WriteObject(func() {
		keyWriter.WriteUint64Field("ts", ts)
		keyWriter.WriteIntField("t", int(model.MessageTypeResolved))
	})

	util.ReturnJSONWriter(keyWriter)

	key := keyBuf.Bytes()

	var keyLenByte [8]byte
	var valueLenByte [8]byte
	var versionByte [8]byte
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	binary.BigEndian.PutUint64(valueLenByte[:], 0)
	binary.BigEndian.PutUint64(versionByte[:], encoder.BatchVersion1)

	keyOutput := new(bytes.Buffer)

	keyOutput.Write(versionByte[:])
	keyOutput.Write(keyLenByte[:])
	keyOutput.Write(key)

	valueOutput := new(bytes.Buffer)
	valueOutput.Write(valueLenByte[:])

	return keyOutput.Bytes(), valueOutput.Bytes(), nil
}

func writeColumnFieldValue(writer *util.JSONWriter, col *common.Column) error {
	colType := col.Type
	whereHandle := col.Flag.IsHandleKey()
	flag := col.Flag

	writer.WriteStringField("t", string(colType)) // todo:please check performance
	writer.WriteBoolField("h", whereHandle)
	writer.WriteUint64Field("f", uint64(flag))
	if col.Value == nil {
		writer.WriteNullField("v")
		return nil
	} else {
		switch col.Type {
		case mysql.TypeBit:
			v, ok := col.Value.(int64)
			if !ok {
				return cerror.ErrOpenProtocolCodecInvalidData.GenWithStack(
					"unexpected column value type %T for bit column %s",
					col.Value,
					col.Name)
			}
			writer.WriteInt64Field("v", v)
			return nil
		case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
			v, ok := col.Value.([]byte)
			if !ok {
				return cerror.ErrOpenProtocolCodecInvalidData.GenWithStack(
					"unexpected column value type %T for blob string column %s",
					col.Value,
					col.Name)
			}
			writer.WriteBase64StringField(col.Name, v)
			return nil
		case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString:
			v, ok := col.Value.(string)
			if !ok {
				return cerror.ErrOpenProtocolCodecInvalidData.GenWithStack(
					"unexpected column value type %T for string column %s",
					col.Value,
					col.Name)
			}
			writer.WriteStringField(col.Name, v)
			return nil
		case mysql.TypeEnum, mysql.TypeSet:
			v, ok := col.Value.(int64)
			if !ok {
				return cerror.ErrOpenProtocolCodecInvalidData.GenWithStack(
					"unexpected column value type %T for enum column %s",
					col.Value,
					col.Name)
			}
			writer.WriteInt64Field(col.Name, v)
		case mysql.TypeNewDecimal:
			v, ok := col.Value.(string)
			if !ok {
				return cerror.ErrOpenProtocolCodecInvalidData.GenWithStack(
					"unexpected column value type %T for decimal column %s",
					col.Value,
					col.Name)
			}

			writer.WriteStringField(col.Name, v)
			return nil
		case mysql.TypeFloat, mysql.TypeDouble:
			v, ok := col.Value.(float64)
			if !ok {
				return cerror.ErrOpenProtocolCodecInvalidData.GenWithStack(
					"unexpected column value type %T for float/double column %s",
					col.Value,
					col.Name)
			}

			writer.WriteFloat64Field(col.Name, v)
			return nil
		case mysql.TypeYear:
			v, ok := col.Value.(uint64)
			if !ok {
				return cerror.ErrOpenProtocolCodecInvalidData.GenWithStack(
					"unexpected column value type %T for year column %s",
					col.Value,
					col.Name)
			}
			writer.WriteUint64Field(col.Name, v)
			return nil
		case mysql.TypeLonglong:
			v, ok := col.Value.(uint64)
			if !ok {
				return cerror.ErrOpenProtocolCodecInvalidData.GenWithStack(
					"unexpected column value type %T for long long column %s",
					col.Value,
					col.Name)
			}
			writer.WriteUint64Field(col.Name, v)
			return nil
		default:
			writer.WriteAnyField(col.Name, col.Value)
		}
	}
	return nil
}

func writeColumnFieldValues(jWriter *util.JSONWriter, cols []*common.Column, onlyHandleKeyColumns bool) error {
	flag := false // flag to check if any column is written
	for _, col := range cols {
		if onlyHandleKeyColumns && !col.Flag.IsHandleKey() {
			continue
		}
		flag = true
		jWriter.WriteObjectField(col.Name, func() {
			writeColumnFieldValue(jWriter, col)
		})
	}
	if !flag {
		return cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found handle key columns for the delete event")
	}
	return nil
}
