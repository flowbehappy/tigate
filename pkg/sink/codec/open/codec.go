package open

import (
	"bytes"
	"encoding/binary"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/sink/codec/encoder"
	"github.com/flowbehappy/tigate/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	ticommon "github.com/pingcap/tiflow/pkg/sink/codec/common"
)

func encodeRowChangedEvent(e *common.RowEvent, config *ticommon.Config, largeMessageOnlyHandleKeyColumns bool, claimCheckLocationName string) ([]byte, []byte, int, error) {
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
			err = writeColumnFieldValues(valueWriter, e.GetPreRows(), e.TableInfo, e.ColumnSelector, onlyHandleKeyColumns)
		})
		if err != nil {
			return nil, nil, 0, err
		}
	} else if e.IsInsert() {
		valueWriter.WriteArrayField("u", func() {
			err = writeColumnFieldValues(valueWriter, e.GetRows(), e.TableInfo, e.ColumnSelector, largeMessageOnlyHandleKeyColumns)
		})
		if err != nil {
			return nil, nil, 0, err
		}
	} else if e.IsUpdate() {
		valueWriter.WriteArrayField("u", func() {
			err = writeColumnFieldValues(valueWriter, e.GetRows(), e.TableInfo, e.ColumnSelector, largeMessageOnlyHandleKeyColumns)
		})
		if err != nil {
			return nil, nil, 0, err
		}
		valueWriter.WriteArrayField("p", func() {
			err = writeColumnFieldValues(valueWriter, e.GetPreRows(), e.TableInfo, e.ColumnSelector, largeMessageOnlyHandleKeyColumns)
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

func writeColumnFieldValue(writer *util.JSONWriter, col *timodel.ColumnInfo, row *chunk.Row, idx int, tableInfo *common.TableInfo) error {
	colType := col.GetType()
	flag := *tableInfo.ColumnsFlag[col.ID]
	whereHandle := flag.IsHandleKey()

	writer.WriteStringField("t", string(colType)) // todo:please check performance
	writer.WriteBoolField("h", whereHandle)
	writer.WriteUint64Field("f", uint64(flag))

	// TODO:deal with nil
	switch col.GetType() {
	case mysql.TypeBit:
		d := row.GetDatum(idx, &col.FieldType)
		dp := &d
		// Encode bits as integers to avoid pingcap/tidb#10988 (which also affects MySQL itself)
		value, err := dp.GetBinaryLiteral().ToInt(types.DefaultStmtNoWarningContext)
		if err != nil {
			return nil
		}
		writer.WriteUint64Field(col.Name.O, value)
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		value := row.GetBytes(idx)
		if value == nil {
			value = common.EmptyBytes
		}
		writer.WriteBase64StringField(col.Name.O, value)
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString:
		value := row.GetBytes(idx)
		if value == nil {
			value = common.EmptyBytes
		}
		writer.WriteStringField(col.Name.O, string(hack.String(value)))
	case mysql.TypeEnum, mysql.TypeSet:
		value := row.GetEnum(idx).Value
		writer.WriteUint64Field(col.Name.O, value)
	case mysql.TypeNewDecimal:
		d := row.GetMyDecimal(idx)
		value := d.String()
		writer.WriteStringField(col.Name.O, value)
	case mysql.TypeFloat:
		value := row.GetFloat32(idx)
		writer.WriteFloat32Field(col.Name.O, value)
	case mysql.TypeDouble:
		value := row.GetFloat64(idx)
		writer.WriteFloat64Field(col.Name.O, value)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		value := row.GetTime(idx).String()
		writer.WriteStringField(col.Name.O, value)
	case mysql.TypeDuration:
		value := row.GetDuration(idx, 0).String()
		writer.WriteStringField(col.Name.O, value)
	case mysql.TypeJSON:
		value := row.GetJSON(idx).String()
		writer.WriteStringField(col.Name.O, value)
	default:
		d := row.GetDatum(idx, &col.FieldType)
		// NOTICE: GetValue() may return some types that go sql not support, which will cause sink DML fail
		// Make specified convert upper if you need
		// Go sql support type ref to: https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
		value := d.GetValue()
		writer.WriteAnyField(col.Name.O, value)
	}
	return nil
}

func writeColumnFieldValues(jWriter *util.JSONWriter, row *chunk.Row, tableInfo *common.TableInfo, selector *common.Selector, onlyHandleKeyColumns bool) error {
	flag := false // flag to check if any column is written

	colInfo := tableInfo.Columns

	for idx, col := range colInfo {
		if selector.Select(col) {
			if onlyHandleKeyColumns && !tableInfo.ColumnsFlag[col.ID].IsHandleKey() {
				continue
			}
			flag = true
			jWriter.WriteObjectField(col.Name.O, func() {
				writeColumnFieldValue(jWriter, col, row, idx, tableInfo)
			})
		}

	}
	if !flag {
		return cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found handle key columns for the delete event")
	}
	return nil
}
