package open

import (
	"bytes"
	"encoding/binary"
	"strconv"

	"github.com/flowbehappy/tigate/pkg/common"
	newcommon "github.com/flowbehappy/tigate/pkg/sink/codec/common"
	"github.com/flowbehappy/tigate/pkg/sink/codec/encoder"
	"github.com/flowbehappy/tigate/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	ticommon "github.com/pingcap/tiflow/pkg/sink/codec/common"
)

func encodeRowChangedEvent(e *common.RowEvent, config *newcommon.Config, largeMessageOnlyHandleKeyColumns bool, claimCheckLocationName string) ([]byte, []byte, int, error) {
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
		valueWriter.WriteObject(func() {
			valueWriter.WriteObjectField("d", func() {
				err = writeColumnFieldValues(valueWriter, e.GetPreRows(), e.TableInfo, e.ColumnSelector, onlyHandleKeyColumns)
			})
		})
	} else if e.IsInsert() {
		valueWriter.WriteObject(func() {
			valueWriter.WriteObjectField("u", func() {
				err = writeColumnFieldValues(valueWriter, e.GetRows(), e.TableInfo, e.ColumnSelector, largeMessageOnlyHandleKeyColumns)
			})
		})
	} else if e.IsUpdate() {
		valueWriter.WriteObject(func() {
			valueWriter.WriteObjectField("u", func() {
				err = writeColumnFieldValues(valueWriter, e.GetRows(), e.TableInfo, e.ColumnSelector, largeMessageOnlyHandleKeyColumns)
			})
			if err != nil {
				return
			}
			if !config.OnlyOutputUpdatedColumns {
				valueWriter.WriteObjectField("p", func() {
					err = writeColumnFieldValues(valueWriter, e.GetPreRows(), e.TableInfo, e.ColumnSelector, largeMessageOnlyHandleKeyColumns)
				})
			} else {
				valueWriter.WriteObjectField("p", func() {
					writeUpdatedColumnFieldValues(valueWriter, e.GetPreRows(), e.GetRows(), e.TableInfo, e.ColumnSelector, largeMessageOnlyHandleKeyColumns)
				})
			}
		})
	}

	util.ReturnJSONWriter(keyWriter)
	util.ReturnJSONWriter(valueWriter)

	if err != nil {
		return nil, nil, 0, err
	}

	key := keyBuf.Bytes()
	value := valueBuf.Bytes()

	valueCompressed, err := ticommon.Compress(
		config.ChangefeedID, config.LargeMessageHandle.LargeMessageHandleCompression, value,
	)
	if err != nil {
		return nil, nil, 0, err
	}

	// for single message that is longer than max-message-bytes
	// 16 is the length of `keyLenByte` and `valueLenByte`, 8 is the length of `versionHead`
	length := len(key) + len(valueCompressed) + ticommon.MaxRecordOverhead + 16 + 8

	return key, valueCompressed, length, nil
}

func encodeDDLEvent(e *common.DDLEvent, config *newcommon.Config) ([]byte, []byte, error) {
	keyBuf := &bytes.Buffer{}
	valueBuf := &bytes.Buffer{}
	keyWriter := util.BorrowJSONWriter(keyBuf)
	valueWriter := util.BorrowJSONWriter(valueBuf)

	keyWriter.WriteObject(func() {
		keyWriter.WriteUint64Field("ts", e.FinishedTs)
		keyWriter.WriteStringField("scm", e.SchemaName)
		keyWriter.WriteStringField("tbl", e.TableName)
		keyWriter.WriteIntField("t", int(model.MessageTypeDDL))
	})

	valueWriter.WriteObject(func() {
		valueWriter.WriteStringField("q", e.Query)
		valueWriter.WriteIntField("t", int(e.Type))
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

	writer.WriteIntField("t", int(colType)) // todo:please check performance
	if whereHandle {
		writer.WriteBoolField("h", whereHandle)
	}
	writer.WriteUint64Field("f", uint64(flag))

	if row.IsNull(idx) {
		writer.WriteNullField("v")
		return nil
	}

	switch col.GetType() {
	case mysql.TypeBit:
		d := row.GetDatum(idx, &col.FieldType)
		if d.IsNull() {
			writer.WriteNullField("v")
		} else {
			dp := &d
			// Encode bits as integers to avoid pingcap/tidb#10988 (which also affects MySQL itself)
			value, err := dp.GetBinaryLiteral().ToInt(types.DefaultStmtNoWarningContext)
			if err != nil {
				return nil
			}
			writer.WriteUint64Field("v", value)
		}
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		value := row.GetBytes(idx)
		if len(value) == 0 {
			writer.WriteNullField("v")
		} else {
			writer.WriteBase64StringField("v", value)
		}
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString:
		value := row.GetBytes(idx)
		if len(value) == 0 {
			writer.WriteNullField("v")
		} else {
			if flag.IsBinary() {
				str := string(value)
				str = strconv.Quote(str)
				str = str[1 : len(str)-1]
				writer.WriteStringField("v", str)
			} else {
				writer.WriteStringField("v", string(value))
			}
		}
	case mysql.TypeEnum, mysql.TypeSet:
		value := row.GetEnum(idx).Value
		if value == 0 {
			writer.WriteNullField("v")
		} else {
			writer.WriteUint64Field("v", value)
		}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		value := row.GetTime(idx)
		if value.IsZero() {
			writer.WriteNullField("v")
		} else {
			writer.WriteStringField("v", value.String())
		}
	case mysql.TypeDuration:
		value := row.GetDuration(idx, 0)
		if value.ToNumber().IsZero() {
			writer.WriteNullField("v")
		} else {
			writer.WriteStringField("v", value.String())
		}
	case mysql.TypeJSON:
		value := row.GetJSON(idx)
		if value.IsZero() {
			writer.WriteNullField("v")
		} else {
			writer.WriteStringField("v", value.String())
		}
	case mysql.TypeNewDecimal:
		value := row.GetMyDecimal(idx)
		if value.IsZero() {
			writer.WriteNullField("v")
		} else {
			writer.WriteStringField("v", value.String())
		}
	default:
		d := row.GetDatum(idx, &col.FieldType)
		// NOTICE: GetValue() may return some types that go sql not support, which will cause sink DML fail
		// Make specified convert upper if you need
		// Go sql support type ref to: https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
		value := d.GetValue()
		writer.WriteAnyField("v", value)
	}
	return nil
}

func writeColumnFieldValues(
	jWriter *util.JSONWriter,
	row *chunk.Row,
	tableInfo *common.TableInfo,
	selector common.Selector,
	onlyHandleKeyColumns bool,
) error {
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

func writeUpdatedColumnFieldValues(
	jWriter *util.JSONWriter,
	preRow *chunk.Row,
	row *chunk.Row,
	tableInfo *common.TableInfo,
	selector common.Selector,
	onlyHandleKeyColumns bool,
) {
	// we don't need check here whether after column selector there still exists handle key column
	// because writeUpdatedColumnFieldValues only can be called after successfully dealing with one row event
	colInfo := tableInfo.Columns

	for idx, col := range colInfo {
		if selector.Select(col) {
			if onlyHandleKeyColumns && !tableInfo.ColumnsFlag[col.ID].IsHandleKey() {
				continue
			}
			writeColumnFieldValueIfUpdated(jWriter, col, preRow, row, idx, tableInfo)
		}
	}
}

func writeColumnFieldValueIfUpdated(
	writer *util.JSONWriter,
	col *timodel.ColumnInfo,
	preRow *chunk.Row,
	row *chunk.Row,
	idx int,
	tableInfo *common.TableInfo,
) error {
	colType := col.GetType()
	flag := *tableInfo.ColumnsFlag[col.ID]
	whereHandle := flag.IsHandleKey()

	writeFunc := func(writeColumnValue func()) {
		writer.WriteObjectField(col.Name.O, func() {
			writer.WriteIntField("t", int(colType))
			if whereHandle {
				writer.WriteBoolField("h", whereHandle)
			}
			writer.WriteUint64Field("f", uint64(flag))
			writeColumnValue()
		})
	}

	if row.IsNull(idx) && preRow.IsNull(idx) {
		return nil
	} else if preRow.IsNull(idx) && !row.IsNull(idx) {
		writeFunc(func() { writer.WriteNullField("v") })
		return nil
	} else if !preRow.IsNull(idx) && row.IsNull(idx) {
		return writeColumnFieldValue(writer, col, preRow, idx, tableInfo)
	}

	switch col.GetType() {
	case mysql.TypeBit:
		rowDatum := row.GetDatum(idx, &col.FieldType)
		rowDatumPoint := &rowDatum
		// Encode bits as integers to avoid pingcap/tidb#10988 (which also affects MySQL itself)
		rowValue, _ := rowDatumPoint.GetBinaryLiteral().ToInt(types.DefaultStmtNoWarningContext)

		preRowDatum := row.GetDatum(idx, &col.FieldType)
		preRowDatumPoint := &preRowDatum
		// Encode bits as integers to avoid pingcap/tidb#10988 (which also affects MySQL itself)
		preRowValue, _ := preRowDatumPoint.GetBinaryLiteral().ToInt(types.DefaultStmtNoWarningContext)
		// if err != nil {
		// 	return false, err
		// }

		if rowValue != preRowValue {
			writeFunc(func() { writer.WriteUint64Field("v", preRowValue) })
		}
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		rowValue := row.GetBytes(idx)
		preRowValue := preRow.GetBytes(idx)
		if !bytes.Equal(rowValue, preRowValue) {
			if len(preRowValue) == 0 {
				writeFunc(func() { writer.WriteNullField("v") })
			} else {
				writeFunc(func() { writer.WriteBase64StringField("v", preRowValue) })
			}
		}
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString:
		rowValue := row.GetBytes(idx)
		preRowValue := preRow.GetBytes(idx)
		if !bytes.Equal(rowValue, preRowValue) {
			if len(preRowValue) == 0 {
				writeFunc(func() { writer.WriteNullField("v") })
			} else {
				if flag.IsBinary() {
					str := string(preRowValue)
					str = strconv.Quote(str)
					str = str[1 : len(str)-1]
					writeFunc(func() { writer.WriteStringField("v", str) })
				} else {
					writeFunc(func() { writer.WriteStringField("v", string(preRowValue)) })
				}
			}
		}
	case mysql.TypeEnum, mysql.TypeSet:
		rowValue := row.GetEnum(idx).Value
		preRowValue := preRow.GetEnum(idx).Value
		if rowValue != preRowValue {
			if preRowValue == 0 {
				writeFunc(func() { writer.WriteNullField("v") })
			} else {
				writeFunc(func() { writer.WriteUint64Field("v", preRowValue) })
			}
		}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		rowValue := row.GetTime(idx)
		preRowValue := preRow.GetTime(idx)
		if rowValue != preRowValue {
			if preRowValue.IsZero() {
				writeFunc(func() { writer.WriteNullField("v") })
			} else {
				writeFunc(func() { writer.WriteStringField("v", preRowValue.String()) })
			}
		}
	case mysql.TypeDuration:
		rowValue := row.GetDuration(idx, 0)
		preRowValue := preRow.GetDuration(idx, 0)
		if rowValue != preRowValue {
			if preRowValue.ToNumber().IsZero() {
				writeFunc(func() { writer.WriteNullField("v") })
			} else {
				writeFunc(func() { writer.WriteStringField("v", preRowValue.String()) })
			}
		}
	case mysql.TypeJSON:
		rowValue := row.GetJSON(idx).String()
		preRowValue := preRow.GetJSON(idx).String()
		if rowValue != preRowValue {
			if preRow.GetJSON(idx).IsZero() {
				writeFunc(func() { writer.WriteNullField("v") })
			} else {
				writeFunc(func() { writer.WriteStringField("v", preRowValue) })
			}
		}
	case mysql.TypeNewDecimal:
		rowValue := row.GetMyDecimal(idx)
		preValue := preRow.GetMyDecimal(idx)
		if rowValue.Compare(preValue) != 0 {
			if preValue.IsZero() {
				writeFunc(func() { writer.WriteNullField("v") })
			} else {
				writeFunc(func() { writer.WriteStringField("v", preValue.String()) })
			}
		}
	default:
		rowDatum := row.GetDatum(idx, &col.FieldType)
		// NOTICE: GetValue() may return some types that go sql not support, which will cause sink DML fail
		// Make specified convert upper if you need
		// Go sql support type ref to: https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
		rowValue := rowDatum.GetValue()

		preRowDatum := preRow.GetDatum(idx, &col.FieldType)
		preRowValue := preRowDatum.GetValue()

		if rowValue != preRowValue {
			writeFunc(func() { writer.WriteAnyField("v", preRowValue) })
		}
	}
	return nil

}
