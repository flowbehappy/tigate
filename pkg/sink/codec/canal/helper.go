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

package canal

import (
	"fmt"
	"math"
	"strconv"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/codec/internal" // nolint:staticcheck
	mm "github.com/pingcap/tidb/pkg/meta/model"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	canal "github.com/pingcap/tiflow/proto/canal"
)

func formatColumnValue(row *chunk.Row, idx int, columnInfo *timodel.ColumnInfo, flag *common.ColumnFlagType) (string, internal.JavaSQLType, error) {
	colType := columnInfo.GetType()

	var value string
	var javaType internal.JavaSQLType

	switch colType {
	case mysql.TypeBit:
		javaType = internal.JavaSQLTypeBIT
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			uintValue, err := d.GetMysqlBit().ToInt(types.DefaultStmtNoWarningContext)
			if err != nil {
				return "", 0, err
			}
			value = strconv.FormatUint(uintValue, 10)
		}
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		bytesValue := row.GetBytes(idx)
		if flag.IsBinary() {
			javaType = internal.JavaSQLTypeBLOB
		} else {
			javaType = internal.JavaSQLTypeCLOB
		}
		if string(bytesValue) == "" {
			value = "null"
			break
		}

		if flag.IsBinary() {
			javaType = internal.JavaSQLTypeBLOB
			decoded, err := bytesDecoder.Bytes(bytesValue)
			if err != nil {
				return "", 0, err
			}
			value = string(decoded)
		} else {
			javaType = internal.JavaSQLTypeCLOB
			value = string(bytesValue)
		}
	case mysql.TypeVarchar, mysql.TypeVarString:
		if flag.IsBinary() {
			javaType = internal.JavaSQLTypeBLOB
		} else {
			javaType = internal.JavaSQLTypeVARCHAR
		}
		value = string(row.GetBytes(idx))
		if value == "" {
			value = "null"
		}
	case mysql.TypeString:
		if flag.IsBinary() {
			javaType = internal.JavaSQLTypeBLOB
		} else {
			javaType = internal.JavaSQLTypeCHAR
		}
		value = string(row.GetBytes(idx))
		if value == "" {
			value = "null"
		}
	case mysql.TypeEnum:
		javaType = internal.JavaSQLTypeINTEGER
		enumValue := row.GetEnum(idx).Value
		if enumValue == 0 {
			value = "null"
		} else {
			value = fmt.Sprintf("%d", enumValue)
		}
	case mysql.TypeSet:
		javaType = internal.JavaSQLTypeBIT
		bitValue := row.GetEnum(idx).Value
		if bitValue == 0 {
			value = "null"
		} else {
			value = fmt.Sprintf("%d", bitValue)
		}
	case mysql.TypeDate, mysql.TypeNewDate:
		javaType = internal.JavaSQLTypeDATE
		timeValue := row.GetTime(idx)
		if timeValue.IsZero() {
			value = "null"
		} else {
			value = timeValue.String()
		}
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		javaType = internal.JavaSQLTypeTIMESTAMP
		timeValue := row.GetTime(idx)
		if timeValue.IsZero() {
			value = "null"
		} else {
			value = timeValue.String()
		}
	case mysql.TypeDuration:
		javaType = internal.JavaSQLTypeTIME
		durationValue := row.GetDuration(idx, 0)
		if durationValue.ToNumber().IsZero() {
			value = "null"
		} else {
			value = durationValue.String()
		}
	case mysql.TypeJSON:
		javaType = internal.JavaSQLTypeVARCHAR
		// json needs null check before, otherwise it will panic.
		if row.IsNull(idx) {
			value = "null"
		} else {
			jsonValue := row.GetJSON(idx)
			if jsonValue.IsZero() {
				value = "null"
			} else {
				value = jsonValue.String()
			}
		}
	case mysql.TypeNewDecimal:
		javaType = internal.JavaSQLTypeDECIMAL
		decimalValue := row.GetMyDecimal(idx)
		if decimalValue.IsZero() {
			value = "null"
		} else {
			value = decimalValue.String()
		}
	case mysql.TypeInt24:
		javaType = internal.JavaSQLTypeINTEGER
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			if flag.IsUnsigned() {
				uintValue := d.GetUint64()
				value = strconv.FormatUint(uintValue, 10)
			} else {
				intValue := d.GetInt64()
				value = strconv.FormatInt(intValue, 10)
			}
		}
	case mysql.TypeTiny:
		javaType = internal.JavaSQLTypeTINYINT
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			if flag.IsUnsigned() {
				uintValue := d.GetUint64()
				if uintValue > math.MaxInt8 {
					javaType = internal.JavaSQLTypeSMALLINT
				}
				value = strconv.FormatUint(uintValue, 10)
			} else {
				intValue := d.GetInt64()
				value = strconv.FormatInt(intValue, 10)
			}
		}
	case mysql.TypeShort:
		javaType = internal.JavaSQLTypeSMALLINT
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			if flag.IsUnsigned() {
				uintValue := d.GetUint64()
				if uintValue > math.MaxInt16 {
					javaType = internal.JavaSQLTypeINTEGER
				}
				value = strconv.FormatUint(uintValue, 10)
			} else {
				intValue := d.GetInt64()
				value = strconv.FormatInt(intValue, 10)
			}
		}
	case mysql.TypeLong:
		javaType = internal.JavaSQLTypeINTEGER
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			if flag.IsUnsigned() {
				uintValue := d.GetUint64()
				if uintValue > math.MaxInt32 {
					javaType = internal.JavaSQLTypeBIGINT
				}
				value = strconv.FormatUint(uintValue, 10)
			} else {
				intValue := d.GetInt64()
				value = strconv.FormatInt(intValue, 10)
			}
		}
	case mysql.TypeLonglong:
		javaType = internal.JavaSQLTypeBIGINT
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			if flag.IsUnsigned() {
				uintValue := d.GetUint64()
				if uintValue > math.MaxInt64 {
					javaType = internal.JavaSQLTypeDECIMAL
				}
				value = strconv.FormatUint(uintValue, 10)
			} else {
				intValue := d.GetInt64()
				value = strconv.FormatInt(intValue, 10)
			}
		}
	case mysql.TypeFloat:
		javaType = internal.JavaSQLTypeREAL
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			floatValue := d.GetFloat32()
			value = strconv.FormatFloat(float64(floatValue), 'f', -1, 32)
		}
	case mysql.TypeDouble:
		javaType = internal.JavaSQLTypeDOUBLE
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			floatValue := d.GetFloat64()
			value = strconv.FormatFloat(floatValue, 'f', -1, 64)
		}
	// case mysql.TypeNull:
	// 	javaType = internal.JavaSQLTypeNULL
	// 	value = "null"
	case mysql.TypeYear:
		javaType = internal.JavaSQLTypeVARCHAR
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			yearValue := d.GetInt64()
			value = strconv.FormatInt(yearValue, 10)
		}

	default:
		javaType = internal.JavaSQLTypeVARCHAR
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			// NOTICE: GetValue() may return some types that go sql not support, which will cause sink DML fail
			// Make specified convert upper if you need
			// Go sql support type ref to: https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
			value = fmt.Sprintf("%v", d.GetValue())
		}
	}
	return value, javaType, nil
}

// convert ts in tidb to timestamp(in ms) in canal
func convertToCanalTs(commitTs uint64) int64 {
	return int64(commitTs >> 18)
}

// get the canal EventType according to the DDLEvent
func convertDdlEventType(e *commonEvent.DDLEvent) canal.EventType {
	// see https://github.com/alibaba/canal/blob/d53bfd7ee76f8fe6eb581049d64b07d4fcdd692d/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/ddl/DruidDdlParser.java#L59-L178
	switch mm.ActionType(e.Type) {
	case mm.ActionCreateSchema, mm.ActionDropSchema, mm.ActionShardRowID, mm.ActionCreateView,
		mm.ActionDropView, mm.ActionRecoverTable, mm.ActionModifySchemaCharsetAndCollate,
		mm.ActionLockTable, mm.ActionUnlockTable, mm.ActionRepairTable, mm.ActionSetTiFlashReplica,
		mm.ActionUpdateTiFlashReplicaStatus, mm.ActionCreateSequence, mm.ActionAlterSequence,
		mm.ActionDropSequence, mm.ActionModifyTableAutoIDCache, mm.ActionRebaseAutoRandomBase:
		return canal.EventType_QUERY
	case mm.ActionCreateTable:
		return canal.EventType_CREATE
	case mm.ActionRenameTable, mm.ActionRenameTables:
		return canal.EventType_RENAME
	case mm.ActionAddIndex, mm.ActionAddForeignKey, mm.ActionAddPrimaryKey:
		return canal.EventType_CINDEX
	case mm.ActionDropIndex, mm.ActionDropForeignKey, mm.ActionDropPrimaryKey:
		return canal.EventType_DINDEX
	case mm.ActionAddColumn, mm.ActionDropColumn, mm.ActionModifyColumn, mm.ActionRebaseAutoID,
		mm.ActionSetDefaultValue, mm.ActionModifyTableComment, mm.ActionRenameIndex, mm.ActionAddTablePartition,
		mm.ActionDropTablePartition, mm.ActionModifyTableCharsetAndCollate, mm.ActionTruncateTablePartition,
		mm.ActionAlterIndexVisibility, mm.ActionMultiSchemaChange, mm.ActionReorganizePartition,
		mm.ActionAlterTablePartitioning, mm.ActionRemovePartitioning,
		// AddColumns and DropColumns are removed in TiDB v6.2.0, see https://github.com/pingcap/tidb/pull/35862.
		mm.ActionAddColumns, mm.ActionDropColumns:
		return canal.EventType_ALTER
	case mm.ActionDropTable:
		return canal.EventType_ERASE
	case mm.ActionTruncateTable:
		return canal.EventType_TRUNCATE
	default:
		return canal.EventType_QUERY
	}
}
