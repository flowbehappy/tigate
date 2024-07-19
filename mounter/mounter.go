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

package mounter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tiflow/pkg/spanz"
	"go.uber.org/zap"
)

type baseKVEntry struct {
	StartTs uint64
	// Commit or resolved TS
	CRTs uint64

	PhysicalTableID int64
	RecordID        kv.Handle
	Delete          bool
}

type rowKVEntry struct {
	baseKVEntry
	Row    map[int64]types.Datum
	PreRow map[int64]types.Datum

	// In some cases, row data may exist but not contain any Datum,
	// use this RowExist/PreRowExist variable to distinguish between row data that does not exist
	// or row data that does not contain any Datum.
	RowExist    bool
	PreRowExist bool
}

// DDLTableInfo contains the tableInfo about tidb_ddl_job and tidb_ddl_history
// and the column id of `job_meta` in these two tables.
type DDLTableInfo struct {
	// ddlJobsTable use to parse all ddl jobs except `create table`
	DDLJobTable *common.TableInfo
	// It holds the column id of `job_meta` in table `tidb_ddl_jobs`.
	JobMetaColumnIDinJobTable int64
	// ddlHistoryTable only use to parse `create table` ddl job
	DDLHistoryTable *common.TableInfo
	// It holds the column id of `job_meta` in table `tidb_ddl_history`.
	JobMetaColumnIDinHistoryTable int64
}

// Mounter is used to parse SQL events from KV events
type Mounter interface {
	DecodeEvent(rawKV *common.RawKVEntry, tableInfo *common.TableInfo) (*common.RowChangedEvent, error)
}

type mounter struct {
	tz *time.Location
	// decoder and preDecoder are used to decode the raw value, also used to extract checksum,
	// they should not be nil after decode at least one event in the row format v2.
	decoder    *rowcodec.DatumMapDecoder
	preDecoder *rowcodec.DatumMapDecoder
}

// NewMounter creates a mounter
func NewMounter(tz *time.Location) Mounter {
	return &mounter{
		tz: tz,
	}
}

func (m *mounter) DecodeEvent(rawKV *common.RawKVEntry, tableInfo *common.TableInfo) (*common.RowChangedEvent, error) {
	return m.unmarshalAndMountRowChanged(rawKV, tableInfo)
}

func (m *mounter) unmarshalAndMountRowChanged(
	raw *common.RawKVEntry,
	tableInfo *common.TableInfo,
) (*common.RowChangedEvent, error) {
	if !bytes.HasPrefix(raw.Key, tablePrefix) {
		return nil, nil
	}
	key, physicalTableID, err := decodeTableID(raw.Key)
	if err != nil {
		return nil, err
	}
	if len(raw.OldValue) == 0 && len(raw.Value) == 0 {
		log.Warn("empty value and old value",
			zap.Any("row", raw))
	}
	baseInfo := baseKVEntry{
		StartTs:         raw.StartTs,
		CRTs:            raw.CRTs,
		PhysicalTableID: physicalTableID,
		Delete:          raw.OpType == common.OpTypeDelete,
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if bytes.HasPrefix(key, recordPrefix) {
		rowKV, err := m.unmarshalRowKVEntry(tableInfo, raw.Key, raw.Value, raw.OldValue, baseInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowKV == nil {
			return nil, nil
		}
		row, _, err := m.mountRowKVEntry(tableInfo, rowKV, raw.ApproximateDataSize())
		if err != nil {
			return nil, err
		}
		// TODO: filter DML event in somewhere
		return row, nil
	}
	return nil, nil
}

func (m *mounter) unmarshalRowKVEntry(
	tableInfo *common.TableInfo,
	rawKey []byte,
	rawValue []byte,
	rawOldValue []byte,
	base baseKVEntry,
) (*rowKVEntry, error) {
	recordID, err := tablecodec.DecodeRowKey(rawKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	base.RecordID = recordID

	var (
		row, preRow           map[int64]types.Datum
		rowExist, preRowExist bool
	)

	row, rowExist, err = m.decodeRow(rawValue, recordID, tableInfo, false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	preRow, preRowExist, err = m.decodeRow(rawOldValue, recordID, tableInfo, true)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &rowKVEntry{
		baseKVEntry: base,
		Row:         row,
		PreRow:      preRow,
		RowExist:    rowExist,
		PreRowExist: preRowExist,
	}, nil
}

func (m *mounter) decodeRow(
	rawValue []byte, recordID kv.Handle, tableInfo *common.TableInfo, isPreColumns bool,
) (map[int64]types.Datum, bool, error) {
	if len(rawValue) == 0 {
		return map[int64]types.Datum{}, false, nil
	}
	handleColIDs, handleColFt, reqCols := tableInfo.GetRowColInfos()
	var (
		datums map[int64]types.Datum
		err    error
	)

	if rowcodec.IsNewFormat(rawValue) {
		decoder := rowcodec.NewDatumMapDecoder(reqCols, m.tz)
		if isPreColumns {
			m.preDecoder = decoder
		} else {
			m.decoder = decoder
		}
		datums, err = decodeRowV2(decoder, rawValue)
	} else {
		datums, err = decodeRowV1(rawValue, tableInfo, m.tz)
	}

	if err != nil {
		return nil, false, errors.Trace(err)
	}

	datums, err = tablecodec.DecodeHandleToDatumMap(
		recordID, handleColIDs, handleColFt, m.tz, datums)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	return datums, true, nil
}

// IsLegacyFormatJob returns true if the job is from the legacy DDL list key.
func IsLegacyFormatJob(rawKV *common.RawKVEntry) bool {
	return bytes.HasPrefix(rawKV.Key, metaPrefix)
}

// ParseDDLJob parses the job from the raw KV entry.
func ParseDDLJob(rawKV *common.RawKVEntry, ddlTableInfo *DDLTableInfo) (*model.Job, error) {
	var v []byte
	var datum types.Datum

	// for test case only
	if bytes.HasPrefix(rawKV.Key, metaPrefix) {
		v = rawKV.Value
		job, err := parseJob(v, rawKV.StartTs, rawKV.CRTs, false)
		if err != nil || job == nil {
			job, err = parseJob(v, rawKV.StartTs, rawKV.CRTs, true)
		}
		return job, err
	}

	recordID, err := tablecodec.DecodeRowKey(rawKV.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tableID := tablecodec.DecodeTableID(rawKV.Key)

	// parse it with tidb_ddl_job
	if tableID == spanz.JobTableID {
		row, err := decodeRow(rawKV.Value, recordID, ddlTableInfo.DDLJobTable, time.UTC)
		if err != nil {
			return nil, errors.Trace(err)
		}
		datum = row[ddlTableInfo.JobMetaColumnIDinJobTable]
		v = datum.GetBytes()

		return parseJob(v, rawKV.StartTs, rawKV.CRTs, false)
	} else if tableID == spanz.JobHistoryID {
		// parse it with tidb_ddl_history
		row, err := decodeRow(rawKV.Value, recordID, ddlTableInfo.DDLHistoryTable, time.UTC)
		if err != nil {
			return nil, errors.Trace(err)
		}
		datum = row[ddlTableInfo.JobMetaColumnIDinHistoryTable]
		v = datum.GetBytes()

		return parseJob(v, rawKV.StartTs, rawKV.CRTs, true)
	}

	return nil, fmt.Errorf("invalid tableID %v in rawKV.Key", tableID)
}

// parseJob unmarshal the job from "v".
// fromHistoryTable is used to distinguish the job is from tidb_dd_job or tidb_ddl_history
// We need to be compatible with the two modes, enable_fast_create_table=on and enable_fast_create_table=off
// When enable_fast_create_table=on, `create table` will only be inserted into tidb_ddl_history after being executed successfully.
// When enable_fast_create_table=off, `create table` just like other ddls will be firstly inserted to tidb_ddl_job,
// and being inserted into tidb_ddl_history after being executed successfully.
// In both two modes, other ddls are all firstly inserted into tidb_ddl_job, and then inserted into tidb_ddl_history after being executed successfully.
//
// To be compatible with these two modes, we will get `create table` ddl from tidb_ddl_history, and all ddls from tidb_ddl_job.
// When enable_fast_create_table=off, for each `create table` ddl we will get twice(once from tidb_ddl_history, once from tidb_ddl_job)
// Because in `handleJob` we will skip the repeated ddls, thus it's ok for us to get `create table` twice.
// Besides, the `create table` from tidb_ddl_job always have a earlier commitTs than from tidb_ddl_history.
// Therefore, we always use the commitTs of ddl from `tidb_ddl_job` as StartTs, which ensures we can get all the dmls.
func parseJob(v []byte, startTs, CRTs uint64, fromHistoryTable bool) (*model.Job, error) {
	var job model.Job
	err := json.Unmarshal(v, &job)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if fromHistoryTable {
		// we only want to get `create table` and `create tables` ddl from tidb_ddl_history, so we just throw out others ddls.
		// We only want the job with `JobStateSynced`, which is means the ddl job is done successfully.
		// Besides, to satisfy the subsequent processing,
		// We need to set the job to be Done to make it will replay in schemaStorage
		if (job.Type != model.ActionCreateTable && job.Type != model.ActionCreateTables) || job.State != model.JobStateSynced {
			return nil, nil
		}
		job.State = model.JobStateDone
	} else {
		// we need to get all ddl job which is done from tidb_ddl_job
		if !job.IsDone() {
			return nil, nil
		}
	}

	// FinishedTS is only set when the job is synced,
	// but we can use the entry's ts here
	job.StartTS = startTs
	// Since ddl in stateDone doesn't contain the FinishedTS,
	// we need to set it as the txn's commit ts.
	job.BinlogInfo.FinishedTS = CRTs
	return &job, nil
}

func datum2Column(
	tableInfo *common.TableInfo, datums map[int64]types.Datum, tz *time.Location,
) ([]*common.Column, []types.Datum, []*model.ColumnInfo, []rowcodec.ColInfo, error) {
	cols := make([]*common.Column, len(tableInfo.RowColumnsOffset))
	rawCols := make([]types.Datum, len(tableInfo.RowColumnsOffset))

	// columnInfos and rowColumnInfos hold different column metadata,
	// they should have the same length and order.
	columnInfos := make([]*model.ColumnInfo, len(tableInfo.RowColumnsOffset))
	rowColumnInfos := make([]rowcodec.ColInfo, len(tableInfo.RowColumnsOffset))

	_, _, extendColumnInfos := tableInfo.GetRowColInfos()

	for idx, colInfo := range tableInfo.Columns {
		colName := colInfo.Name.O
		colID := colInfo.ID
		colDatums, exist := datums[colID]

		var (
			colValue interface{}
			size     int
			warn     string
			err      error
		)
		if exist {
			colValue, size, warn, err = formatColVal(colDatums, colInfo)
		} else {
			colDatums, colValue, size, warn, err = getDefaultOrZeroValue(colInfo, tz)
		}
		if err != nil {
			return nil, nil, nil, nil, errors.Trace(err)
		}
		if warn != "" {
			log.Warn(warn, zap.String("table", tableInfo.TableName.String()),
				zap.String("column", colInfo.Name.String()))
		}

		defaultValue := GetDDLDefaultDefinition(colInfo)
		offset := tableInfo.RowColumnsOffset[colID]
		rawCols[offset] = colDatums
		cols[offset] = &common.Column{
			Name:      colName,
			Type:      colInfo.GetType(),
			Charset:   colInfo.GetCharset(),
			Collation: colInfo.GetCollate(),
			Value:     colValue,
			Default:   defaultValue,
			Flag:      *tableInfo.ColumnsFlag[colID],
			// ApproximateBytes = column data size + column struct size
			ApproximateBytes: size + sizeOfEmptyColumn,
		}
		columnInfos[offset] = colInfo
		rowColumnInfos[offset] = extendColumnInfos[idx]
	}
	return cols, rawCols, columnInfos, rowColumnInfos, nil
}

func (m *mounter) mountRowKVEntry(tableInfo *common.TableInfo, row *rowKVEntry, dataSize int64) (*common.RowChangedEvent, common.RowChangedDatums, error) {
	var (
		rawRow common.RowChangedDatums
		err    error
	)

	// Decode previous columns.
	var (
		preCols    []*common.Column
		preRawCols []types.Datum
	)
	if row.PreRowExist {
		// FIXME(leoppro): using pre table info to mounter pre column datum
		// the pre column and current column in one event may using different table info
		preCols, preRawCols, _, _, err = datum2Column(tableInfo, row.PreRow, m.tz)
		if err != nil {
			return nil, rawRow, errors.Trace(err)
		}
	}

	var (
		cols    []*common.Column
		rawCols []types.Datum
	)
	if row.RowExist {
		cols, rawCols, _, _, err = datum2Column(tableInfo, row.Row, m.tz)
		if err != nil {
			return nil, rawRow, errors.Trace(err)
		}
	}

	rawRow.PreRowDatums = preRawCols
	rawRow.RowDatums = rawCols

	return &common.RowChangedEvent{
		PhysicalTableID: row.PhysicalTableID,

		StartTs:  row.StartTs,
		CommitTs: row.CRTs,

		TableInfo:  tableInfo,
		Columns:    cols,
		PreColumns: preCols,
	}, rawRow, nil
}

var emptyBytes = make([]byte, 0)

const (
	sizeOfEmptyColumn = int(unsafe.Sizeof(common.Column{}))
	sizeOfEmptyBytes  = int(unsafe.Sizeof(emptyBytes))
	sizeOfEmptyString = int(unsafe.Sizeof(""))
)

func sizeOfDatum(d types.Datum) int {
	array := [...]types.Datum{d}
	return int(types.EstimatedMemUsage(array[:], 1))
}

func sizeOfString(s string) int {
	// string data size + string struct size.
	return len(s) + sizeOfEmptyString
}

func sizeOfBytes(b []byte) int {
	// bytes data size + bytes struct size.
	return len(b) + sizeOfEmptyBytes
}

// formatColVal return interface{} need to meet the same requirement as getDefaultOrZeroValue
func formatColVal(datum types.Datum, col *model.ColumnInfo) (
	value interface{}, size int, warn string, err error,
) {
	if datum.IsNull() {
		return nil, 0, "", nil
	}
	switch col.GetType() {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		v := datum.GetMysqlTime().String()
		return v, sizeOfString(v), "", nil
	case mysql.TypeDuration:
		v := datum.GetMysqlDuration().String()
		return v, sizeOfString(v), "", nil
	case mysql.TypeJSON:
		v := datum.GetMysqlJSON().String()
		return v, sizeOfString(v), "", nil
	case mysql.TypeNewDecimal:
		d := datum.GetMysqlDecimal()
		if d == nil {
			// nil takes 0 byte.
			return nil, 0, "", nil
		}
		v := d.String()
		return v, sizeOfString(v), "", nil
	case mysql.TypeEnum:
		v := datum.GetMysqlEnum().Value
		const sizeOfV = unsafe.Sizeof(v)
		return v, int(sizeOfV), "", nil
	case mysql.TypeSet:
		v := datum.GetMysqlSet().Value
		const sizeOfV = unsafe.Sizeof(v)
		return v, int(sizeOfV), "", nil
	case mysql.TypeBit:
		// Encode bits as integers to avoid pingcap/tidb#10988 (which also affects MySQL itself)
		v, err := datum.GetBinaryLiteral().ToInt(types.DefaultStmtNoWarningContext)
		const sizeOfV = unsafe.Sizeof(v)
		return v, int(sizeOfV), "", err
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		b := datum.GetBytes()
		if b == nil {
			b = emptyBytes
		}
		return b, sizeOfBytes(b), "", nil
	case mysql.TypeFloat:
		v := datum.GetFloat32()
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 1) || math.IsInf(float64(v), -1) {
			warn = fmt.Sprintf("the value is invalid in column: %f", v)
			v = 0
		}
		const sizeOfV = unsafe.Sizeof(v)
		return v, int(sizeOfV), warn, nil
	case mysql.TypeDouble:
		v := datum.GetFloat64()
		if math.IsNaN(v) || math.IsInf(v, 1) || math.IsInf(v, -1) {
			warn = fmt.Sprintf("the value is invalid in column: %f", v)
			v = 0
		}
		const sizeOfV = unsafe.Sizeof(v)
		return v, int(sizeOfV), warn, nil
	default:
		// NOTICE: GetValue() may return some types that go sql not support, which will cause sink DML fail
		// Make specified convert upper if you need
		// Go sql support type ref to: https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
		return datum.GetValue(), sizeOfDatum(datum), "", nil
	}
}

// Scenarios when call this function:
// (1) column define default null at creating + insert without explicit column
// (2) alter table add column default xxx + old existing data
// (3) amend + insert without explicit column + alter table add column default xxx
// (4) online DDL drop column + data insert at state delete-only
//
// getDefaultOrZeroValue return interface{} need to meet to require type in
// https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
// Supported type is: nil, basic type(Int, Int8,..., Float32, Float64, String), Slice(uint8), other types not support
// TODO: Check default expr support
func getDefaultOrZeroValue(
	col *model.ColumnInfo, tz *time.Location,
) (types.Datum, any, int, string, error) {
	var (
		d   types.Datum
		err error
	)
	// NOTICE: SHOULD use OriginDefaultValue here, more info pls ref to
	// https://github.com/pingcap/tiflow/issues/4048
	// FIXME: Too many corner cases may hit here, like type truncate, timezone
	// (1) If this column is uk(no pk), will cause data inconsistency in Scenarios(2)
	// (2) If not fix here, will cause data inconsistency in Scenarios(3) directly
	// Ref: https://github.com/pingcap/tidb/blob/d2c352980a43bb593db81fd1db996f47af596d91/table/column.go#L489
	if col.GetOriginDefaultValue() != nil {
		datum := types.NewDatum(col.GetOriginDefaultValue())
		d, err = datum.ConvertTo(types.DefaultStmtNoWarningContext, &col.FieldType)
		if err != nil {
			return d, d.GetValue(), sizeOfDatum(d), "", errors.Trace(err)
		}
		switch col.GetType() {
		case mysql.TypeTimestamp:
			t := d.GetMysqlTime()
			err = t.ConvertTimeZone(time.UTC, tz)
			if err != nil {
				return d, d.GetValue(), sizeOfDatum(d), "", errors.Trace(err)
			}
			d.SetMysqlTime(t)
		}
	} else if !mysql.HasNotNullFlag(col.GetFlag()) {
		// NOTICE: NotNullCheck need do after OriginDefaultValue check, as when TiDB meet "amend + add column default xxx",
		// ref: https://github.com/pingcap/ticdc/issues/3929
		// must use null if TiDB not write the column value when default value is null
		// and the value is null, see https://github.com/pingcap/tidb/issues/9304
		d = types.NewDatum(nil)
	} else {
		switch col.GetType() {
		case mysql.TypeEnum:
			// For enum type, if no default value and not null is set,
			// the default value is the first element of the enum list
			name := col.FieldType.GetElem(0)
			enumValue, err := types.ParseEnumName(col.FieldType.GetElems(), name, col.GetCollate())
			if err != nil {
				return d, nil, 0, "", errors.Trace(err)
			}
			d = types.NewMysqlEnumDatum(enumValue)
		case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
			return d, emptyBytes, sizeOfEmptyBytes, "", nil
		default:
			d = table.GetZeroValue(col)
			if d.IsNull() {
				log.Error("meet unsupported column type", zap.String("columnInfo", col.FieldType.String()))
			}
		}
	}
	v, size, warn, err := formatColVal(d, col)
	return d, v, size, warn, err
}

// GetDDLDefaultDefinition returns the default definition of a column.
func GetDDLDefaultDefinition(col *model.ColumnInfo) interface{} {
	defaultValue := col.GetDefaultValue()
	if defaultValue == nil {
		defaultValue = col.GetOriginDefaultValue()
	}
	defaultDatum := types.NewDatum(defaultValue)
	return defaultDatum.GetValue()
}

// DecodeTableID decodes the raw key to a table ID
func DecodeTableID(key []byte) (int64, error) {
	_, physicalTableID, err := decodeTableID(key)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return physicalTableID, nil
}
