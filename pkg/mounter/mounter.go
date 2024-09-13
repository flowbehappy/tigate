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
	"time"
	"unsafe"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tiflow/pkg/spanz"

	"go.uber.org/zap"
)

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
	DecodeToChunk(rawKV *common.RawKVEntry, tableInfo *common.TableInfo, chk *chunk.Chunk) (int, error)
}

type mounter struct {
	tz *time.Location
}

// NewMounter creates a mounter
func NewMounter(tz *time.Location) Mounter {
	return &mounter{
		tz: tz,
	}
}

// DecodeToChunk decodes the raw KV entry to a chunk, it returns the number of rows decoded.
func (m *mounter) DecodeToChunk(raw *common.RawKVEntry, tableInfo *common.TableInfo, chk *chunk.Chunk) (int, error) {
	recordID, err := tablecodec.DecodeRowKey(raw.Key)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if !bytes.HasPrefix(raw.Key, tablePrefix) {
		return 0, nil
	}

	// key, physicalTableID, err := decodeTableID(raw.Key)
	// if err != nil {
	// 	return nil
	// }
	count := 0
	if len(raw.OldValue) != 0 {
		if !rowcodec.IsNewFormat(raw.OldValue) {
			err := m.rawKVToChunkV1(raw.OldValue, tableInfo, chk, recordID)
			if err != nil {
				return 0, errors.Trace(err)
			}
		} else {
			err := m.rawKVToChunkV2(raw.OldValue, tableInfo, chk, recordID)
			if err != nil {
				return 0, errors.Trace(err)
			}
		}
		count++
	}
	if len(raw.Value) != 0 {
		if !rowcodec.IsNewFormat(raw.Value) {
			err := m.rawKVToChunkV1(raw.Value, tableInfo, chk, recordID)
			if err != nil {
				return 0, errors.Trace(err)
			}
		} else {
			err := m.rawKVToChunkV2(raw.Value, tableInfo, chk, recordID)
			if err != nil {
				return 0, errors.Trace(err)
			}
		}
		count++
	}
	return count, nil
}

// IsLegacyFormatJob returns true if the job is from the legacy DDL list key.
func IsLegacyFormatJob(rawKV *common.RawKVEntry) bool {
	return bytes.HasPrefix(rawKV.Key, metaPrefix)
}

// ParseDDLJob parses the job from the raw KV entry.
func ParseDDLJob(rawKV *common.RawKVEntry, ddlTableInfo *DDLTableInfo) (*model.Job, error) {
	var v []byte
	var datum types.Datum

	zstdDecoder, err := zstd.NewReader(nil)
	if err != nil {
		log.Panic("failed to create zstd decoder", zap.Error(err))
	}
	defer zstdDecoder.Close()
	if rawKV.CompressType == common.CompressTypeZstd {
		rawKV.Value, err = zstdDecoder.DecodeAll(rawKV.Value, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

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
