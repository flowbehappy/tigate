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

package filter

import (
	"strings"

	"github.com/flowbehappy/tigate/pkg/apperror"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	tfilter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
)

const (
	// SyncPointTable is the tale name use to write ts-map when sync-point is enable.
	SyncPointTable = "syncpoint_v1"

	// TiCDCSystemSchema is the schema only use by TiCDC.
	TiCDCSystemSchema = "tidb_cdc"
	// LightningTaskInfoSchema is the schema only generated by Lightning
	LightningTaskInfoSchema = "lightning_task_info"
)

// Filter are safe for concurrent use.
// TODO: find a better way to abstract this interface.
type Filter interface {
	// ShouldIgnoreDMLEvent returns true if the DML event should not be sent to downstream.
	ShouldIgnoreDMLEvent(dml *model.RowChangedEvent, rawRow model.RowChangedDatums, tableInfo *model.TableInfo) (bool, error)
	// ShouldIgnoreDDLEvent returns true if the DDL event should not be sent to downstream.
	ShouldIgnoreDDLEvent(ddl *model.DDLEvent) (bool, error)
	// ShouldDiscardDDL returns true if this DDL should be discarded.
	// If a ddl is discarded, it will neither be applied to cdc's schema storage
	// nor sent to downstream.
	ShouldDiscardDDL(ddlType timodel.ActionType, schema, table string) bool
	// ShouldIgnoreTable returns true if the table should be ignored.
	ShouldIgnoreTable(schema, table string) bool
	// ShouldIgnoreSchema returns true if the schema should be ignored.
	ShouldIgnoreSchema(schema string) bool
	// Verify should only be called by create changefeed OpenAPI.
	// Its purpose is to verify the expression filter config.
	Verify(tableInfos []*model.TableInfo) error

	// filter ddl event to update query and influenced table spans
	FilterDDLEvent(ddl *commonEvent.DDLEvent) error
}

// filter implements Filter.
type filter struct {
	// tableFilter is used to filter in dml/ddl event by table name.
	tableFilter tfilter.Filter
	// dmlExprFilter is used to filter out dml event by its columns value.
	dmlExprFilter *dmlExprFilter
	// sqlEventFilter is used to filter out dml/ddl event by its type or query.
	sqlEventFilter *sqlEventFilter
	// ignoreTxnStartTs is used to filter out dml/ddl event by its starsTs.
	ignoreTxnStartTs []uint64
}

// NewFilter creates a filter.
func NewFilter(cfg *config.FilterConfig, tz string, caseSensitive bool) (Filter, error) {
	f, err := VerifyTableRules(cfg)
	if err != nil {
		return nil, err
	}

	if !caseSensitive {
		f = tfilter.CaseInsensitive(f)
	}

	dmlExprFilter, err := newExprFilter(tz, cfg)
	if err != nil {
		return nil, err
	}
	sqlEventFilter, err := newSQLEventFilter(cfg)
	if err != nil {
		return nil, err
	}
	return &filter{
		tableFilter:      f,
		dmlExprFilter:    dmlExprFilter,
		sqlEventFilter:   sqlEventFilter,
		ignoreTxnStartTs: cfg.IgnoreTxnStartTs,
	}, nil
}

func (f *filter) FilterDDLEvent(ddl *commonEvent.DDLEvent) error {
	query := ddl.Query
	queryList := strings.Split(query, ";")
	if len(queryList) == 1 {
		return nil
	}
	multiTableInfos := ddl.MultipleTableInfos
	schemaName := ddl.SchemaName
	if len(multiTableInfos) != len(queryList) {
		log.Error("DDL Event is not valid, query count not equal to table count", zap.Any("ddl", ddl))
		return apperror.NewAppError(apperror.ErrorInvalidDDLEvent, "DDL Event is not valid, query count not equal to table count")
	}
	finalQuery := make([]string, 0, len(queryList))
	for i, query := range queryList {
		tableInfo := multiTableInfos[i]
		// 只需要 判断 table name 需不需要过滤就行，如果 schema name 要过滤的话，整个 query 就不会给 dispatcher 了
		tableName := tableInfo.Name.O
		if !f.ShouldIgnoreTable(schemaName, tableName) {
			finalQuery = append(finalQuery, query)
		}
	}
	if len(finalQuery) != len(queryList) {
		ddl.Query = strings.Join(finalQuery, ";")
	}
	// TODO: 应该同时要更新一下 ddl 依赖的 table 信息
	return nil
}

// ShouldIgnoreDMLEvent checks if a DML event should be ignore by conditions below:
// 0. By startTs.
// 1. By table name.
// 2. By type.
// 3. By columns value.
func (f *filter) ShouldIgnoreDMLEvent(
	dml *model.RowChangedEvent,
	rawRow model.RowChangedDatums,
	ti *model.TableInfo,
) (bool, error) {
	if f.shouldIgnoreStartTs(dml.StartTs) {
		return true, nil
	}

	if f.ShouldIgnoreTable(dml.TableInfo.GetSchemaName(), dml.TableInfo.GetTableName()) {
		return true, nil
	}

	ignoreByEventType, err := f.sqlEventFilter.shouldSkipDML(dml)
	if err != nil {
		return false, err
	}
	if ignoreByEventType {
		return true, nil
	}
	return f.dmlExprFilter.shouldSkipDML(dml, rawRow, ti)
}

// ShouldDiscardDDL checks if a DDL should be discarded by conditions below:
// 0. By allow list.
// 1. By schema name.
// 2. By table name.
func (f *filter) ShouldDiscardDDL(ddlType timodel.ActionType, schema, table string) bool {
	if !isAllowedDDL(ddlType) {
		return true
	}

	if IsSchemaDDL(ddlType) {
		return f.ShouldIgnoreSchema(schema)
	}
	return f.ShouldIgnoreTable(schema, table)
}

// ShouldIgnoreDDLEvent checks if a DDL event should be ignore by conditions below:
// 1. By ddl type.
// 2. By ddl query.
//
// If a ddl is ignored, it will be applied to cdc's schema storage,
// but will not be sent to downstream.
// Note that a ignored ddl is different from a discarded ddl. For example, suppose
// we have a changefeed-test with the following config:
//   - table filter: rules = ['test.*']
//   - event-filters: matcher = ["test.worker"] ignore-event = ["create table"]
//
// Then, for the following DDLs:
//  1. `CREATE TABLE test.worker` will be ignored, but the table will be replicated by changefeed-test.
//  2. `CREATE TABLE other.worker` will be discarded, and the table will not be replicated by changefeed-test.
func (f *filter) ShouldIgnoreDDLEvent(ddl *model.DDLEvent) (bool, error) {
	if f.shouldIgnoreStartTs(ddl.StartTs) {
		return true, nil
	}
	return f.sqlEventFilter.shouldSkipDDL(ddl)
}

// ShouldIgnoreTable returns true if the specified table should be ignored by this changefeed.
// NOTICE: Set `tbl` to an empty string to test against the whole database.
func (f *filter) ShouldIgnoreTable(db, tbl string) bool {
	if IsSysSchema(db) {
		return true
	}
	return !f.tableFilter.MatchTable(db, tbl)
}

// ShouldIgnoreSchema returns true if the specified schema should be ignored by this changefeed.
func (f *filter) ShouldIgnoreSchema(schema string) bool {
	return IsSysSchema(schema) || !f.tableFilter.MatchSchema(schema)
}

func (f *filter) Verify(tableInfos []*model.TableInfo) error {
	return f.dmlExprFilter.verify(tableInfos)
}

func (f *filter) shouldIgnoreStartTs(ts uint64) bool {
	for _, ignoreTs := range f.ignoreTxnStartTs {
		if ignoreTs == ts {
			return true
		}
	}
	return false
}

func isAllowedDDL(actionType timodel.ActionType) bool {
	_, ok := ddlWhiteListMap[actionType]
	return ok
}

// IsSchemaDDL returns true if the action type is a schema DDL.
func IsSchemaDDL(actionType timodel.ActionType) bool {
	switch actionType {
	case timodel.ActionCreateSchema, timodel.ActionDropSchema,
		timodel.ActionModifySchemaCharsetAndCollate:
		return true
	default:
		return false
	}
}
