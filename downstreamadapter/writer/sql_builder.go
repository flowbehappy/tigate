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

package writer

import (
	"strings"

	"github.com/flowbehappy/tigate/pkg/common"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tiflow/pkg/quotes"
	"go.uber.org/zap"
)

type preparedDMLs struct {
	sqls            []string
	values          [][]interface{}
	rowCount        int
	approximateSize int64
	startTs         []uint64
}

// prepareReplace builds a parametrics REPLACE statement as following
// sql: `REPLACE INTO `test`.`t` VALUES (?,?,?)`
func buildInsert(
	tableInfo *common.TableInfo,
	row commonEvent.RowChange,
	safeMode bool,
) (string, []interface{}) {
	args, err := getArgs(&row.Row, tableInfo)
	if err != nil {
		// FIXME: handle error
		log.Panic("getArgs failed", zap.Error(err))
		return "", nil
	}
	if len(args) == 0 {
		return "", nil
	}

	var sql string
	if safeMode {
		sql = tableInfo.GetPreReplaceSQL()
	} else {
		sql = tableInfo.GetPreInsertSQL()
	}

	if sql == "" {
		log.Panic("PreInsertSQL should not be empty")
	}

	return sql, args
}

// prepareDelete builds a parametric DELETE statement as following
// sql: `DELETE FROM `test`.`t` WHERE x = ? AND y >= ? LIMIT 1`
func buildDelete(tableInfo *common.TableInfo, row commonEvent.RowChange) (string, []interface{}) {
	var builder strings.Builder
	quoteTable := tableInfo.TableName.QuoteString()
	builder.WriteString("DELETE FROM ")
	builder.WriteString(quoteTable)
	builder.WriteString(" WHERE ")

	colNames, whereArgs := whereSlice(&row.PreRow, tableInfo)
	if len(whereArgs) == 0 {
		return "", nil
	}
	args := make([]interface{}, 0, len(whereArgs))
	for i := 0; i < len(colNames); i++ {
		if i > 0 {
			builder.WriteString(" AND ")
		}
		if whereArgs[i] == nil {
			builder.WriteString(quotes.QuoteName(colNames[i]))
			builder.WriteString(" IS NULL")
		} else {
			builder.WriteString(quotes.QuoteName(colNames[i]))
			builder.WriteString(" = ?")
			args = append(args, whereArgs[i])
		}
	}
	builder.WriteString(" LIMIT 1")
	sql := builder.String()
	return sql, args
}

func buildUpdate(tableInfo *common.TableInfo, row commonEvent.RowChange) (string, []interface{}) {
	var builder strings.Builder
	if tableInfo.GetPreUpdateSQL() == "" {
		log.Panic("PreUpdateSQL should not be empty")
	}
	builder.WriteString(tableInfo.GetPreUpdateSQL())

	args, err := getArgs(&row.Row, tableInfo)
	if err != nil {
		// FIXME: handle error
		log.Panic("getArgs failed", zap.Error(err))
		return "", nil
	}
	if len(args) == 0 {
		return "", nil
	}

	whereColNames, whereArgs := whereSlice(&row.PreRow, tableInfo)
	if len(whereArgs) == 0 {
		return "", nil
	}

	builder.WriteString(" WHERE ")
	for i := 0; i < len(whereColNames); i++ {
		if i > 0 {
			builder.WriteString(" AND ")
		}
		if whereArgs[i] == nil {
			builder.WriteString(quotes.QuoteName(whereColNames[i]))
			builder.WriteString(" IS NULL")
		} else {
			builder.WriteString(quotes.QuoteName(whereColNames[i]))
			builder.WriteString(" = ?")
			args = append(args, whereArgs[i])
		}
	}

	builder.WriteString(" LIMIT 1")
	sql := builder.String()
	return sql, args
}

func getArgs(row *chunk.Row, tableInfo *common.TableInfo) ([]interface{}, error) {
	args := make([]interface{}, 0, len(tableInfo.Columns))
	for i, col := range tableInfo.Columns {
		if col == nil || tableInfo.ColumnsFlag[col.ID].IsGeneratedColumn() {
			continue
		}
		v, err := common.FormatColVal(row, col, i)
		if err != nil {
			return nil, err
		}
		args = append(args, v)
	}
	return args, nil
}

// whereSlice returns the column names and values for the WHERE clause
func whereSlice(row *chunk.Row, tableInfo *common.TableInfo) ([]string, []interface{}) {
	args := make([]interface{}, 0, len(tableInfo.Columns))
	colNames := make([]string, 0, len(tableInfo.Columns))
	// Try to use unique key values when available
	for i, col := range tableInfo.Columns {
		if col == nil || !tableInfo.ColumnsFlag[col.ID].IsHandleKey() {
			continue
		}
		colNames = append(colNames, col.Name.O)
		v, err := common.FormatColVal(row, col, i)
		if err != nil {
			// FIXME: handle error
			log.Panic("formatColVal failed", zap.Error(err))
		}
		args = append(args, v)
	}

	// if no explicit row id but force replicate, use all key-values in where condition
	if len(colNames) == 0 {
		for i, col := range tableInfo.Columns {
			colNames = append(colNames, col.Name.O)
			v, err := common.FormatColVal(row, col, i)
			if err != nil {
				// FIXME: handle error
				log.Panic("formatColVal failed", zap.Error(err))
			}
			args = append(args, v)
		}
	}
	return colNames, args
}
