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

package mysql

import (
	"strings"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/quotes"
)

type preparedDMLs struct {
	sqls            []string
	values          []*argsSlice
	rowCount        int
	approximateSize int64
	startTs         []uint64
}

var dmlsPool = sync.Pool{
	New: func() interface{} {
		return &preparedDMLs{
			sqls:    make([]string, 0, 128),
			values:  make([]*argsSlice, 0, 128),
			startTs: make([]uint64, 0, 128),
		}
	},
}

func putDMLs(dmls *preparedDMLs) {
	dmls.reset()
	dmlsPool.Put(dmls)
}

type argsSlice []interface{}

var argsPool = sync.Pool{
	New: func() interface{} {
		args := make([]interface{}, 0, 64)
		return &argsSlice{args}
	},
}

func (d *preparedDMLs) reset() {
	d.sqls = d.sqls[:0]

	for _, v := range d.values {
		putArgs(v)
	}
	d.values = d.values[:0]

	d.startTs = d.startTs[:0]
	d.rowCount = 0
	d.approximateSize = 0
}

// prepareReplace builds a parametrics REPLACE statement as following
// sql: `REPLACE INTO `test`.`t` VALUES (?,?,?)`
func buildInsert(
	tableInfo *common.TableInfo,
	row commonEvent.RowChange,
	translateToInsert bool,
) (string, *argsSlice, error) {
	args, err := getArgs(&row.Row, tableInfo)

	// Fizz delete this after testing
	defer putArgs(args)

	if err != nil {
		return "", nil, errors.Trace(err)
	}
	if len(*args) == 0 {
		return "", nil, nil
	}
	var sql string
	if translateToInsert {
		sql = tableInfo.GetPreInsertSQL()
	} else {
		sql = tableInfo.GetPreReplaceSQL()
	}

	if sql == "" {
		log.Panic("PreInsertSQL should not be empty")
	}

	return sql, args, nil
}

// prepareDelete builds a parametric DELETE statement as following
// sql: `DELETE FROM `test`.`t` WHERE x = ? AND y >= ? LIMIT 1`
func buildDelete(tableInfo *common.TableInfo, row commonEvent.RowChange) (string, *argsSlice, error) {
	var builder strings.Builder
	quoteTable := tableInfo.TableName.QuoteString()
	builder.WriteString("DELETE FROM ")
	builder.WriteString(quoteTable)
	builder.WriteString(" WHERE ")

	colNames, whereArgs, err := whereSlice(&row.PreRow, tableInfo)
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	if len(whereArgs) == 0 {
		return "", nil, nil
	}
	args := argsPool.Get().(*argsSlice)
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
			*args = append(*args, whereArgs[i])
		}
	}
	builder.WriteString(" LIMIT 1")
	sql := builder.String()
	return sql, args, nil
}

func buildUpdate(tableInfo *common.TableInfo, row commonEvent.RowChange) (string, *argsSlice, error) {
	var builder strings.Builder
	if tableInfo.GetPreUpdateSQL() == "" {
		log.Panic("PreUpdateSQL should not be empty")
	}
	builder.WriteString(tableInfo.GetPreUpdateSQL())

	args, err := getArgs(&row.Row, tableInfo)
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	if len(*args) == 0 {
		return "", nil, nil
	}

	whereColNames, whereArgs, err := whereSlice(&row.PreRow, tableInfo)
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	if len(whereArgs) == 0 {
		return "", nil, nil
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
			*args = append(*args, whereArgs[i])
		}
	}

	builder.WriteString(" LIMIT 1")
	sql := builder.String()
	return sql, args, nil
}

func getArgs(row *chunk.Row, tableInfo *common.TableInfo) (*argsSlice, error) {
	args := argsPool.Get().(*argsSlice)

	for i, col := range tableInfo.Columns {
		if col == nil || tableInfo.ColumnsFlag[col.ID].IsGeneratedColumn() {
			continue
		}
		v, err := common.FormatColVal(row, col, i)
		if err != nil {
			argsPool.Put(args)
			return nil, err
		}
		*args = append(*args, v)
	}
	return args, nil
}

// whereSlice returns the column names and values for the WHERE clause
func whereSlice(row *chunk.Row, tableInfo *common.TableInfo) ([]string, []interface{}, error) {
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
			return nil, nil, errors.Trace(err)
		}
		args = append(args, v)
	}

	// if no explicit row id but force replicate, use all key-values in where condition
	if len(colNames) == 0 {
		for i, col := range tableInfo.Columns {
			colNames = append(colNames, col.Name.O)
			v, err := common.FormatColVal(row, col, i)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			args = append(args, v)
		}
	}
	return colNames, args, nil
}

func putArgs(args *argsSlice) {
	if args != nil {
		*args = (*args)[:0]
		argsPool.Put(args)
	}
}
