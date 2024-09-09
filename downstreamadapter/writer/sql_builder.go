package writer

import (
	"strings"

	"github.com/flowbehappy/tigate/pkg/common"
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
	row common.RowDelta,
	appendPlaceHolder bool,
	safeMode bool,
) (string, []interface{}) {
	var builder strings.Builder
	args, err := getArgs(&row.Row, tableInfo)
	if err != nil {
		// FIXME: handle error
		log.Panic("getArgs failed", zap.Error(err))
		return "", nil
	}
	if len(args) == 0 {
		return "", nil
	}

	colList := "(" + getColumnList(tableInfo) + ")"
	quoteTable := tableInfo.TableName.QuoteString()

	if safeMode {
		builder.WriteString("REPLACE INTO " + quoteTable + " " + colList + " VALUES ")
	} else {
		builder.WriteString("INSERT INTO " + quoteTable + " " + colList + " VALUES ")
	}
	if appendPlaceHolder {
		builder.WriteString("(" + placeHolder(len(args)) + ")")
	}

	return builder.String(), args
}

// prepareDelete builds a parametric DELETE statement as following
// sql: `DELETE FROM `test`.`t` WHERE x = ? AND y >= ? LIMIT 1`
func buildDelete(tableInfo *common.TableInfo, row common.RowDelta) (string, []interface{}) {
	var builder strings.Builder
	quoteTable := tableInfo.TableName.QuoteString()
	builder.WriteString("DELETE FROM " + quoteTable + " WHERE ")

	colNames, wargs := whereSlice(&row.PreRow, tableInfo)
	if len(wargs) == 0 {
		return "", nil
	}
	args := make([]interface{}, 0, len(wargs))
	for i := 0; i < len(colNames); i++ {
		if i > 0 {
			builder.WriteString(" AND ")
		}
		if wargs[i] == nil {
			builder.WriteString(quotes.QuoteName(colNames[i]) + " IS NULL")
		} else {
			builder.WriteString(quotes.QuoteName(colNames[i]) + " = ?")
			args = append(args, wargs[i])
		}
	}
	builder.WriteString(" LIMIT 1")
	sql := builder.String()
	return sql, args
}

func buildUpdate(tableInfo *common.TableInfo, row common.RowDelta) (string, []interface{}) {
	var builder strings.Builder
	builder.WriteString("UPDATE " + tableInfo.TableName.QuoteString() + " SET ")

	columnNames := make([]string, 0, len(tableInfo.Columns))
	allArgs := make([]interface{}, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		if col == nil || tableInfo.ColumnsFlag[col.ID].IsGeneratedColumn() {
			continue
		}
		columnNames = append(columnNames, col.Name.O)
	}

	args, err := getArgs(&row.Row, tableInfo)
	if err != nil {
		// FIXME: handle error
		log.Panic("getArgs failed", zap.Error(err))
		return "", nil
	}
	if len(args) == 0 {
		return "", nil
	}
	allArgs = append(allArgs, args...)

	for i, column := range columnNames {
		if i == len(columnNames)-1 {
			builder.WriteString("`" + quotes.EscapeName(column) + "` = ?")
		} else {
			builder.WriteString("`" + quotes.EscapeName(column) + "` = ?, ")
		}
	}

	builder.WriteString(" WHERE ")
	colNames, wargs := whereSlice(&row.PreRow, tableInfo)
	if len(wargs) == 0 {
		return "", nil
	}
	for i := 0; i < len(colNames); i++ {
		if i > 0 {
			builder.WriteString(" AND ")
		}
		if wargs[i] == nil {
			builder.WriteString(quotes.QuoteName(colNames[i]) + " IS NULL")
		} else {
			builder.WriteString(quotes.QuoteName(colNames[i]) + " = ?")
			allArgs = append(allArgs, wargs[i])
		}
	}
	builder.WriteString(" LIMIT 1")
	sql := builder.String()
	return sql, allArgs
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

func getColumnList(tableInfo *common.TableInfo) string {
	var b strings.Builder
	for i, col := range tableInfo.Columns {
		if col == nil || tableInfo.ColumnsFlag[col.ID].IsGeneratedColumn() {
			continue
		}
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(quotes.QuoteName(col.Name.O))
	}
	return b.String()
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

// placeHolder returns a string with n placeholders separated by commas
// n must be greater or equal than 1, or the function will panic
func placeHolder(n int) string {
	var builder strings.Builder
	builder.Grow((n-1)*2 + 1)
	for i := 0; i < n; i++ {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("?")
	}
	return builder.String()
}
