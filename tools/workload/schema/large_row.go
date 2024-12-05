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

package schema

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const varcharColumnMaxLen = 16383

func newColumnValues(r *rand.Rand, size, count int) [][]byte {
	result := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		buf := make([]byte, size)
		randomBytes(r, buf)
		result = append(result, buf)
	}
	return result
}

func newRowValues(r *rand.Rand, columnSize int, columnCount int, rowCount int) []string {
	const numColumnValues = 512
	columns := newColumnValues(r, columnSize, numColumnValues)

	result := make([]string, 0, rowCount)

	var sb strings.Builder
	for i := 0; i < rowCount; i++ {
		sb.Reset()

		for j := 0; j < columnCount; j++ {
			if sb.Len() != 0 {
				sb.Write([]byte(","))
			}
			index := rand.Int() % numColumnValues
			columnValue := columns[index]

			sb.WriteByte('\'')
			sb.Write(columnValue)
			sb.WriteByte('\'')
		}
		result = append(result, sb.String())
	}
	return result
}

func (l *LargeRowWorkload) getSmallRow() string {
	index := l.r.Int() % len(l.smallRows)
	return l.smallRows[index]
}

func (l *LargeRowWorkload) getLargeRow() string {
	index := l.r.Int() % len(l.largeRows)
	return l.largeRows[index]
}

type LargeRowWorkload struct {
	smallRows []string
	largeRows []string

	largeRatio float64

	columnCount int

	r *rand.Rand
}

func NewLargeRowWorkload(
	normalRowSize, largeRowSize int, largeRatio float64,
) Workload {
	columnCount := int(float64(largeRowSize) / varcharColumnMaxLen)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	smallColumnSize := int(float64(normalRowSize) / float64(columnCount))

	return &LargeRowWorkload{
		r:          r,
		largeRatio: largeRatio,

		columnCount: columnCount,

		smallRows: newRowValues(r, smallColumnSize, columnCount, 512),
		largeRows: newRowValues(r, varcharColumnMaxLen, columnCount, 128),
	}
}

func getTableName(n int) string {
	return fmt.Sprintf("large_row_%d", n)
}

func (l *LargeRowWorkload) BuildCreateTableStatement(n int) string {
	var cols string
	for i := 0; i < l.columnCount; i++ {
		cols = fmt.Sprintf("%s, col_%d VARCHAR(%d)", cols, i, varcharColumnMaxLen)
	}
	tableName := getTableName(n)
	query := fmt.Sprintf("CREATE TABLE %s(id bigint primary key %s);", tableName, cols)

	log.Info("large row workload, create the table", zap.Int("table", n), zap.Int("length", len(query)))

	return query
}

func (l *LargeRowWorkload) BuildInsertSql(tableN int, rowCount int) string {
	tableName := getTableName(tableN)
	insertSQL := fmt.Sprintf("INSERT INTO %s VALUES (%d,%s)", tableName, rand.Int63(), l.getSmallRow())

	var largeRowCount int
	for i := 1; i < rowCount; i++ {
		if l.r.Float64() < l.largeRatio {
			insertSQL = fmt.Sprintf("%s,(%d,%s)", insertSQL, rand.Int63(), l.getLargeRow())
			largeRowCount++
		} else {
			insertSQL = fmt.Sprintf("%s,(%d,%s)", insertSQL, rand.Int63(), l.getSmallRow())
		}
	}

	log.Info("large row workload, insert the table",
		zap.Int("table", tableN), zap.Int("rowCount", rowCount),
		zap.Int("largeRowCount", largeRowCount), zap.Int("length", len(insertSQL)))

	return insertSQL
}

func (l *LargeRowWorkload) BuildUpdateSql(opts UpdateOption) string {
	tableName := getTableName(opts.Table)
	upsertSQL := strings.Builder{}
	upsertSQL.WriteString(fmt.Sprintf("INSERT INTO %s VALUES (%d,%s)", tableName, rand.Int63(), l.getSmallRow()))

	var largeRowCount int
	for i := 1; i < opts.RowCount; i++ {
		if l.r.Float64() < l.largeRatio {
			upsertSQL.WriteString(fmt.Sprintf(",(%d,%s)", rand.Int63(), l.getLargeRow()))
			largeRowCount++
		} else {
			upsertSQL.WriteString(fmt.Sprintf(",(%d,%s)", rand.Int63(), l.getSmallRow()))
		}
	}
	upsertSQL.WriteString(" ON DUPLICATE KEY UPDATE col_0=VALUES(col_0)")

	log.Info("large row workload, upsert the table",
		zap.Int("table", opts.Table), zap.Int("rowCount", opts.RowCount),
		zap.Int("largeRowCount", largeRowCount))
	return upsertSQL.String()
}

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randomBytes(r *rand.Rand, buffer []byte) {
	for i := range buffer {
		buffer[i] = letters[r.Intn(len(letters))]
	}
}
