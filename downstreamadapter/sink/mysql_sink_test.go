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

package sink

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/flowbehappy/tigate/common"
	"github.com/flowbehappy/tigate/downstreamadapter/writer"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/assert"
)

// 测试 mysql sink 的功能，输入一系列的 排序的 event（中间可能有 conflict ），检查是否按预期写入 mysql，并且检查 tableProgress 状态
func TestMysqlSinkBasicFunctionality(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.Nil(t, err)

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("CREATE TABLE `test`.`t` (`id` INT PRIMARY KEY, `name` VARCHAR(255))").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `test_schema`.`test_table` (`id`,`name`) VALUES (?,?)").
		WithArgs(1, "Alice").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("UPDATE `test`.`users` SET `id` = ?, `name` = ? WHERE `id` = ? LIMIT 1").
		WithArgs(1, "Bob", 1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mysqlSink := NewMysqlSink(8, writer.NewMysqlConfig(), db)
	assert.NotNil(t, mysqlSink)

	tableSpan := common.TableSpan{TableID: 1}
	mysqlSink.AddTableSpan(&tableSpan)

	mysqlSink.AddDDLAndSyncPointEvent(&tableSpan, &common.TxnEvent{
		StartTs:  3,
		CommitTs: 4,
		DDLEvent: &common.DDLEvent{
			Job: &model.Job{
				Type:       model.ActionCreateTable,
				SchemaID:   10,
				SchemaName: "test",
				TableName:  "t",
				Query:      "CREATE TABLE `test`.`t` (`id` INT PRIMARY KEY, `name` VARCHAR(255))",
			},
			CommitTS: 4,
		},
	})

	mysqlSink.AddDMLEvent(&tableSpan, &common.TxnEvent{
		StartTs:  1,
		CommitTs: 2,
		Rows: []*common.RowChangedEvent{
			{
				TableInfo: &common.TableInfo{
					TableName: common.TableName{
						Schema: "test_schema",
						Table:  "test_table",
					},
				},
				Columns: []*common.Column{
					{Name: "id", Value: 1, Flag: common.HandleKeyFlag | common.PrimaryKeyFlag},
					{Name: "name", Value: "Alice"},
				},
			},
		},
	})

	mysqlSink.AddDMLEvent(&tableSpan, &common.TxnEvent{
		StartTs:  2,
		CommitTs: 3,
		Rows: []*common.RowChangedEvent{
			{
				TableInfo: &common.TableInfo{
					TableName: common.TableName{
						Schema: "test",
						Table:  "users",
					},
				},
				PreColumns: []*common.Column{
					{Name: "id", Value: 1, Flag: common.HandleKeyFlag | common.PrimaryKeyFlag},
					{Name: "name", Value: "Alice"},
				},
				Columns: []*common.Column{
					{Name: "id", Value: 1, Flag: common.HandleKeyFlag | common.PrimaryKeyFlag},
					{Name: "name", Value: "Bob"},
				},
			},
		},
	})

	require.Equal(t, mysqlSink.IsEmpty(&tableSpan), false)
	require.NotEqual(t, mysqlSink.GetSmallestCommitTs(&tableSpan), 0)

	time.Sleep(1 * time.Second)
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)

	require.Equal(t, mysqlSink.IsEmpty(&tableSpan), true)
	require.Equal(t, mysqlSink.GetSmallestCommitTs(&tableSpan), uint64(0))

}
