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
	"github.com/flowbehappy/tigate/downstreamadapter/sink/types"
	"github.com/flowbehappy/tigate/pkg/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tiflow/cdc/model"
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
	mock.ExpectExec("INSERT INTO `test_schema`.`test_table` (`id`,`name`) VALUES (?,?);UPDATE `test`.`users` SET `id` = ?, `name` = ? WHERE `id` = ? LIMIT 1").
		WithArgs(1, "Alice", 1, "Bob", 1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mysqlSink := NewMysqlSink(model.DefaultChangeFeedID("test1"), 8, writer.NewMysqlConfig(), db)
	assert.NotNil(t, mysqlSink)

	tableProgress := types.NewTableProgress()

	ts, isEmpty := tableProgress.GetCheckpointTs()
	require.NotEqual(t, ts, 0)
	require.Equal(t, isEmpty, true)

	mysqlSink.AddDDLAndSyncPointEvent(&common.TxnEvent{
		StartTs:  1,
		CommitTs: 1,
		DDLEvent: &common.DDLEvent{
			Job: &timodel.Job{
				Type:       timodel.ActionCreateTable,
				SchemaID:   10,
				SchemaName: "test",
				TableName:  "t",
				Query:      "CREATE TABLE `test`.`t` (`id` INT PRIMARY KEY, `name` VARCHAR(255))",
			},
			CommitTS: 1,
		},
	}, tableProgress)

	mysqlSink.AddDMLEvent(&common.TxnEvent{
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
				PhysicalTableID: 1,
			},
		},
	}, tableProgress)

	mysqlSink.AddDMLEvent(&common.TxnEvent{
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
				PhysicalTableID: 1,
			},
		},
	}, tableProgress)

	time.Sleep(1 * time.Second)

	mysqlSink.PassDDLAndSyncPointEvent(&common.TxnEvent{
		StartTs:  3,
		CommitTs: 4,
		DDLEvent: &common.DDLEvent{
			Job: &timodel.Job{
				Type:       timodel.ActionCreateTable,
				SchemaID:   10,
				SchemaName: "test",
				TableName:  "t2",
				Query:      "CREATE TABLE `test`.`t2` (`id` INT PRIMARY KEY, `name` VARCHAR(255))",
			},
			CommitTS: 4,
		},
	}, tableProgress)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)

	ts, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, ts, uint64(3))
	require.Equal(t, isEmpty, true)

}
