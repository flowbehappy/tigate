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

package dispatcher

import (
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/downstreamadapter/writer"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/stretchr/testify/require"
)

func newTestMockDB(t *testing.T) (db *sql.DB, mock sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.Nil(t, err)
	return
}

// 测一下我直接给dispatcher 数据塞 chan 里，检查 db 是否正确收到了，以及 消费前后的心跳和memory usage 是否正常

func TestTableEventDispatcher(t *testing.T) {
	db, mock := newTestMockDB(t)
	defer db.Close()

	mysqlSink := sink.NewMysqlSink(8, writer.NewMysqlConfig(), db)
	tableSpan := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{TableID: 1}}
	startTs := uint64(100)

	tableEventDispatcher := NewTableEventDispatcher(tableSpan, mysqlSink, startTs, nil)

	tableEventDispatcher.PushTxnEvent(&common.TxnEvent{
		StartTs:  100,
		CommitTs: 101,
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

	tableEventDispatcher.PushTxnEvent(&common.TxnEvent{
		StartTs:  102,
		CommitTs: 105,
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
	tableEventDispatcher.UpdateResolvedTs(110)

	heartBeatInfo := CollectDispatcherHeartBeatInfo(tableEventDispatcher)
	require.Equal(t, uint64(100), heartBeatInfo.CheckpointTs)
	//require.NotEqual(t, 0, tableEventDispatcher.GetMemoryUsage().GetUsedBytes())

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `test_schema`.`test_table` (`id`,`name`) VALUES (?,?)").
		WithArgs(1, "Alice").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("UPDATE `test`.`users` SET `id` = ?, `name` = ? WHERE `id` = ? LIMIT 1").
		WithArgs(1, "Bob", 1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	time.Sleep(1 * time.Second)

	err := mock.ExpectationsWereMet()
	require.NoError(t, err)

	heartBeatInfo = CollectDispatcherHeartBeatInfo(tableEventDispatcher)
	require.Equal(t, uint64(110), heartBeatInfo.CheckpointTs)
	//require.Equal(t, 0, tableEventDispatcher.GetMemoryUsage().GetUsedBytes())
}
