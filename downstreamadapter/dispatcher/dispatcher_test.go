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
	"github.com/flowbehappy/tigate/pkg/filter"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func newTestMockDB(t *testing.T) (db *sql.DB, mock sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.Nil(t, err)
	return
}

func teardown() {
	GetDispatcherTaskScheduler().Stop()
}

// BasicDispatcher with normal dml cases
func TestBasicDispatcher(t *testing.T) {
	db, mock := newTestMockDB(t)
	defer db.Close()

	mysqlSink := sink.NewMysqlSink(model.DefaultChangeFeedID("test1"), 8, writer.NewMysqlConfig(), db)
	tableSpan := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{TableID: 1}}
	startTs := uint64(100)

	tableSpanStatusChan := make(chan *heartbeatpb.TableSpanStatus, 10)
	filter, _ := filter.NewFilter(&config.ReplicaConfig{Filter: &config.FilterConfig{}}, "")

	dispatcher := NewDispatcher(tableSpan, mysqlSink, startTs, tableSpanStatusChan, filter)

	dispatcherEventsDynamicStream := GetDispatcherEventsDynamicStream()

	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
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
		DispatcherID: dispatcher.id,
	}

	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
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
		DispatcherID: dispatcher.id,
	}
	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{ResolvedTs: 110, DispatcherID: dispatcher.id}

	heartBeatInfo := &HeartBeatInfo{}
	dispatcher.CollectDispatcherHeartBeatInfo(heartBeatInfo)
	require.Equal(t, uint64(100), heartBeatInfo.CheckpointTs)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `test_schema`.`test_table` (`id`,`name`) VALUES (?,?);UPDATE `test`.`users` SET `id` = ?, `name` = ? WHERE `id` = ? LIMIT 1").
		WithArgs(1, "Alice", 1, "Bob", 1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	time.Sleep(1 * time.Second)

	err := mock.ExpectationsWereMet()
	require.NoError(t, err)

	dispatcher.CollectDispatcherHeartBeatInfo(heartBeatInfo)
	require.Equal(t, uint64(110), heartBeatInfo.CheckpointTs)
}

func TestDispatcherWithSingleTableDDL(t *testing.T) {
	db, mock := newTestMockDB(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("USE `test_schema`;").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("ALTER TABLE `test_schema`.`test_table` ADD COLUMN `age` INT").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mysqlSink := sink.NewMysqlSink(model.DefaultChangeFeedID("test1"), 8, writer.NewMysqlConfig(), db)
	tableSpan := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{TableID: 1}}
	startTs := uint64(100)

	tableSpanStatusChan := make(chan *heartbeatpb.TableSpanStatus, 10)
	filter, _ := filter.NewFilter(&config.ReplicaConfig{Filter: &config.FilterConfig{}}, "")

	dispatcher := NewDispatcher(tableSpan, mysqlSink, startTs, tableSpanStatusChan, filter)

	dispatcherEventsDynamicStream := GetDispatcherEventsDynamicStream()
	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
		StartTs:  102,
		CommitTs: 102,
		DDLEvent: &common.DDLEvent{
			Job: &timodel.Job{
				Type:       timodel.ActionAddColumn,
				SchemaName: "test_schema",
				TableName:  "test_table",
				Query:      "ALTER TABLE `test_schema`.`test_table` ADD COLUMN `age` INT",
			},
			CommitTS: 102,
		},
		DispatcherID: dispatcher.id,
	}

	time.Sleep(10 * time.Millisecond)

	heartBeatInfo := &HeartBeatInfo{}
	dispatcher.CollectDispatcherHeartBeatInfo(heartBeatInfo)
	require.Equal(t, uint64(101), heartBeatInfo.CheckpointTs)

	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{ResolvedTs: 110, DispatcherID: dispatcher.id}

	time.Sleep(1 * time.Second)
	dispatcher.CollectDispatcherHeartBeatInfo(heartBeatInfo)
	require.Equal(t, uint64(110), heartBeatInfo.CheckpointTs)

	err := mock.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestDispatcherWithCrossTableDDL(t *testing.T) {
	defer teardown()
	db, mock := newTestMockDB(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("USE `test_schema`;").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("Create table `test_schema`.`test_table` (id int primary key, name varchar(255))").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mysqlSink := sink.NewMysqlSink(model.DefaultChangeFeedID("test1"), 8, writer.NewMysqlConfig(), db)
	tableSpan := &common.DDLSpan
	startTs := uint64(100)

	tableSpanStatusChan := make(chan *heartbeatpb.TableSpanStatus, 10)
	filter, _ := filter.NewFilter(&config.ReplicaConfig{Filter: &config.FilterConfig{}}, "")

	dispatcher := NewDispatcher(tableSpan, mysqlSink, startTs, tableSpanStatusChan, filter)

	dispatcherEventsDynamicStream := GetDispatcherEventsDynamicStream()
	dispatcherStatusDynamicStream := GetDispatcherStatusDynamicStream()

	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
		StartTs:  102,
		CommitTs: 102,
		DDLEvent: &common.DDLEvent{
			Job: &timodel.Job{
				Type:       timodel.ActionCreateTable,
				SchemaName: "test_schema",
				TableName:  "test_table",
				Query:      "Create table `test_schema`.`test_table` (id int primary key, name varchar(255))",
			},
			CommitTS: 102,
		},
		DispatcherID: dispatcher.id,
	}

	// 检查可以从 tableSpanStatusChan 中拿到消息
	<-dispatcher.GetStatusesChan()

	require.NotEqual(t, dispatcher.ddlPendingEvent, nil)

	dispatcherStatusDynamicStream.In() <- DispatcherStatusWithID{
		id: dispatcher.id,
		status: &heartbeatpb.DispatcherStatus{
			Ack:    &heartbeatpb.ACK{CommitTs: 102},
			Action: &heartbeatpb.DispatcherAction{Action: heartbeatpb.Action_Write, CommitTs: 102},
		},
	}

	time.Sleep(10 * time.Millisecond)

	err := mock.ExpectationsWereMet()
	require.NoError(t, err)

	heartBeatInfo := &HeartBeatInfo{}
	dispatcher.CollectDispatcherHeartBeatInfo(heartBeatInfo)
	require.Equal(t, uint64(101), heartBeatInfo.CheckpointTs)
}

func TestDispatcherWithCrossTableDDLAndDML(t *testing.T) {
	//defer teardown()
	db, mock := newTestMockDB(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("USE `test_schema`;").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("Create table `test_schema`.`test_table` (id int primary key, name varchar(255))").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `test`.`users` SET `id` = ?, `name` = ? WHERE `id` = ? LIMIT 1").
		WithArgs(1, "Bob", 1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mysqlSink := sink.NewMysqlSink(model.DefaultChangeFeedID("test1"), 8, writer.NewMysqlConfig(), db)
	tableSpan := &common.DDLSpan
	startTs := uint64(100)

	tableSpanStatusChan := make(chan *heartbeatpb.TableSpanStatus, 10)
	filter, _ := filter.NewFilter(&config.ReplicaConfig{Filter: &config.FilterConfig{}}, "")

	dispatcher := NewDispatcher(tableSpan, mysqlSink, startTs, tableSpanStatusChan, filter)

	dispatcherEventsDynamicStream := GetDispatcherEventsDynamicStream()
	dispatcherStatusDynamicStream := GetDispatcherStatusDynamicStream()

	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
		StartTs:  102,
		CommitTs: 102,
		DDLEvent: &common.DDLEvent{
			Job: &timodel.Job{
				Type:       timodel.ActionCreateTable,
				SchemaName: "test_schema",
				TableName:  "test_table",
				Query:      "Create table `test_schema`.`test_table` (id int primary key, name varchar(255))",
			},
			CommitTS: 102,
		},
		DispatcherID: dispatcher.id,
	}

	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
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
		DispatcherID: dispatcher.id,
	}

	// 检查可以从 tableSpanStatusChan 中拿到消息
	<-dispatcher.GetStatusesChan()
	require.NotEqual(t, dispatcher.ddlPendingEvent, nil)

	heartBeatInfo := &HeartBeatInfo{}
	dispatcher.CollectDispatcherHeartBeatInfo(heartBeatInfo)
	require.Equal(t, uint64(100), heartBeatInfo.CheckpointTs)

	dispatcherStatusDynamicStream.In() <- DispatcherStatusWithID{
		id: dispatcher.id,
		status: &heartbeatpb.DispatcherStatus{
			Ack:    &heartbeatpb.ACK{CommitTs: 102},
			Action: &heartbeatpb.DispatcherAction{Action: heartbeatpb.Action_Write, CommitTs: 102},
		},
	}

	time.Sleep(30 * time.Millisecond)

	err := mock.ExpectationsWereMet()
	require.NoError(t, err)

	dispatcher.CollectDispatcherHeartBeatInfo(heartBeatInfo)
	require.Equal(t, uint64(104), heartBeatInfo.CheckpointTs)
}
