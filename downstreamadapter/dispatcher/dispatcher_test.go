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
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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

	dispatcher := NewDispatcher(common.NewDispatcherID(), tableSpan, mysqlSink, startTs, tableSpanStatusChan, filter)

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

	dispatcher := NewDispatcher(common.NewDispatcherID(), tableSpan, mysqlSink, startTs, tableSpanStatusChan, filter)

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

	dispatcher := NewDispatcher(common.NewDispatcherID(), tableSpan, mysqlSink, startTs, tableSpanStatusChan, filter)

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

	dispatcherStatusDynamicStream.In() <- &heartbeatpb.DispatcherStatus{
		ID:     dispatcher.id.ToPB(),
		Ack:    &heartbeatpb.ACK{CommitTs: 102},
		Action: &heartbeatpb.DispatcherAction{Action: heartbeatpb.Action_Write, CommitTs: 102},
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

	dispatcher := NewDispatcher(common.NewDispatcherID(), tableSpan, mysqlSink, startTs, tableSpanStatusChan, filter)

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

	dispatcherStatusDynamicStream.In() <- &heartbeatpb.DispatcherStatus{
		ID:     dispatcher.id.ToPB(),
		Ack:    &heartbeatpb.ACK{CommitTs: 102},
		Action: &heartbeatpb.DispatcherAction{Action: heartbeatpb.Action_Write, CommitTs: 102},
	}

	time.Sleep(30 * time.Millisecond)

	err := mock.ExpectationsWereMet()
	require.NoError(t, err)

	dispatcher.CollectDispatcherHeartBeatInfo(heartBeatInfo)
	require.Equal(t, uint64(104), heartBeatInfo.CheckpointTs)
}

func mockMaintainerResponse(statusChan chan *heartbeatpb.TableSpanStatus, dispatcherIDToTableSpanMap map[common.DispatcherID]uint64) {
	dispatcherStatusDynamicStream := GetDispatcherStatusDynamicStream()
	tsMap := make(map[uint64]uint64)
	finishCommitTs := uint64(0)
	for {
		select {
		case msg := <-statusChan:
			if msg.State == nil {
				continue
			}
			if msg.State.IsBlocked {
				response := &heartbeatpb.DispatcherStatus{
					ID:  msg.ID,
					Ack: &heartbeatpb.ACK{CommitTs: msg.State.BlockTs},
				}
				if msg.State.BlockTs <= finishCommitTs {
					continue
				}
				if msg.State.GetBlockTableSpan() == nil {
					response.Action = &heartbeatpb.DispatcherAction{
						Action:   heartbeatpb.Action_Write,
						CommitTs: msg.State.BlockTs,
					}
					dispatcherStatusDynamicStream.In() <- response
				} else {
					tableID := dispatcherIDToTableSpanMap[common.NewDispatcherIDFromPB(msg.ID)]
					tsMap[tableID] = msg.State.BlockTs
					blockedTableSpan := msg.State.GetBlockTableSpan()
					flag := true
					for _, span := range blockedTableSpan {
						if tsMap[span.TableID] < msg.State.BlockTs {
							flag = false
							break
						}
					}
					if flag {
						finishCommitTs = msg.State.BlockTs
						response.Action = &heartbeatpb.DispatcherAction{
							Action:   heartbeatpb.Action_Write,
							CommitTs: msg.State.BlockTs,
						}
						log.Info("Send Write Action to Dispatcher", zap.Any("commitTs", msg.State.BlockTs))
						dispatcherStatusDynamicStream.In() <- response
						// 同时通知相关的其他 span
						for _, span := range blockedTableSpan {
							if span.TableID != dispatcherIDToTableSpanMap[common.NewDispatcherIDFromPB(msg.ID)] {
								dispatcherStatusDynamicStream.In() <- &heartbeatpb.DispatcherStatus{
									ID: msg.ID,
									Action: &heartbeatpb.DispatcherAction{
										Action:   heartbeatpb.Action_Pass,
										CommitTs: msg.State.BlockTs,
									},
								}
							}
						}
					}
				}
			}
		}
	}
}

func TestMultiDispatcherWithMultipleDDLs(t *testing.T) {
	db, mock := newTestMockDB(t)
	defer db.Close()

	mock.MatchExpectationsInOrder(false)
	mock.ExpectBegin()
	mock.ExpectExec("Create database `test_schema`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("USE `test_schema`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("Create table `test_schema`.`test_table` (id int primary key, name varchar(255))").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("ALTER TABLE `test`.`table1` ADD COLUMN `age` INT").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("ALTER TABLE `test`.`table2` ADD COLUMN `age` INT").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("TRUNCATE TABLE `test`.`table1`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("DROP TABLE `test`.`table2`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mysqlSink := sink.NewMysqlSink(model.DefaultChangeFeedID("test1"), 8, writer.NewMysqlConfig(), db)
	filter, _ := filter.NewFilter(&config.ReplicaConfig{Filter: &config.FilterConfig{}}, "")
	statusChan := make(chan *heartbeatpb.TableSpanStatus, 10)
	startTs := uint64(100)

	ddlTableSpan := &common.DDLSpan

	tableTriggerEventDispatcher := NewDispatcher(common.NewDispatcherID(), ddlTableSpan, mysqlSink, startTs, statusChan, filter)

	table1TableSpan := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{TableID: 1}}
	table2TableSpan := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{TableID: 2}}

	table1Dispatcher := NewDispatcher(common.NewDispatcherID(), table1TableSpan, mysqlSink, startTs, statusChan, filter)
	table2Dispatcher := NewDispatcher(common.NewDispatcherID(), table2TableSpan, mysqlSink, startTs, statusChan, filter)

	dispatcherEventsDynamicStream := GetDispatcherEventsDynamicStream()

	dispatcherIDToTableSpanMap := map[common.DispatcherID]uint64{
		table1Dispatcher.id:            1,
		table2Dispatcher.id:            2,
		tableTriggerEventDispatcher.id: ddlTableSpan.TableID,
	}

	go mockMaintainerResponse(statusChan, dispatcherIDToTableSpanMap)

	/*
		push these events in order:
		1. Create Schema
		2. Create Table 3
		5. Add Column in table 1
		6. Add Column in table 2
		9. Truncate table 1
		10. Drop Table 2
	*/

	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
		StartTs:  101,
		CommitTs: 102,
		DDLEvent: &common.DDLEvent{
			Job: &timodel.Job{
				Type:       timodel.ActionCreateSchema,
				SchemaName: "test_schema",
				Query:      "Create database `test_schema`",
			},
			CommitTS: 102,
		},
		DispatcherID: tableTriggerEventDispatcher.id,
	}

	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
		StartTs:  102,
		CommitTs: 103,
		DDLEvent: &common.DDLEvent{
			Job: &timodel.Job{
				Type:       timodel.ActionCreateTable,
				SchemaName: "test_schema",
				TableName:  "test_table",
				Query:      "Create table `test_schema`.`test_table` (id int primary key, name varchar(255))",
			},
			CommitTS: 103,
			NeedAddedTableSpan: []*heartbeatpb.TableSpan{
				{TableID: 3},
			},
		},
		DispatcherID: tableTriggerEventDispatcher.id,
	}

	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
		StartTs:  105,
		CommitTs: 106,
		DDLEvent: &common.DDLEvent{
			Job: &timodel.Job{
				Type:       timodel.ActionModifyColumn,
				SchemaName: "test",
				TableName:  "table1",
				Query:      "ALTER TABLE `test`.`table1` ADD COLUMN `age` INT",
			},
			CommitTS: 106,
		},
		DispatcherID: table1Dispatcher.id,
	}

	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
		StartTs:  106,
		CommitTs: 107,
		DDLEvent: &common.DDLEvent{
			Job: &timodel.Job{
				Type:       timodel.ActionModifyColumn,
				SchemaName: "test",
				TableName:  "table2",
				Query:      "ALTER TABLE `test`.`table2` ADD COLUMN `age` INT",
			},
			CommitTS: 107,
		},
		DispatcherID: table2Dispatcher.id,
	}

	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
		StartTs:  109,
		CommitTs: 110,
		DDLEvent: &common.DDLEvent{
			Job: &timodel.Job{
				Type:       timodel.ActionTruncateTable,
				SchemaName: "test",
				TableName:  "table1",
				Query:      "TRUNCATE TABLE `test`.`table1`",
			},
			CommitTS: 110,
			BlockedTableSpan: []*heartbeatpb.TableSpan{
				{TableID: 1}, {TableID: ddlTableSpan.TableID},
			},
			NeedAddedTableSpan: []*heartbeatpb.TableSpan{
				{TableID: 4},
			},
			NeedDroppedTableSpan: []*heartbeatpb.TableSpan{
				{TableID: 1},
			},
		},
		DispatcherID: table1Dispatcher.id,
	}

	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
		StartTs:  109,
		CommitTs: 110,
		DDLEvent: &common.DDLEvent{
			Job: &timodel.Job{
				Type:       timodel.ActionTruncateTable,
				SchemaName: "test",
				TableName:  "table1",
				Query:      "TRUNCATE TABLE `test`.`table1`",
			},
			CommitTS: 110,
			BlockedTableSpan: []*heartbeatpb.TableSpan{
				{TableID: 1}, {TableID: ddlTableSpan.TableID},
			},
			NeedAddedTableSpan: []*heartbeatpb.TableSpan{
				{TableID: 4},
			},
			NeedDroppedTableSpan: []*heartbeatpb.TableSpan{
				{TableID: 1},
			},
		},
		DispatcherID: tableTriggerEventDispatcher.id,
	}

	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
		StartTs:  111,
		CommitTs: 112,
		DDLEvent: &common.DDLEvent{
			Job: &timodel.Job{
				Type:       timodel.ActionDropTable,
				SchemaName: "test",
				TableName:  "table2",
				Query:      "DROP TABLE `test`.`table2`",
			},
			CommitTS: 112,
			BlockedTableSpan: []*heartbeatpb.TableSpan{
				{TableID: 2}, {TableID: ddlTableSpan.TableID},
			},
			NeedDroppedTableSpan: []*heartbeatpb.TableSpan{
				{TableID: 2},
			},
		},
		DispatcherID: tableTriggerEventDispatcher.id,
	}

	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
		StartTs:  111,
		CommitTs: 112,
		DDLEvent: &common.DDLEvent{
			Job: &timodel.Job{
				Type:       timodel.ActionDropTable,
				SchemaName: "test",
				TableName:  "table2",
				Query:      "DROP TABLE `test`.`table2`",
			},
			CommitTS: 112,
			BlockedTableSpan: []*heartbeatpb.TableSpan{
				{TableID: 2}, {TableID: ddlTableSpan.TableID},
			},
			NeedDroppedTableSpan: []*heartbeatpb.TableSpan{
				{TableID: 2},
			},
		},
		DispatcherID: table2Dispatcher.id,
	}

	time.Sleep(5 * time.Second)
	err := mock.ExpectationsWereMet()
	require.NoError(t, err)

}
