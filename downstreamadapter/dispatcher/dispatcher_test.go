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

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func newTestMockDB(t *testing.T) (db *sql.DB, mock sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.Nil(t, err)
	return
}

// // BasicDispatcher with normal dml cases
// func TestBasicDispatcher(t *testing.T) {
// 	db, mock := newTestMockDB(t)
// 	defer db.Close()

// 	mysqlSink := sink.NewMysqlSink(model.DefaultChangeFeedID("test1"), 8, writer.NewMysqlConfig(), db)
// 	tableSpan := &heartbeatpb.TableSpan{TableID: 1}
// 	startTs := uint64(100)

// 	tableSpanStatusChan := make(chan *heartbeatpb.TableSpanStatus, 10)
// 	filter, _ := filter.NewFilter(&config.FilterConfig{}, "", false)

// 	dispatcher := NewDispatcher(common.NewDispatcherID(), tableSpan, mysqlSink, startTs, tableSpanStatusChan, filter, 0)

// 	dispatcherEventsDynamicStream := GetDispatcherEventsDynamicStream()

// 	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
// 		StartTs:  100,
// 		CommitTs: 101,
// 		Rows: []*common.RowChangedEvent{
// 			{
// 				TableInfo: &common.TableInfo{
// 					TableName: common.TableName{
// 						Schema: "test_schema",
// 						Table:  "test_table",
// 					},
// 				},
// 				Columns: []*common.Column{
// 					{Name: "id", Value: 1, Flag: common.HandleKeyFlag | common.PrimaryKeyFlag},
// 					{Name: "name", Value: "Alice"},
// 				},
// 			},
// 		},
// 		DispatcherID: dispatcher.id,
// 	}

// 	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
// 		StartTs:  102,
// 		CommitTs: 105,
// 		Rows: []*common.RowChangedEvent{
// 			{
// 				TableInfo: &common.TableInfo{
// 					TableName: common.TableName{
// 						Schema: "test",
// 						Table:  "users",
// 					},
// 				},
// 				PreColumns: []*common.Column{
// 					{Name: "id", Value: 1, Flag: common.HandleKeyFlag | common.PrimaryKeyFlag},
// 					{Name: "name", Value: "Alice"},
// 				},
// 				Columns: []*common.Column{
// 					{Name: "id", Value: 1, Flag: common.HandleKeyFlag | common.PrimaryKeyFlag},
// 					{Name: "name", Value: "Bob"},
// 				},
// 			},
// 		},
// 		DispatcherID: dispatcher.id,
// 	}
// 	dispatcherEventsDynamicStream.In() <- common.ResolvedEvent{
// 		DispatcherID: dispatcher.id,
// 		ResolvedTs:   110,
// 	}

// 	heartBeatInfo := &HeartBeatInfo{}
// 	dispatcher.CollectDispatcherHeartBeatInfo(heartBeatInfo)
// 	require.Equal(t, uint64(100), heartBeatInfo.CheckpointTs)

// 	mock.ExpectBegin()
// 	mock.ExpectExec("INSERT INTO `test_schema`.`test_table` (`id`,`name`) VALUES (?,?);UPDATE `test`.`users` SET `id` = ?, `name` = ? WHERE `id` = ? LIMIT 1").
// 		WithArgs(1, "Alice", 1, "Bob", 1).
// 		WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectCommit()

// 	time.Sleep(1 * time.Second)

// 	err := mock.ExpectationsWereMet()
// 	require.NoError(t, err)

// 	dispatcher.CollectDispatcherHeartBeatInfo(heartBeatInfo)
// 	require.Equal(t, uint64(110), heartBeatInfo.CheckpointTs)
// }

// func TestDispatcherWithSingleTableDDL(t *testing.T) {
// 	db, mock := newTestMockDB(t)
// 	defer db.Close()

// 	mock.ExpectBegin()
// 	mock.ExpectExec("USE `test_schema`;").
// 		WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectExec("ALTER TABLE `test_schema`.`test_table` ADD COLUMN `age` INT").
// 		WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectCommit()

// 	mysqlSink := sink.NewMysqlSink(model.DefaultChangeFeedID("test1"), 8, writer.NewMysqlConfig(), db)
// 	tableSpan := &heartbeatpb.TableSpan{TableID: 1}
// 	startTs := uint64(100)

// 	tableSpanStatusChan := make(chan *heartbeatpb.TableSpanStatus, 10)
// 	filter, _ := filter.NewFilter(&config.FilterConfig{}, "", false)

// 	dispatcher := NewDispatcher(common.NewDispatcherID(), tableSpan, mysqlSink, startTs, tableSpanStatusChan, filter, 0)

// 	dispatcherEventsDynamicStream := GetDispatcherEventsDynamicStream()
// 	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
// 		StartTs:  102,
// 		CommitTs: 102,
// 		DDLEvent: &common.DDLEvent{
// 			Job: &timodel.Job{
// 				Type:       timodel.ActionAddColumn,
// 				SchemaName: "test_schema",
// 				TableName:  "test_table",
// 				Query:      "ALTER TABLE `test_schema`.`test_table` ADD COLUMN `age` INT",
// 			},
// 			CommitTS: 102,
// 		},
// 		DispatcherID: dispatcher.id,
// 	}

// 	time.Sleep(10 * time.Millisecond)

// 	heartBeatInfo := &HeartBeatInfo{}
// 	dispatcher.CollectDispatcherHeartBeatInfo(heartBeatInfo)
// 	require.Equal(t, uint64(101), heartBeatInfo.CheckpointTs)

// 	dispatcherEventsDynamicStream.In() <- common.ResolvedEvent{
// 		DispatcherID: dispatcher.id,
// 		ResolvedTs:   110,
// 	}

// 	time.Sleep(1 * time.Second)
// 	dispatcher.CollectDispatcherHeartBeatInfo(heartBeatInfo)
// 	require.Equal(t, uint64(110), heartBeatInfo.CheckpointTs)

// 	err := mock.ExpectationsWereMet()
// 	require.NoError(t, err)
// }

// func TestDispatcherWithCrossTableDDL(t *testing.T) {
// 	db, mock := newTestMockDB(t)
// 	defer db.Close()

// 	mock.ExpectBegin()
// 	mock.ExpectExec("USE `test_schema`;").
// 		WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectExec("DROP TABLE `test_schema`.`test_table`").
// 		WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectCommit()

// 	mysqlSink := sink.NewMysqlSink(model.DefaultChangeFeedID("test1"), 8, writer.NewMysqlConfig(), db)
// 	tableSpan := heartbeatpb.DDLSpan
// 	startTs := uint64(100)

// 	tableSpanStatusChan := make(chan *heartbeatpb.TableSpanStatus, 10)
// 	filter, _ := filter.NewFilter(&config.FilterConfig{}, "", false)

// 	dispatcher := NewDispatcher(common.NewDispatcherID(), tableSpan, mysqlSink, startTs, tableSpanStatusChan, filter, 0)

// 	dispatcherEventsDynamicStream := GetDispatcherEventsDynamicStream()
// 	dispatcherStatusDynamicStream := GetDispatcherStatusDynamicStream()

// 	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
// 		StartTs:  102,
// 		CommitTs: 102,
// 		DDLEvent: &common.DDLEvent{
// 			Job: &timodel.Job{
// 				Type:       timodel.ActionDropTable,
// 				SchemaName: "test_schema",
// 				TableName:  "test_table",
// 				Query:      "DROP TABLE `test_schema`.`test_table`",
// 			},
// 			CommitTS: 102,
// 		},
// 		DispatcherID: dispatcher.id,
// 	}

// 	// 检查可以从 tableSpanStatusChan 中拿到消息
// 	<-dispatcher.GetStatusesChan()

// 	require.NotEqual(t, dispatcher.ddlPendingEvent, nil)

// 	dispatcherStatusDynamicStream.In() <- NewDispatcherStatusWithID(&heartbeatpb.DispatcherStatus{
// 		Ack:    &heartbeatpb.ACK{CommitTs: 102},
// 		Action: &heartbeatpb.DispatcherAction{Action: heartbeatpb.Action_Write, CommitTs: 102},
// 	}, dispatcher.id)

// 	time.Sleep(10 * time.Millisecond)

// 	err := mock.ExpectationsWereMet()
// 	require.NoError(t, err)

// 	heartBeatInfo := &HeartBeatInfo{}
// 	dispatcher.CollectDispatcherHeartBeatInfo(heartBeatInfo)
// 	require.Equal(t, uint64(101), heartBeatInfo.CheckpointTs)
// }

// func TestDispatcherWithCrossTableDDLAndDML(t *testing.T) {
// 	db, mock := newTestMockDB(t)
// 	defer db.Close()

// 	mock.ExpectBegin()
// 	mock.ExpectExec("USE `test_schema`;").
// 		WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectExec("DROP TABLE `test_schema`.`test_table`").
// 		WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectCommit()

// 	mock.ExpectBegin()
// 	mock.ExpectExec("UPDATE `test`.`users` SET `id` = ?, `name` = ? WHERE `id` = ? LIMIT 1").
// 		WithArgs(1, "Bob", 1).
// 		WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectCommit()

// 	mysqlSink := sink.NewMysqlSink(model.DefaultChangeFeedID("test1"), 8, writer.NewMysqlConfig(), db)
// 	tableSpan := heartbeatpb.DDLSpan
// 	startTs := uint64(100)

// 	tableSpanStatusChan := make(chan *heartbeatpb.TableSpanStatus, 10)
// 	filter, _ := filter.NewFilter(&config.FilterConfig{}, "", false)

// 	dispatcher := NewDispatcher(common.NewDispatcherID(), tableSpan, mysqlSink, startTs, tableSpanStatusChan, filter, 0)

// 	dispatcherEventsDynamicStream := GetDispatcherEventsDynamicStream()
// 	dispatcherStatusDynamicStream := GetDispatcherStatusDynamicStream()

// 	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
// 		StartTs:  102,
// 		CommitTs: 102,
// 		DDLEvent: &common.DDLEvent{
// 			Job: &timodel.Job{
// 				Type:       timodel.ActionDropTable,
// 				SchemaName: "test_schema",
// 				TableName:  "test_table",
// 				Query:      "DROP TABLE `test_schema`.`test_table`",
// 			},
// 			CommitTS: 102,
// 		},
// 		DispatcherID: dispatcher.id,
// 	}

// 	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
// 		StartTs:  102,
// 		CommitTs: 105,
// 		Rows: []*common.RowChangedEvent{
// 			{
// 				TableInfo: &common.TableInfo{
// 					TableName: common.TableName{
// 						Schema: "test",
// 						Table:  "users",
// 					},
// 				},
// 				PreColumns: []*common.Column{
// 					{Name: "id", Value: 1, Flag: common.HandleKeyFlag | common.PrimaryKeyFlag},
// 					{Name: "name", Value: "Alice"},
// 				},
// 				Columns: []*common.Column{
// 					{Name: "id", Value: 1, Flag: common.HandleKeyFlag | common.PrimaryKeyFlag},
// 					{Name: "name", Value: "Bob"},
// 				},
// 			},
// 		},
// 		DispatcherID: dispatcher.id,
// 	}

// 	// 检查可以从 tableSpanStatusChan 中拿到消息
// 	<-dispatcher.GetStatusesChan()
// 	require.NotEqual(t, dispatcher.ddlPendingEvent, nil)

// 	heartBeatInfo := &HeartBeatInfo{}
// 	dispatcher.CollectDispatcherHeartBeatInfo(heartBeatInfo)
// 	require.Equal(t, uint64(100), heartBeatInfo.CheckpointTs)

// 	dispatcherStatusDynamicStream.In() <- NewDispatcherStatusWithID(&heartbeatpb.DispatcherStatus{
// 		Ack:    &heartbeatpb.ACK{CommitTs: 102},
// 		Action: &heartbeatpb.DispatcherAction{Action: heartbeatpb.Action_Write, CommitTs: 102},
// 	}, dispatcher.id)

// 	time.Sleep(30 * time.Millisecond)

// 	err := mock.ExpectationsWereMet()
// 	require.NoError(t, err)

// 	dispatcher.CollectDispatcherHeartBeatInfo(heartBeatInfo)
// 	require.Equal(t, uint64(104), heartBeatInfo.CheckpointTs)
// }

// func mockMaintainerResponse(statusChan chan *heartbeatpb.TableSpanStatus, dbDispatcherIdsMap map[int64][]common.DispatcherID) {
// 	dispatcherStatusDynamicStream := GetDispatcherStatusDynamicStream()
// 	tsMap := make(map[common.DispatcherID]uint64)
// 	finishCommitTs := uint64(0)
// 	for {
// 		select {
// 		case msg := <-statusChan:
// 			if msg.State == nil {
// 				continue
// 			}
// 			if msg.State.IsBlocked {
// 				response := &heartbeatpb.DispatcherStatus{
// 					Ack: &heartbeatpb.ACK{CommitTs: msg.State.BlockTs},
// 				}
// 				if msg.State.BlockTs <= finishCommitTs {
// 					continue
// 				}
// 				if msg.State.GetBlockDispatchers() == nil {
// 					log.Error("BlockDispatcherIDs is nil, but is blocked is true")
// 				} else {
// 					tsMap[common.NewDispatcherIDFromPB(msg.ID)] = msg.State.BlockTs
// 					blockedDispatchers := msg.State.GetBlockDispatchers()
// 					var dispatcherIds []common.DispatcherID
// 					if blockedDispatchers.InfluenceType == heartbeatpb.InfluenceType_Normal {
// 						for _, dispatcherID := range blockedDispatchers.DispatcherIDs {
// 							dispatcherIds = append(dispatcherIds, common.NewDispatcherIDFromPB(dispatcherID))
// 						}
// 					} else if blockedDispatchers.InfluenceType == heartbeatpb.InfluenceType_DB {
// 						schemaID := blockedDispatchers.SchemaID
// 						dispatcherIds = dbDispatcherIdsMap[schemaID]
// 					}

// 					flag := true
// 					for _, dispatcherID := range dispatcherIds {
// 						if tsMap[dispatcherID] < msg.State.BlockTs {
// 							flag = false
// 							break
// 						}
// 					}
// 					if flag {
// 						finishCommitTs = msg.State.BlockTs
// 						response.Action = &heartbeatpb.DispatcherAction{
// 							Action:   heartbeatpb.Action_Write,
// 							CommitTs: msg.State.BlockTs,
// 						}
// 						dispatcherStatusDynamicStream.In() <- NewDispatcherStatusWithID(response, common.NewDispatcherIDFromPB(msg.ID))
// 						// 同时通知相关的其他 span
// 						for _, dispatcherID := range dispatcherIds {
// 							if dispatcherID != common.NewDispatcherIDFromPB(msg.ID) {
// 								dispatcherStatusDynamicStream.In() <- NewDispatcherStatusWithID(&heartbeatpb.DispatcherStatus{
// 									Action: &heartbeatpb.DispatcherAction{
// 										Action:   heartbeatpb.Action_Pass,
// 										CommitTs: msg.State.BlockTs,
// 									},
// 								}, dispatcherID)
// 							}
// 						}
// 					}

// 				}
// 			}
// 		}
// 	}
// }

// func TestMultiDispatcherWithMultipleDDLs(t *testing.T) {
// 	db, mock := newTestMockDB(t)
// 	defer db.Close()

// 	mock.MatchExpectationsInOrder(false)
// 	mock.ExpectBegin()
// 	mock.ExpectExec("Create database `test_schema`").
// 		WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectCommit()

// 	mock.ExpectBegin()
// 	mock.ExpectExec("USE `test_schema`;").WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectExec("Create table `test_schema`.`test_table` (id int primary key, name varchar(255))").
// 		WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectCommit()

// 	mock.ExpectBegin()
// 	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectExec("ALTER TABLE `test`.`table1` ADD COLUMN `age` INT").
// 		WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectCommit()

// 	mock.ExpectBegin()
// 	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectExec("ALTER TABLE `test`.`table2` ADD COLUMN `age` INT").
// 		WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectCommit()

// 	mock.ExpectBegin()
// 	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectExec("TRUNCATE TABLE `test`.`table1`").WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectCommit()

// 	mock.ExpectBegin()
// 	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectExec("DROP TABLE `test`.`table2`").WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectCommit()

// 	mock.ExpectBegin()
// 	mock.ExpectExec("DROP DATABASE `test`").WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectCommit()

// 	mysqlSink := sink.NewMysqlSink(model.DefaultChangeFeedID("test1"), 8, writer.NewMysqlConfig(), db)
// 	filter, _ := filter.NewFilter(&config.FilterConfig{}, "", false)
// 	statusChan := make(chan *heartbeatpb.TableSpanStatus, 10)
// 	startTs := uint64(100)

// 	ddlTableSpan := heartbeatpb.DDLSpan

// 	tableTriggerEventDispatcher := NewDispatcher(common.NewDispatcherID(), ddlTableSpan, mysqlSink, startTs, statusChan, filter, 0)

// 	table1TableSpan := &heartbeatpb.TableSpan{TableID: 1}
// 	table2TableSpan := &heartbeatpb.TableSpan{TableID: 2}

// 	table1Dispatcher := NewDispatcher(common.NewDispatcherID(), table1TableSpan, mysqlSink, startTs, statusChan, filter, 0)
// 	table2Dispatcher := NewDispatcher(common.NewDispatcherID(), table2TableSpan, mysqlSink, startTs, statusChan, filter, 0)

// 	dbDispatcherIdsMap := make(map[int64][]common.DispatcherID)
// 	dbDispatcherIdsMap[1] = append(dbDispatcherIdsMap[1], table1Dispatcher.id)
// 	dbDispatcherIdsMap[1] = append(dbDispatcherIdsMap[1], table2Dispatcher.id)
// 	dbDispatcherIdsMap[1] = append(dbDispatcherIdsMap[1], tableTriggerEventDispatcher.id)

// 	dispatcherEventsDynamicStream := GetDispatcherEventsDynamicStream()

// 	go mockMaintainerResponse(statusChan, dbDispatcherIdsMap)

// 	/*
// 		push these events in order:
// 		1. Create Schema
// 		2. Create Table 3
// 		5. Add Column in table 1
// 		6. Add Column in table 2
// 		9. Truncate table 1
// 		10. Drop Table 2
// 		11. Drop Schema
// 	*/

// 	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
// 		StartTs:  101,
// 		CommitTs: 102,
// 		DDLEvent: &common.DDLEvent{
// 			Job: &timodel.Job{
// 				Type:       timodel.ActionCreateSchema,
// 				SchemaName: "test_schema",
// 				Query:      "Create database `test_schema`",
// 			},
// 			CommitTS: 102,
// 		},
// 		DispatcherID: tableTriggerEventDispatcher.id,
// 	}

// 	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
// 		StartTs:  102,
// 		CommitTs: 103,
// 		DDLEvent: &common.DDLEvent{
// 			Job: &timodel.Job{
// 				Type:       timodel.ActionCreateTable,
// 				SchemaName: "test_schema",
// 				TableName:  "test_table",
// 				Query:      "Create table `test_schema`.`test_table` (id int primary key, name varchar(255))",
// 			},
// 			CommitTS: 103,
// 			NeedAddedTables: []common.Table{
// 				{
// 					SchemaID: 1,
// 					TableID:  3,
// 				},
// 			},
// 		},
// 		DispatcherID: tableTriggerEventDispatcher.id,
// 	}

// 	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
// 		StartTs:  105,
// 		CommitTs: 106,
// 		DDLEvent: &common.DDLEvent{
// 			Job: &timodel.Job{
// 				Type:       timodel.ActionModifyColumn,
// 				SchemaName: "test",
// 				TableName:  "table1",
// 				Query:      "ALTER TABLE `test`.`table1` ADD COLUMN `age` INT",
// 			},
// 			CommitTS: 106,
// 		},
// 		DispatcherID: table1Dispatcher.id,
// 	}

// 	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
// 		StartTs:  106,
// 		CommitTs: 107,
// 		DDLEvent: &common.DDLEvent{
// 			Job: &timodel.Job{
// 				Type:       timodel.ActionModifyColumn,
// 				SchemaName: "test",
// 				TableName:  "table2",
// 				Query:      "ALTER TABLE `test`.`table2` ADD COLUMN `age` INT",
// 			},
// 			CommitTS: 107,
// 		},
// 		DispatcherID: table2Dispatcher.id,
// 	}

// 	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
// 		StartTs:  109,
// 		CommitTs: 110,
// 		DDLEvent: &common.DDLEvent{
// 			Job: &timodel.Job{
// 				Type:       timodel.ActionTruncateTable,
// 				SchemaName: "test",
// 				TableName:  "table1",
// 				Query:      "TRUNCATE TABLE `test`.`table1`",
// 			},
// 			CommitTS: 110,
// 			BlockedDispatchers: &common.InfluencedDispatchers{
// 				InfluenceType: common.Normal,
// 				DispatcherIDs: []*heartbeatpb.DispatcherID{
// 					table1Dispatcher.id.ToPB(),
// 					tableTriggerEventDispatcher.id.ToPB(),
// 				},
// 			},
// 			NeedAddedTables: []common.Table{
// 				{
// 					SchemaID: 1,
// 					TableID:  1,
// 				},
// 			},
// 			NeedDroppedDispatchers: &common.InfluencedDispatchers{
// 				InfluenceType: common.Normal,
// 				DispatcherIDs: []*heartbeatpb.DispatcherID{
// 					table1Dispatcher.id.ToPB(),
// 				},
// 			},
// 		},
// 		DispatcherID: table1Dispatcher.id,
// 	}

// 	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
// 		StartTs:  111,
// 		CommitTs: 112,
// 		DDLEvent: &common.DDLEvent{
// 			Job: &timodel.Job{
// 				Type:       timodel.ActionDropTable,
// 				SchemaName: "test",
// 				TableName:  "table2",
// 				Query:      "DROP TABLE `test`.`table2`",
// 			},
// 			CommitTS: 112,
// 			BlockedDispatchers: &common.InfluencedDispatchers{
// 				InfluenceType: common.Normal,
// 				DispatcherIDs: []*heartbeatpb.DispatcherID{
// 					table2Dispatcher.id.ToPB(),
// 					tableTriggerEventDispatcher.id.ToPB(),
// 				},
// 			},
// 			NeedDroppedDispatchers: &common.InfluencedDispatchers{
// 				InfluenceType: common.Normal,
// 				DispatcherIDs: []*heartbeatpb.DispatcherID{
// 					table2Dispatcher.id.ToPB(),
// 				},
// 			},
// 		},
// 		DispatcherID: tableTriggerEventDispatcher.id,
// 	}

// 	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
// 		StartTs:  111,
// 		CommitTs: 112,
// 		DDLEvent: &common.DDLEvent{
// 			Job: &timodel.Job{
// 				Type:       timodel.ActionDropTable,
// 				SchemaName: "test",
// 				TableName:  "table2",
// 				Query:      "DROP TABLE `test`.`table2`",
// 			},
// 			CommitTS: 112,
// 			BlockedDispatchers: &common.InfluencedDispatchers{
// 				InfluenceType: common.Normal,
// 				DispatcherIDs: []*heartbeatpb.DispatcherID{
// 					table2Dispatcher.id.ToPB(),
// 					tableTriggerEventDispatcher.id.ToPB(),
// 				},
// 			},
// 			NeedDroppedDispatchers: &common.InfluencedDispatchers{
// 				InfluenceType: common.Normal,
// 				DispatcherIDs: []*heartbeatpb.DispatcherID{
// 					table2Dispatcher.id.ToPB(),
// 				},
// 			},
// 		},
// 		DispatcherID: table2Dispatcher.id,
// 	}

// 	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
// 		StartTs:  112,
// 		CommitTs: 113,
// 		DDLEvent: &common.DDLEvent{
// 			Job: &timodel.Job{
// 				Type:       timodel.ActionDropSchema,
// 				SchemaName: "test",
// 				TableName:  "table2",
// 				Query:      "DROP DATABASE `test`",
// 			},
// 			CommitTS: 112,
// 			BlockedDispatchers: &common.InfluencedDispatchers{
// 				InfluenceType: common.DB,
// 				SchemaID:      1, // test schema id is 1
// 			},
// 			NeedDroppedDispatchers: &common.InfluencedDispatchers{
// 				InfluenceType: common.DB,
// 				SchemaID:      1, // test schema id is 1
// 			},
// 		},
// 		DispatcherID: table1Dispatcher.id,
// 	}

// 	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{ // just a mock. In real, table2 is dropped and won't get the event
// 		StartTs:  112,
// 		CommitTs: 113,
// 		DDLEvent: &common.DDLEvent{
// 			Job: &timodel.Job{
// 				Type:       timodel.ActionDropSchema,
// 				SchemaName: "test",
// 				TableName:  "table2",
// 				Query:      "DROP DATABASE `test`",
// 			},
// 			CommitTS: 112,
// 			BlockedDispatchers: &common.InfluencedDispatchers{
// 				InfluenceType: common.DB,
// 				SchemaID:      1, // test schema id is 1
// 			},
// 			NeedDroppedDispatchers: &common.InfluencedDispatchers{
// 				InfluenceType: common.DB,
// 				SchemaID:      1, // test schema id is 1
// 			},
// 		},
// 		DispatcherID: table2Dispatcher.id,
// 	}

// 	dispatcherEventsDynamicStream.In() <- &common.TxnEvent{
// 		StartTs:  112,
// 		CommitTs: 113,
// 		DDLEvent: &common.DDLEvent{
// 			Job: &timodel.Job{
// 				Type:       timodel.ActionDropSchema,
// 				SchemaName: "test",
// 				TableName:  "table2",
// 				Query:      "DROP DATABASE `test`",
// 			},
// 			CommitTS: 112,
// 			BlockedDispatchers: &common.InfluencedDispatchers{
// 				InfluenceType: common.DB,
// 				SchemaID:      1, // test schema id is 1
// 			},
// 			NeedDroppedDispatchers: &common.InfluencedDispatchers{
// 				InfluenceType: common.DB,
// 				SchemaID:      1, // test schema id is 1
// 			},
// 		},
// 		DispatcherID: tableTriggerEventDispatcher.id,
// 	}

// 	time.Sleep(5 * time.Second)
// 	err := mock.ExpectationsWereMet()
// 	require.NoError(t, err)

// }
