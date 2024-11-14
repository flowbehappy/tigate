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
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/ticdc/downstreamadapter/sink/types"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

var count = 0

// Test callback and tableProgress works as expected after AddDMLEvent
func TestMysqlSinkBasicFunctionality(t *testing.T) {
	sink, mock := MysqlSinkForTest()
	tableProgress := types.NewTableProgress()
	ts, isEmpty := tableProgress.GetCheckpointTs()
	require.NotEqual(t, ts, 0)
	require.Equal(t, isEmpty, true)

	count = 0

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	ddlEvent := &commonEvent.DDLEvent{
		Query:      job.Query,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		FinishedTs: 1,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{{TableID: 1, SchemaID: 1}},
		PostTxnFlushed: []func(){
			func() { count++ },
		},
	}

	ddlEvent2 := &commonEvent.DDLEvent{
		Query:      job.Query,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		FinishedTs: 4,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{{TableID: 1, SchemaID: 1}},
		PostTxnFlushed: []func(){
			func() { count++ },
		},
	}

	dmlEvent := helper.DML2Event("test", "t", "insert into t values (1, 'test')", "insert into t values (2, 'test2');")
	dmlEvent.PostTxnFlushed = []func(){
		func() { count++ },
	}
	dmlEvent.CommitTs = 2

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table t (id int primary key, name varchar(32));").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS tidb_cdc").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("USE tidb_cdc").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS ddl_ts_v1
		(
			ticdc_cluster_id varchar (255),
			changefeed varchar(255),
			ddl_ts varchar(18),
			table_id bigint(21),
			created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			INDEX (ticdc_cluster_id, changefeed, table_id),
			PRIMARY KEY (ticdc_cluster_id, changefeed, table_id)
		);`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id) VALUES ('default', 'test/test', '1', 0), ('default', 'test/test', '1', 1) ON DUPLICATE KEY UPDATE ddl_ts=VALUES(ddl_ts), created_at=CURRENT_TIMESTAMP;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?);INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?)").
		WithArgs(1, "test", 2, "test2").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := sink.WriteBlockEvent(ddlEvent, tableProgress)
	require.NoError(t, err)

	sink.AddDMLEvent(dmlEvent, tableProgress)
	time.Sleep(1 * time.Second)

	sink.PassBlockEvent(ddlEvent2, tableProgress)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)

	ts, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, ts, uint64(3))
	require.Equal(t, isEmpty, true)

	require.Equal(t, count, 3)
}

// test the situation meets error when executing DML
// whether the sink state is correct
func TestMysqlSinkMeetsDMLError(t *testing.T) {
	sink, mock := MysqlSinkForTest()
	tableProgress := types.NewTableProgress()
	ts, isEmpty := tableProgress.GetCheckpointTs()
	require.NotEqual(t, ts, 0)
	require.Equal(t, isEmpty, true)

	count = 0

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	dmlEvent := helper.DML2Event("test", "t", "insert into t values (1, 'test')", "insert into t values (2, 'test2');")
	dmlEvent.PostTxnFlushed = []func(){
		func() { count++ },
	}
	dmlEvent.CommitTs = 2

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?);INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?)").
		WithArgs(1, "test", 2, "test2").
		WillReturnError(errors.New("connect: connection refused"))
	mock.ExpectRollback()

	sink.AddDMLEvent(dmlEvent, tableProgress)

	time.Sleep(1 * time.Second)

	err := mock.ExpectationsWereMet()
	require.NoError(t, err)

	ts, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, ts, uint64(1))
	require.Equal(t, isEmpty, false)

	require.Equal(t, count, 0)

	require.Equal(t, sink.IsNormal(), false)
}

// test the situation meets error when executing DDL
// whether the sink state is correct
func TestMysqlSinkMeetsDDLError(t *testing.T) {
	sink, mock := MysqlSinkForTest()
	tableProgress := types.NewTableProgress()
	ts, isEmpty := tableProgress.GetCheckpointTs()
	require.NotEqual(t, ts, 0)
	require.Equal(t, isEmpty, true)

	count = 0

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	ddlEvent := &commonEvent.DDLEvent{
		Query:      job.Query,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		FinishedTs: 2,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{{TableID: 1, SchemaID: 1}},
		PostTxnFlushed: []func(){
			func() { count++ },
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table t (id int primary key, name varchar(32));").WillReturnError(errors.New("connect: connection refused"))
	mock.ExpectRollback()

	sink.WriteBlockEvent(ddlEvent, tableProgress)

	err := mock.ExpectationsWereMet()
	require.NoError(t, err)

	ts, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, ts, uint64(1))
	require.Equal(t, isEmpty, false)

	require.Equal(t, count, 0)

	require.Equal(t, sink.IsNormal(), false)
}
