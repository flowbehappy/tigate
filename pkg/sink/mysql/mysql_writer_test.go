package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func newTestMysqlWriter(t *testing.T) (*MysqlWriter, *sql.DB, sqlmock.Sqlmock) {
	db, mock := newTestMockDB(t)

	ctx := context.Background()
	cfg := &MysqlConfig{
		MaxAllowedPacket:   int64(variable.DefMaxAllowedPacket),
		SyncPointRetention: time.Duration(100 * time.Second),
	}
	changefeedID := common.NewChangefeedID4Test("test", "test")
	statistics := metrics.NewStatistics(changefeedID, "mysqlSink")
	writer := NewMysqlWriter(ctx, db, cfg, changefeedID, statistics)

	return writer, db, mock
}

func newTestMockDB(t *testing.T) (db *sql.DB, mock sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.Nil(t, err)
	return
}

func TestMysqlWriter_FlushDML(t *testing.T) {
	writer, db, mock := newTestMysqlWriter(t)
	defer db.Close()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	dmlEvent := helper.DML2Event("test", "t", "insert into t values (1, 'test')", "insert into t values (2, 'test2');")
	dmlEvent.CommitTs = 2
	dmlEvent.ReplicatingTs = 1

	dmlEvent2 := helper.DML2Event("test", "t", "insert into t values (3, 'test3');")
	dmlEvent2.CommitTs = 3
	dmlEvent2.ReplicatingTs = 4

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?);INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?);REPLACE INTO `test`.`t` (`id`,`name`) VALUES (?,?)").
		WithArgs(1, "test", 2, "test2", 3, "test3").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := writer.Flush([]*commonEvent.DMLEvent{dmlEvent, dmlEvent2}, 0)
	require.NoError(t, err)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

// Test flush ddl event
// Ensure the ddl query will be write to the databases
// and the ddl_ts_v1 table will be updated with the ddl_ts and table_id
func TestMysqlWriter_FlushDDLEvent(t *testing.T) {
	writer, db, mock := newTestMysqlWriter(t)
	defer db.Close()

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
	}

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

	err := writer.FlushDDLEvent(ddlEvent)
	require.NoError(t, err)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)

	// another flush ddl event
	job = helper.DDL2Job("alter table t add column age int;")
	require.NotNil(t, job)

	ddlEvent = &commonEvent.DDLEvent{
		Query:      job.Query,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		FinishedTs: 2,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{1},
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("alter table t add column age int;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id) VALUES ('default', 'test/test', '2', 1) ON DUPLICATE KEY UPDATE ddl_ts=VALUES(ddl_ts), created_at=CURRENT_TIMESTAMP;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = writer.FlushDDLEvent(ddlEvent)
	require.NoError(t, err)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestMysqlWriter_Flush_EmptyEvents(t *testing.T) {
	writer, db, mock := newTestMysqlWriter(t)
	defer db.Close()

	events := []*commonEvent.DMLEvent{}

	err := writer.Flush(events, 0)
	require.NoError(t, err)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestMysqlWriter_FlushSyncPointEvent(t *testing.T) {
	writer, db, mock := newTestMysqlWriter(t)
	defer db.Close()

	syncPointEvent := &commonEvent.SyncPointEvent{
		CommitTs: 1,
	}

	mock.ExpectBegin()
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS tidb_cdc").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("USE tidb_cdc").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS syncpoint_v1
		(
			ticdc_cluster_id varchar (255),
			changefeed varchar(255),
			primary_ts varchar(18),
			secondary_ts varchar(18),
			created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			INDEX (created_at),
			PRIMARY KEY (changefeed, primary_ts)
		);`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectQuery("select @@tidb_current_ts").WillReturnRows(sqlmock.NewRows([]string{"@@tidb_current_ts"}).AddRow(0))
	mock.ExpectExec("insert ignore into tidb_cdc.syncpoint_v1 (ticdc_cluster_id, changefeed, primary_ts, secondary_ts) VALUES ('default', 'test/test', 1, 0)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("set global tidb_external_ts = 0").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := writer.FlushSyncPointEvent(syncPointEvent)
	require.NoError(t, err)

	syncPointEvent = &commonEvent.SyncPointEvent{
		CommitTs: 2,
	}

	mock.ExpectBegin()
	mock.ExpectQuery("select @@tidb_current_ts").WillReturnRows(sqlmock.NewRows([]string{"@@tidb_current_ts"}).AddRow(2))
	mock.ExpectExec("insert ignore into tidb_cdc.syncpoint_v1 (ticdc_cluster_id, changefeed, primary_ts, secondary_ts) VALUES ('default', 'test/test', 2, 2)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("set global tidb_external_ts = 2").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = writer.FlushSyncPointEvent(syncPointEvent)
	require.NoError(t, err)
}

func TestMysqlWriter_RemoveDDLTsTable(t *testing.T) {
	writer, db, mock := newTestMysqlWriter(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed) IN (('default', 'test/test'))").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := writer.RemoveDDLTsItem()
	require.NoError(t, err)
}
