package writer

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

// func TestPrepareDMLs(t *testing.T) {
// 	// Mock db
// 	db, _ := newTestMockDB(t)
// 	defer db.Close()

// 	cfg := &MysqlConfig{}
// 	writer := NewMysqlWriter(db, cfg, model.ChangeFeedID4Test("test", "test"))

// }

// func TestMysqlWriter_Flush(t *testing.T) {
// 	db, mock := newTestMockDB(t)
// 	defer db.Close()

// 	cfg := &MysqlConfig{
// 		SafeMode: false,
// 	}

// 	writer := NewMysqlWriter(db, cfg, model.ChangeFeedID4Test("test", "test"))

// 	events := []*common.dml{
// 		{
// 			StartTs:  1,
// 			CommitTs: 2,
// 			Rows: []*common.RowChangedEvent{
// 				{
// 					TableInfo: &common.TableInfo{
// 						TableName: common.TableName{
// 							Schema: "test_schema",
// 							Table:  "test_table",
// 						},
// 					},
// 					Columns: []*common.Column{
// 						{Name: "id", Value: 1, Flag: common.HandleKeyFlag | common.PrimaryKeyFlag},
// 						{Name: "name", Value: "test"},
// 					},
// 				},
// 			},
// 		},
// 	}

// 	mock.ExpectBegin()
// 	mock.ExpectExec("INSERT INTO `test_schema`.`test_table` (`id`,`name`) VALUES (?,?)").
// 		WithArgs(1, "test").
// 		WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectCommit()

// 	err := writer.Flush(events, 0, 1)
// 	require.NoError(t, err)

// 	err = mock.ExpectationsWereMet()
// 	require.NoError(t, err)
// }

// func TestMysqlWriter_Flush_EmptyEvents(t *testing.T) {
// 	db, mock := newTestMockDB(t)
// 	defer db.Close()

// 	cfg := &MysqlConfig{
// 		SafeMode: false,
// 	}

// 	writer := NewMysqlWriter(db, cfg, model.ChangeFeedID4Test("test", "test"))

// 	events := []*common.TxnEvent{}

// 	err := writer.Flush(events, 0, 0)
// 	require.NoError(t, err)

// 	err = mock.ExpectationsWereMet()
// 	require.NoError(t, err)
// }

// func TestMysqlWriter_FlushDDLEvent(t *testing.T) {
// 	db, mock := newTestMockDB(t)
// 	defer db.Close()

// 	cfg := &MysqlConfig{}

// 	writer := NewMysqlWriter(db, cfg, model.ChangeFeedID4Test("test", "test"))

// 	event := common.TxnEvent{DDLEvent: &common.DDLEvent{
// 		Job: &timodel.Job{
// 			SchemaName: "test",
// 			TableName:  "table1",
// 			Query:      "CREATE TABLE table1 (id INT PRIMARY KEY, name VARCHAR(255))",
// 		},
// 		CommitTS: 10,
// 	}}

// 	mock.ExpectBegin()
// 	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectExec("CREATE TABLE table1 (id INT PRIMARY KEY, name VARCHAR(255))").WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectCommit()

// 	err := writer.FlushDDLEvent(&event)
// 	require.NoError(t, err)

// 	err = mock.ExpectationsWereMet()
// 	require.NoError(t, err)
// }
