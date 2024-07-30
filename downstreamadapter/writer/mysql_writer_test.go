package writer

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/assert"

	"github.com/flowbehappy/tigate/pkg/common"
)

func newTestMockDB(t *testing.T) (db *sql.DB, mock sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.Nil(t, err)
	return
}

func TestPrepareDMLs(t *testing.T) {
	// Mock db
	db, _ := newTestMockDB(t)
	defer db.Close()

	cfg := &MysqlConfig{}
	writer := NewMysqlWriter(db, cfg, model.ChangeFeedID4Test("test", "test"))

	tests := []struct {
		name     string
		events   []*common.TxnEvent
		expected *preparedDMLs
	}{
		{
			name: "Single Insert Event",
			events: []*common.TxnEvent{
				{
					StartTs: 1,
					Rows: []*common.RowChangedEvent{
						{
							TableInfo: &common.TableInfo{
								TableName: common.TableName{
									Schema: "test",
									Table:  "users",
								},
							},
							Columns: []*common.Column{
								{Name: "id", Value: 1},
								{Name: "name", Value: "Alice"},
							},
						},
					},
				},
			},
			expected: &preparedDMLs{
				startTs:  []uint64{1},
				sqls:     []string{"REPLACE INTO `test`.`users` (`id`,`name`) VALUES (?,?)"},
				values:   [][]interface{}{{1, "Alice"}},
				rowCount: 1,
			},
		},
		{
			name: "Multiple Events",
			events: []*common.TxnEvent{
				{
					StartTs: 1,
					Rows: []*common.RowChangedEvent{
						{
							TableInfo: &common.TableInfo{
								TableName: common.TableName{
									Schema: "test",
									Table:  "users",
								},
							},
							Columns: []*common.Column{
								{Name: "id", Value: 1},
								{Name: "name", Value: "Alice"},
							},
						},
					},
				},
				{
					StartTs: 2,
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
								{Name: "id", Value: 1},
								{Name: "name", Value: "Bob"},
							},
						},
					},
				},
			},
			expected: &preparedDMLs{
				startTs: []uint64{1, 2},
				sqls: []string{
					"REPLACE INTO `test`.`users` (`id`,`name`) VALUES (?,?)",
					"UPDATE `test`.`users` SET `id` = ?, `name` = ? WHERE `id` = ? LIMIT 1",
				},
				values: [][]interface{}{
					{1, "Alice"},
					{1, "Bob", 1},
				},
				rowCount: 2,
			},
		},
		{
			name: "Empty Event",
			events: []*common.TxnEvent{
				{
					StartTs: 1,
					Rows:    []*common.RowChangedEvent{},
				},
			},
			expected: &preparedDMLs{
				startTs:  []uint64{},
				sqls:     []string{},
				values:   [][]interface{}{},
				rowCount: 0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := writer.prepareDMLs(tt.events)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMysqlWriter_Flush(t *testing.T) {
	db, mock := newTestMockDB(t)
	defer db.Close()

	cfg := &MysqlConfig{
		SafeMode: false,
	}

	writer := NewMysqlWriter(db, cfg, model.ChangeFeedID4Test("test", "test"))

	events := []*common.TxnEvent{
		{
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
						{Name: "name", Value: "test"},
					},
				},
			},
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `test_schema`.`test_table` (`id`,`name`) VALUES (?,?)").
		WithArgs(1, "test").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := writer.Flush(events, 0)
	require.NoError(t, err)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestMysqlWriter_Flush_EmptyEvents(t *testing.T) {
	db, mock := newTestMockDB(t)
	defer db.Close()

	cfg := &MysqlConfig{
		SafeMode: false,
	}

	writer := NewMysqlWriter(db, cfg, model.ChangeFeedID4Test("test", "test"))

	events := []*common.TxnEvent{}

	err := writer.Flush(events, 0)
	require.NoError(t, err)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

/*
TODO:
func TestWithMysqlCluster(t *testing.T) {
	_, db, err := NewMysqlConfigAndDB("tidb://root:@127.0.0.1:4000")
	require.NoError(t, err)
	rows, err := db.Query("SELECT * from d1.t1")
	for rows.Next() {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		require.NoError(t, err)
		log.Info("id, name", zap.Any("id", id), zap.Any("name", name))
	}
}*/
