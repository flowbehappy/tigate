package writer

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/assert"

	"github.com/flowbehappy/tigate/common"
)

func newTestMockDB(t *testing.T) (db *sql.DB, mock sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	// mock.ExpectQuery("select tidb_version()").WillReturnError(&dmysql.MySQLError{
	// 	Number:  1305,
	// 	Message: "FUNCTION test.tidb_version does not exist",
	// })
	// // mock a different possible error for the second query
	// mock.ExpectQuery("select tidb_version()").WillReturnError(&dmysql.MySQLError{
	// 	Number:  1044,
	// 	Message: "Access denied for user 'cdc'@'%' to database 'information_schema'",
	// })
	require.Nil(t, err)
	return
}

func TestPrepareDMLs(t *testing.T) {
	// Mock data
	db, _ := newTestMockDB(t)

	cfg := &MysqlConfig{}
	writer := NewMysqlWriter(db, cfg)

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
				sqls:     []string{"REPLACE INTO `test`.`users` (`id`, `name`) VALUES (?, ?)"},
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
								{Name: "id", Value: 1},
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
					"REPLACE INTO `test`.`users` (`id`, `name`) VALUES (?, ?)",
					"UPDATE `test`.`users` SET `name` = ? WHERE `id` = ?",
				},
				values: [][]interface{}{
					{1, "Alice"},
					{"Bob", 1},
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
				startTs:  []uint64{1},
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
