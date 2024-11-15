package util

import (
	"testing"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

func TestTableSchemaStore(t *testing.T) {
	tableSchemaStore := NewTableSchemaStore()

	// add table event
	event1 := &commonEvent.DDLEvent{
		FinishedTs: 3,
		TableNameChange: &commonEvent.TableNameChange{
			AddName: []commonEvent.SchemaTableName{
				{
					SchemaName: "test1",
					TableName:  "table1",
				},
				{
					SchemaName: "test2",
					TableName:  "table2",
				},
			},
		},
		NeedAddedTables: []commonEvent.Table{
			{
				SchemaID: 1,
				TableID:  1,
			},
			{
				SchemaID: 2,
				TableID:  2,
			},
		},
	}

	tableSchemaStore.AddEvent(event1)

	tableIds := tableSchemaStore.GetAllTableIds()
	require.Equal(t, 3, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(1)
	require.Equal(t, 2, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(2)
	require.Equal(t, 2, len(tableIds))

	tableNames := tableSchemaStore.GetAllTableNames(2)
	require.Equal(t, 0, len(tableNames))
	tableNames = tableSchemaStore.GetAllTableNames(3)
	require.Equal(t, 2, len(tableNames))
	tableNames = tableSchemaStore.GetAllTableNames(3)
	require.Equal(t, 2, len(tableNames))

	// drop databases
	event2 := &commonEvent.DDLEvent{
		FinishedTs: 5,
		TableNameChange: &commonEvent.TableNameChange{
			DropDatabaseName: "test1",
		},
		NeedDroppedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeDB,
			SchemaID:      1,
		},
	}
	tableSchemaStore.AddEvent(event2)
	tableIds = tableSchemaStore.GetAllTableIds()
	require.Equal(t, 2, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(1)
	require.Equal(t, 1, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(2)
	require.Equal(t, 2, len(tableIds))

	tableNames = tableSchemaStore.GetAllTableNames(5)
	require.Equal(t, 1, len(tableNames))

	// rename
	event3 := &commonEvent.DDLEvent{
		FinishedTs: 7,
		TableNameChange: &commonEvent.TableNameChange{
			AddName: []commonEvent.SchemaTableName{
				{
					SchemaName: "test3",
					TableName:  "table3",
				},
			},
			DropName: []commonEvent.SchemaTableName{
				{
					SchemaName: "test2",
					TableName:  "table2",
				},
			},
		},
		UpdatedSchemas: []commonEvent.SchemaIDChange{
			{
				TableID:     2,
				OldSchemaID: 2,
				NewSchemaID: 3,
			},
		},
	}
	tableSchemaStore.AddEvent(event3)
	tableIds = tableSchemaStore.GetAllTableIds()
	require.Equal(t, 2, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(2)
	require.Equal(t, 1, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(3)
	require.Equal(t, 2, len(tableIds))

	tableNames = tableSchemaStore.GetAllTableNames(5)
	require.Equal(t, 1, len(tableNames))

}
