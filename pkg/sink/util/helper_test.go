package util

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

func TestTableSchemaStoreWhenMysqlSink(t *testing.T) {
	schemaInfos := make([]*heartbeatpb.SchemaInfo, 0)
	schemaInfos = append(schemaInfos, &heartbeatpb.SchemaInfo{
		SchemaID: 1,
		Tables: []*heartbeatpb.TableInfo{
			{
				TableID: 1,
			},
			{
				TableID: 2,
			},
		},
	})
	schemaInfos = append(schemaInfos, &heartbeatpb.SchemaInfo{
		SchemaID: 2,
		Tables: []*heartbeatpb.TableInfo{
			{
				TableID: 3,
			},
			{
				TableID: 4,
			},
		},
	})

	tableSchemaStore := NewTableSchemaStore(schemaInfos, common.MysqlSinkType)
	tableIds := tableSchemaStore.GetAllTableIds()
	require.Equal(t, 5, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(1)
	require.Equal(t, 3, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(2)
	require.Equal(t, 3, len(tableIds))

	// add table event
	event1 := &commonEvent.DDLEvent{
		FinishedTs: 3,
		NeedAddedTables: []commonEvent.Table{
			{
				SchemaID: 1,
				TableID:  5,
			},
			{
				SchemaID: 2,
				TableID:  6,
			},
		},
	}

	tableSchemaStore.AddEvent(event1)

	tableIds = tableSchemaStore.GetAllTableIds()
	require.Equal(t, 7, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(1)
	require.Equal(t, 4, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(2)
	require.Equal(t, 4, len(tableIds))

	// drop databases
	event2 := &commonEvent.DDLEvent{
		FinishedTs: 5,
		NeedDroppedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeDB,
			SchemaID:      1,
		},
	}
	tableSchemaStore.AddEvent(event2)
	tableIds = tableSchemaStore.GetAllTableIds()
	require.Equal(t, 4, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(1)
	require.Equal(t, 1, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(2)
	require.Equal(t, 4, len(tableIds))

	// rename
	event3 := &commonEvent.DDLEvent{
		FinishedTs: 7,
		UpdatedSchemas: []commonEvent.SchemaIDChange{
			{
				TableID:     6,
				OldSchemaID: 2,
				NewSchemaID: 3,
			},
		},
	}
	tableSchemaStore.AddEvent(event3)
	tableIds = tableSchemaStore.GetAllTableIds()
	require.Equal(t, 4, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(2)
	require.Equal(t, 3, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(3)
	require.Equal(t, 2, len(tableIds))
}

func TestTableSchemaStoreWhenNonMysqlSink(t *testing.T) {
	schemaInfos := make([]*heartbeatpb.SchemaInfo, 0)
	schemaInfos = append(schemaInfos, &heartbeatpb.SchemaInfo{
		SchemaName: "test1",
		Tables: []*heartbeatpb.TableInfo{
			{
				TableName: "table1",
			},
			{
				TableName: "table2",
			},
		},
	})
	schemaInfos = append(schemaInfos, &heartbeatpb.SchemaInfo{
		SchemaName: "test2",
		Tables: []*heartbeatpb.TableInfo{
			{
				TableName: "table3",
			},
			{
				TableName: "table4",
			},
		},
	})

	tableSchemaStore := NewTableSchemaStore(schemaInfos, common.KafkaSinkType)
	tableNames := tableSchemaStore.GetAllTableNames(1)
	require.Equal(t, 4, len(tableNames))

	// add table event
	event1 := &commonEvent.DDLEvent{
		FinishedTs: 3,
		TableNameChange: &commonEvent.TableNameChange{
			AddName: []commonEvent.SchemaTableName{
				{
					SchemaName: "test1",
					TableName:  "table5",
				},
				{
					SchemaName: "test2",
					TableName:  "table6",
				},
			},
		},
	}

	tableSchemaStore.AddEvent(event1)

	tableNames = tableSchemaStore.GetAllTableNames(2)
	require.Equal(t, 4, len(tableNames))
	tableNames = tableSchemaStore.GetAllTableNames(3)
	require.Equal(t, 6, len(tableNames))

	// drop databases
	event2 := &commonEvent.DDLEvent{
		FinishedTs: 5,
		TableNameChange: &commonEvent.TableNameChange{
			DropDatabaseName: "test1",
		},
	}
	tableSchemaStore.AddEvent(event2)

	tableNames = tableSchemaStore.GetAllTableNames(5)
	require.Equal(t, 3, len(tableNames))

	// rename
	event3 := &commonEvent.DDLEvent{
		FinishedTs: 7,
		TableNameChange: &commonEvent.TableNameChange{
			AddName: []commonEvent.SchemaTableName{
				{
					SchemaName: "test3",
					TableName:  "table7",
				},
			},
			DropName: []commonEvent.SchemaTableName{
				{
					SchemaName: "test2",
					TableName:  "table6",
				},
			},
		},
	}
	tableSchemaStore.AddEvent(event3)
	tableNames = tableSchemaStore.GetAllTableNames(7)
	require.Equal(t, 3, len(tableNames))

}
