package schemastore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/tidb/pkg/parser/model"
)

func TestCreateTable(t *testing.T) {
	version := uint64(100)
	store := newEmptyVersionedTableInfoStore()
	createDDLJob := &model.Job{
		Type:       model.ActionCreateTable,
		SchemaID:   10,
		SchemaName: "test",
		TableName:  "t",
		BinlogInfo: &model.HistoryInfo{
			TableInfo: &model.TableInfo{
				ID:   100,
				Name: model.NewCIStr("t"),
			},
			FinishedTS: version,
		},
	}
	store.applyDDL(createDDLJob)
	require.Equal(t, len(store.infos), 1)
	require.Equal(t, store.infos[0].version, Timestamp(version))
}

func TestRenameTable(t *testing.T) {
	version := uint64(100)
	store := newEmptyVersionedTableInfoStore()
	createDDLJob := &model.Job{
		Type:       model.ActionCreateTable,
		SchemaID:   10,
		SchemaName: "test",
		TableName:  "t",
		BinlogInfo: &model.HistoryInfo{
			TableInfo: &model.TableInfo{
				ID:   100,
				Name: model.NewCIStr("t"),
			},
			FinishedTS: version,
		},
	}
	renameDDLJob := &model.Job{
		Type:       model.ActionRenameTable,
		SchemaID:   10,
		SchemaName: "test",
		TableName:  "t2",
		BinlogInfo: &model.HistoryInfo{
			TableInfo: &model.TableInfo{
				ID:   100,
				Name: model.NewCIStr("t2"),
			},
			FinishedTS: version + 1,
		},
	}
	store.applyDDL(createDDLJob)
	store.applyDDL(renameDDLJob)
	require.Equal(t, len(store.infos), 2)
	require.Equal(t, store.infos[0].version, Timestamp(version))
	require.Equal(t, store.infos[1].version, Timestamp(version+1))
}

func TestDropTable(t *testing.T) {
	version := uint64(100)
	store := newEmptyVersionedTableInfoStore()
	createDDLJob := &model.Job{
		Type:       model.ActionCreateTable,
		SchemaID:   10,
		SchemaName: "test",
		TableName:  "t",
		BinlogInfo: &model.HistoryInfo{
			TableInfo: &model.TableInfo{
				ID:   100,
				Name: model.NewCIStr("t"),
			},
			FinishedTS: version,
		},
	}
	dropDDLJob := &model.Job{
		Type:       model.ActionDropTable,
		SchemaID:   10,
		SchemaName: "test",
		TableName:  "t",
		BinlogInfo: &model.HistoryInfo{
			TableInfo: &model.TableInfo{
				ID:   100,
				Name: model.NewCIStr("t"),
			},
			FinishedTS: version + 10,
		},
	}
	store.applyDDL(createDDLJob)
	store.applyDDL(dropDDLJob)
	require.Equal(t, len(store.infos), 1)
	require.Equal(t, store.infos[0].version, Timestamp(version))
	require.True(t, store.isDeleted())
}
