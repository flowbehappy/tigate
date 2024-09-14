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

package schemastore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/tidb/pkg/parser/model"
)

func TestCreateTable(t *testing.T) {
	version := uint64(100)
	store := newEmptyVersionedTableInfoStore(100)
	store.setTableInfoInitialized()
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
	require.Equal(t, store.getFirstVersion(), version)
}

func TestRenameTable(t *testing.T) {
	version := uint64(100)
	store := newEmptyVersionedTableInfoStore(100)
	store.setTableInfoInitialized()
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
	renameDDLJob2 := &model.Job{
		Type:       model.ActionRenameTable,
		SchemaID:   10,
		SchemaName: "test",
		TableName:  "t3",
		BinlogInfo: &model.HistoryInfo{
			TableInfo: &model.TableInfo{
				ID:   100,
				Name: model.NewCIStr("t3"),
			},
			FinishedTS: version + 10,
		},
	}
	store.applyDDL(createDDLJob)
	store.applyDDL(renameDDLJob)
	store.applyDDL(renameDDLJob2)
	require.Equal(t, len(store.infos), 3)
	require.Equal(t, store.getFirstVersion(), version)
	tableInfo, err := store.getTableInfo(version)
	require.Nil(t, err)
	require.Equal(t, tableInfo.Name.O, "t")
	tableInfo, err = store.getTableInfo(version + 1)
	require.Nil(t, err)
	require.Equal(t, tableInfo.Name.O, "t2")
	tableInfo, err = store.getTableInfo(version + 2)
	require.Nil(t, err)
	require.Equal(t, tableInfo.Name.O, "t2")
	tableInfo, err = store.getTableInfo(version + 10)
	require.Nil(t, err)
	require.Equal(t, tableInfo.Name.O, "t3")
}

func TestDropTable(t *testing.T) {
	version := uint64(100)
	store := newEmptyVersionedTableInfoStore(100)
	store.setTableInfoInitialized()
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
	require.Equal(t, store.getFirstVersion(), version)
	tableInfo, err := store.getTableInfo(version)
	require.Nil(t, err)
	require.Equal(t, tableInfo.Name.O, "t")
}

// TODO: test the case that nothing need to be copied
func TestCopyTail(t *testing.T) {
	version := uint64(100)
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
	renameDDLJobNum := 20
	renameDDLJobs := make([]*model.Job, 0)
	for i := 0; i < renameDDLJobNum; i++ {
		renameDDLJobs = append(renameDDLJobs, &model.Job{
			Type:       model.ActionRenameTable,
			SchemaID:   10,
			SchemaName: "test",
			TableName:  fmt.Sprintf("test%d", i),
			BinlogInfo: &model.HistoryInfo{
				TableInfo: &model.TableInfo{
					ID:   100,
					Name: model.NewCIStr(fmt.Sprintf("test%d", i)),
				},
				FinishedTS: version + uint64(i),
			},
		})
	}

	oldStore := newEmptyVersionedTableInfoStore(100)
	oldStore.setTableInfoInitialized()
	oldStore.applyDDL(createDDLJob)
	for _, job := range renameDDLJobs {
		oldStore.applyDDL(job)
	}
	// mock gc
	oldStore.infos = oldStore.infos[renameDDLJobNum/2:]

	newStore := newEmptyVersionedTableInfoStore(100)
	newStore.applyDDL(createDDLJob)
	for i := 0; i < renameDDLJobNum/2; i++ {
		newStore.applyDDL(renameDDLJobs[i])
	}
	require.Equal(t, len(newStore.infos), renameDDLJobNum/2+1)
	newStore.checkAndCopyTailFrom(oldStore)
	require.Equal(t, len(newStore.infos), renameDDLJobNum+1)
}

// TODO: test gc
// TODO: test register dispatcher and unregister dispatcher
