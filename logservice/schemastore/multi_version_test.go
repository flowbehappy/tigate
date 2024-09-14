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
	"testing"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestCreateTable(t *testing.T) {
	version := uint64(100)
	store := newEmptyVersionedTableInfoStore(100)
	store.setTableInfoInitialized()
	createDDLJob := PersistedDDLEvent{
		Type:       byte(model.ActionCreateTable),
		SchemaID:   10,
		SchemaName: "test",
		TableInfo: &model.TableInfo{
			ID:   100,
			Name: model.NewCIStr("t"),
		},
		FinishedTs: version,
	}
	store.applyDDL(createDDLJob)
	require.Equal(t, len(store.infos), 1)
}

func TestRenameTable(t *testing.T) {
	version := uint64(100)
	store := newEmptyVersionedTableInfoStore(100)
	store.setTableInfoInitialized()
	createDDLJob := PersistedDDLEvent{
		Type:       byte(model.ActionCreateTable),
		SchemaID:   10,
		SchemaName: "test",
		TableInfo: &model.TableInfo{
			ID:   100,
			Name: model.NewCIStr("t"),
		},
		FinishedTs: version,
	}
	renameDDLJob := PersistedDDLEvent{
		Type:       byte(model.ActionRenameTable),
		SchemaID:   10,
		SchemaName: "test",
		TableInfo: &model.TableInfo{
			ID:   100,
			Name: model.NewCIStr("t2"),
		},
		FinishedTs: version + 1,
	}
	renameDDLJob2 := PersistedDDLEvent{
		Type:       byte(model.ActionRenameTable),
		SchemaID:   10,
		SchemaName: "test",
		TableInfo: &model.TableInfo{
			ID:   100,
			Name: model.NewCIStr("t3"),
		},
		FinishedTs: version + 10,
	}
	store.applyDDL(createDDLJob)
	store.applyDDL(renameDDLJob)
	store.applyDDL(renameDDLJob2)
	require.Equal(t, len(store.infos), 3)
	tableInfo, err := store.getTableInfo(uint64(version))
	require.Nil(t, err)
	require.Equal(t, tableInfo.Name.O, "t")
	tableInfo, err = store.getTableInfo(uint64(version + 1))
	require.Nil(t, err)
	require.Equal(t, tableInfo.Name.O, "t2")
	tableInfo, err = store.getTableInfo(uint64(version + 2))
	require.Nil(t, err)
	require.Equal(t, tableInfo.Name.O, "t2")
	tableInfo, err = store.getTableInfo(uint64(version + 10))
	require.Nil(t, err)
	require.Equal(t, tableInfo.Name.O, "t3")
}

func TestDropTable(t *testing.T) {
	version := uint64(100)
	store := newEmptyVersionedTableInfoStore(100)
	store.setTableInfoInitialized()
	createDDLJob := PersistedDDLEvent{
		Type:       byte(model.ActionCreateTable),
		SchemaID:   10,
		SchemaName: "test",
		TableInfo: &model.TableInfo{
			ID:   100,
			Name: model.NewCIStr("t"),
		},
		FinishedTs: version,
	}
	dropDDLJob := PersistedDDLEvent{
		Type:       byte(model.ActionDropTable),
		SchemaID:   10,
		SchemaName: "test",
		TableInfo: &model.TableInfo{
			ID:   100,
			Name: model.NewCIStr("t"),
		},
		FinishedTs: version + 10,
	}
	store.applyDDL(createDDLJob)
	store.applyDDL(dropDDLJob)
	require.Equal(t, len(store.infos), 1)
	tableInfo, err := store.getTableInfo(uint64(version))
	require.Nil(t, err)
	require.Equal(t, tableInfo.Name.O, "t")
}

// TODO: test gc
// TODO: test register dispatcher and unregister dispatcher
