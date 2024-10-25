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

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestCreateTruncateAndDropTable(t *testing.T) {
	tableID1 := int64(100)
	store1 := newEmptyVersionedTableInfoStore(tableID1)
	store1.setTableInfoInitialized()
	createVersion := uint64(300)
	{
		createDDLEvent := &PersistedDDLEvent{
			Type:              byte(model.ActionCreateTable),
			CurrentSchemaID:   10,
			CurrentTableID:    tableID1,
			CurrentSchemaName: "test",
			CurrentTableName:  "t",
			TableInfo: &model.TableInfo{
				ID:   tableID1,
				Name: model.NewCIStr("t"),
			},
			FinishedTs: createVersion,
		}
		store1.applyDDL(createDDLEvent)
	}

	tableID2 := tableID1 + 1
	store2 := newEmptyVersionedTableInfoStore(tableID2)
	store2.setTableInfoInitialized()
	truncateVersion := createVersion + 10
	{
		truncateDDLEvent := &PersistedDDLEvent{
			Type:              byte(model.ActionTruncateTable),
			CurrentSchemaID:   10,
			CurrentTableID:    tableID2,
			CurrentSchemaName: "test",
			CurrentTableName:  "t",
			PrevTableID:       tableID1,
			TableInfo: &model.TableInfo{
				ID:   tableID1,
				Name: model.NewCIStr("t"),
			},
			FinishedTs: truncateVersion,
		}
		store1.applyDDL(truncateDDLEvent)
		store2.applyDDL(truncateDDLEvent)
	}

	dropVersion := truncateVersion + 10
	{
		dropDDLEvent := &PersistedDDLEvent{
			Type:              byte(model.ActionDropTable),
			CurrentSchemaID:   10,
			CurrentTableID:    tableID2,
			CurrentSchemaName: "test",
			CurrentTableName:  "t",
			TableInfo: &model.TableInfo{
				ID:   tableID2,
				Name: model.NewCIStr("t"),
			},
			FinishedTs: dropVersion,
		}
		store2.applyDDL(dropDDLEvent)
	}

	{
		require.Equal(t, len(store1.infos), 1)
		tableInfo, err := store1.getTableInfo(createVersion)
		require.Nil(t, err)
		require.Equal(t, "t", tableInfo.Name.O)
		require.Equal(t, truncateVersion, store1.deleteVersion)
	}

	{
		require.Equal(t, len(store2.infos), 1)
		tableInfo, err := store2.getTableInfo(truncateVersion)
		require.Nil(t, err)
		require.Equal(t, "t", tableInfo.Name.O)
		require.Equal(t, dropVersion, store2.deleteVersion)
	}
}

func TestRenameTable(t *testing.T) {
	tableID := int64(100)
	store := newEmptyVersionedTableInfoStore(tableID)
	store.setTableInfoInitialized()

	createVersion := uint64(100)
	schemaID1 := int64(10)
	{
		createDDLEvent := &PersistedDDLEvent{
			Type:              byte(model.ActionCreateTable),
			CurrentSchemaID:   10,
			CurrentTableID:    tableID,
			CurrentSchemaName: "test",
			CurrentTableName:  "t",
			TableInfo: &model.TableInfo{
				ID:   tableID,
				Name: model.NewCIStr("t"),
			},
			FinishedTs: createVersion,
		}
		store.applyDDL(createDDLEvent)
	}

	renameVersion := createVersion + 10
	schemaID2 := schemaID1 + 100
	{
		renameDDLEvent := &PersistedDDLEvent{
			Type:              byte(model.ActionRenameTable),
			CurrentSchemaID:   schemaID2,
			CurrentTableID:    tableID,
			CurrentSchemaName: "test2",
			CurrentTableName:  "t2",
			PrevSchemaID:      schemaID1,
			PrevSchemaName:    "test",
			PrevTableName:     "t",
			TableInfo: &model.TableInfo{
				ID:   tableID,
				Name: model.NewCIStr("t2"),
			},
			FinishedTs: renameVersion,
		}
		store.applyDDL(renameDDLEvent)
	}

	{
		require.Equal(t, len(store.infos), 2)
		tableInfo, err := store.getTableInfo(createVersion)
		require.Nil(t, err)
		require.Equal(t, tableInfo.Name.O, "t")
		require.Equal(t, tableInfo.SchemaID, schemaID1)
		tableInfo, err = store.getTableInfo(renameVersion)
		require.Nil(t, err)
		require.Equal(t, tableInfo.Name.O, "t2")
		require.Equal(t, tableInfo.SchemaID, schemaID2)
	}
}

func TestGCMultiVersionTableInfo(t *testing.T) {
	tableID := int64(100)
	store := newEmptyVersionedTableInfoStore(tableID)
	store.setTableInfoInitialized()

	store.infos = append(store.infos, &tableInfoItem{version: 100, info: &common.TableInfo{}})
	store.infos = append(store.infos, &tableInfoItem{version: 200, info: &common.TableInfo{}})
	store.infos = append(store.infos, &tableInfoItem{version: 300, info: &common.TableInfo{}})
	store.deleteVersion = 1000

	require.False(t, store.gc(200))
	require.Equal(t, 2, len(store.infos))
	require.False(t, store.gc(300))
	require.Equal(t, 1, len(store.infos))
	require.False(t, store.gc(500))
	require.Equal(t, 1, len(store.infos))
	require.True(t, store.gc(1000))
}
