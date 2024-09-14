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
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func writeMetaData(db *pebble.DB, gcTS common.Ts, metaTS *schemaMetaTS) error {
	batch := db.NewBatch()
	writeTSToBatch(batch, gcTSKey(), gcTS)
	writeTSToBatch(batch, metaTSKey(), metaTS.ResolvedTS, metaTS.FinishedDDLTS, metaTS.SchemaVersion)
	return batch.Commit(pebble.Sync)
}

func writeSchemaSnapshot(db *pebble.DB, snapTS common.Ts, dbInfo *model.DBInfo) error {
	batch := db.NewBatch()
	schemaKey, err := snapshotSchemaKey(snapTS, dbInfo.ID)
	if err != nil {
		log.Fatal("generate schema key failed", zap.Error(err))
	}
	schemaValue, err := json.Marshal(dbInfo)
	if err != nil {
		log.Fatal("marshal schema failed", zap.Error(err))
	}
	batch.Set(schemaKey, schemaValue, pebble.NoSync)
	return batch.Commit(pebble.Sync)
}

func writeTableSnapshot(db *pebble.DB, snapTS common.Ts, schemaID int64, tableInfo *model.TableInfo) error {
	batch := db.NewBatch()
	tableKey, err := snapshotTableKey(snapTS, tableInfo.ID)
	if err != nil {
		log.Fatal("generate table key failed", zap.Error(err))
	}
	tableValue, err := json.Marshal(tableInfo)
	if err != nil {
		log.Fatal("marshal table failed", zap.Error(err))
	}
	batch.Set(tableKey, tableValue, pebble.NoSync)
	indexKey, err := indexSnapshotKey(tableInfo.ID, snapTS, schemaID)
	if err != nil {
		log.Fatal("generate index key failed", zap.Error(err))
	}
	batch.Set(indexKey, nil, pebble.NoSync)
	return batch.Commit(pebble.Sync)
}

func TestLoadEmptyPersistentStorage(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.Nil(t, err)
	defer db.Close()

	gcTS := common.Ts(1000)
	metaTS := &schemaMetaTS{
		ResolvedTS:    1000,
		FinishedDDLTS: 3000,
		SchemaVersion: 4000,
	}

	err = writeMetaData(db, gcTS, metaTS)
	require.Nil(t, err)

	dataStorage, newMetaTS, databaseMap := loadPersistentStorage(db, 1000)
	require.Equal(t, metaTS.ResolvedTS, newMetaTS.ResolvedTS)
	require.Equal(t, metaTS.FinishedDDLTS, newMetaTS.FinishedDDLTS)
	require.Equal(t, metaTS.SchemaVersion, newMetaTS.SchemaVersion)
	require.Equal(t, 0, len(databaseMap))
	require.Equal(t, gcTS, dataStorage.getGCTS())
}

func TestLoadPersistentStorageWithoutRequiredData(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.Nil(t, err)
	defer db.Close()

	gcTS := common.Ts(1000)
	metaTS := &schemaMetaTS{
		ResolvedTS:    1000,
		FinishedDDLTS: 3000,
		SchemaVersion: 4000,
	}

	err = writeMetaData(db, gcTS, metaTS)
	require.Nil(t, err)

	dataStorage, _, databaseMap := loadPersistentStorage(db, metaTS.ResolvedTS+100)
	require.Nil(t, dataStorage)
	require.Nil(t, databaseMap)
}

func TestBuildVersionedTableInfoStore(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.Nil(t, err)
	defer db.Close()

	gcTS := common.Ts(1000)
	metaTS := &schemaMetaTS{
		ResolvedTS:    2000,
		FinishedDDLTS: 3000,
		SchemaVersion: 4000,
	}

	err = writeMetaData(db, gcTS, metaTS)
	require.Nil(t, err)

	schemaID := 50
	tableID := common.TableID(99)
	writeSchemaSnapshot(db, gcTS, &model.DBInfo{
		ID:   int64(schemaID),
		Name: model.NewCIStr("test"),
	})
	writeTableSnapshot(db, gcTS, int64(schemaID), &model.TableInfo{
		ID:   tableID,
		Name: model.NewCIStr("t"),
	})

	dataStorage, _, databaseMap := loadPersistentStorage(db, 1500)
	require.Equal(t, 1, len(databaseMap))
	require.Equal(t, "test", databaseMap[int64(schemaID)].Name)

	{
		store := newEmptyVersionedTableInfoStore(tableID)
		getSchemaName := func(schemaID int64) (string, error) {
			if _, ok := databaseMap[schemaID]; !ok {
				return "", fmt.Errorf("schema not found")
			}
			return databaseMap[schemaID].Name, nil
		}
		dataStorage.buildVersionedTableInfoStore(store, gcTS, metaTS.ResolvedTS, getSchemaName)
		store.setTableInfoInitialized()
		require.Equal(t, gcTS, store.getFirstVersion())
		tableInfo, err := store.getTableInfo(gcTS)
		require.Nil(t, err)
		require.Equal(t, "t", tableInfo.Name.O)
		require.Equal(t, tableID, tableInfo.ID)
	}

	// mock write some ddl event and load again
	renameVersion := uint64(1500)
	err = dataStorage.writeDDLEvent(DDLEvent{
		Job: &model.Job{
			Type:     model.ActionRenameTable,
			SchemaID: int64(schemaID),
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 3000,
				TableInfo: &model.TableInfo{
					ID:   tableID,
					Name: model.NewCIStr("t2"),
				},
				FinishedTS: renameVersion,
			},
			TableID: tableID,
		},
	})
	require.Nil(t, err)

	dataStorage, _, _ = loadPersistentStorage(db, 1500)
	{
		store := newEmptyVersionedTableInfoStore(tableID)
		getSchemaName := func(schemaID int64) (string, error) {
			if _, ok := databaseMap[schemaID]; !ok {
				return "", fmt.Errorf("schema not found")
			}
			return databaseMap[schemaID].Name, nil
		}
		dataStorage.buildVersionedTableInfoStore(store, gcTS, metaTS.ResolvedTS, getSchemaName)
		store.setTableInfoInitialized()
		require.Equal(t, gcTS, store.getFirstVersion())
		require.Equal(t, 2, len(store.infos))
		tableInfo, err := store.getTableInfo(gcTS)
		require.Nil(t, err)
		require.Equal(t, "t", tableInfo.Name.O)
		require.Equal(t, tableID, tableInfo.ID)
		tableInfo2, err := store.getTableInfo(renameVersion)
		require.Nil(t, err)
		require.Equal(t, "t2", tableInfo2.Name.O)
	}
}

func TestBuildVersionedTableInfoAndApplyDDL(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.Nil(t, err)
	defer db.Close()

	gcTS := common.Ts(1000)
	metaTS := &schemaMetaTS{
		ResolvedTS:    2000,
		FinishedDDLTS: 3000,
		SchemaVersion: 4000,
	}

	err = writeMetaData(db, gcTS, metaTS)
	require.Nil(t, err)

	schemaID := 50
	tableID := common.TableID(99)
	writeSchemaSnapshot(db, gcTS, &model.DBInfo{
		ID:   int64(schemaID),
		Name: model.NewCIStr("test"),
	})
	writeTableSnapshot(db, gcTS, int64(schemaID), &model.TableInfo{
		ID:   tableID,
		Name: model.NewCIStr("t"),
	})

	dataStorage, _, databaseMap := loadPersistentStorage(db, 1500)
	require.Equal(t, 1, len(databaseMap))
	require.Equal(t, "test", databaseMap[int64(schemaID)].Name)

	store := newEmptyVersionedTableInfoStore(tableID)
	getSchemaName := func(schemaID int64) (string, error) {
		if _, ok := databaseMap[schemaID]; !ok {
			return "", fmt.Errorf("schema not found")
		}
		return databaseMap[schemaID].Name, nil
	}
	dataStorage.buildVersionedTableInfoStore(store, gcTS, metaTS.ResolvedTS, getSchemaName)
	renameVersion := uint64(1500)
	store.applyDDL(&model.Job{
		Type:     model.ActionRenameTable,
		SchemaID: int64(schemaID),
		BinlogInfo: &model.HistoryInfo{
			SchemaVersion: 3000,
			TableInfo: &model.TableInfo{
				ID:   tableID,
				Name: model.NewCIStr("t2"),
			},
			FinishedTS: renameVersion,
		},
		TableID: tableID,
	})
	store.setTableInfoInitialized()
	require.Equal(t, gcTS, store.getFirstVersion())
	require.Equal(t, 2, len(store.infos))
	tableInfo, err := store.getTableInfo(gcTS)
	require.Nil(t, err)
	require.Equal(t, "t", tableInfo.Name.O)
	require.Equal(t, tableID, tableInfo.ID)
	tableInfo2, err := store.getTableInfo(renameVersion)
	require.Nil(t, err)
	require.Equal(t, "t2", tableInfo2.Name.O)
}
