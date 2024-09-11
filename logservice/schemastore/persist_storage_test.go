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
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

// func writeMetaData(db *pebble.DB, gcTS common.Ts, metaTS *schemaMetaTS) error {
// 	batch := db.NewBatch()
// 	writeTSToBatch(batch, gcTSKey(), gcTS)
// 	writeTSToBatch(batch, metaTSKey(), metaTS.ResolvedTS, metaTS.FinishedDDLTS, metaTS.SchemaVersion)
// 	return batch.Commit(pebble.Sync)
// }

// func writeSchemaSnapshot(db *pebble.DB, snapTS common.Ts, dbInfo *model.DBInfo) error {
// 	batch := db.NewBatch()
// 	schemaKey, err := snapshotSchemaKey(snapTS, int64(dbInfo.ID))
// 	if err != nil {
// 		log.Fatal("generate schema key failed", zap.Error(err))
// 	}
// 	schemaValue, err := json.Marshal(dbInfo)
// 	if err != nil {
// 		log.Fatal("marshal schema failed", zap.Error(err))
// 	}
// 	batch.Set(schemaKey, schemaValue, pebble.NoSync)
// 	return batch.Commit(pebble.Sync)
// }

// func writeTableSnapshot(db *pebble.DB, snapTS common.Ts, schemaID int64, tableInfo *model.TableInfo) error {
// 	batch := db.NewBatch()
// 	tableKey, err := snapshotTableKey(snapTS, common.TableID(tableInfo.ID))
// 	if err != nil {
// 		log.Fatal("generate table key failed", zap.Error(err))
// 	}
// 	tableValue, err := json.Marshal(tableInfo)
// 	if err != nil {
// 		log.Fatal("marshal table failed", zap.Error(err))
// 	}
// 	batch.Set(tableKey, tableValue, pebble.NoSync)
// 	indexKey, err := indexSnapshotKey(common.TableID(tableInfo.ID), snapTS, int64(schemaID))
// 	if err != nil {
// 		log.Fatal("generate index key failed", zap.Error(err))
// 	}
// 	batch.Set(indexKey, nil, pebble.NoSync)
// 	return batch.Commit(pebble.Sync)
// }

// func TestLoadEmptyPersistentStorage(t *testing.T) {
// 	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
// 	err := os.RemoveAll(dbPath)
// 	require.Nil(t, err)
// 	db, err := pebble.Open(dbPath, &pebble.Options{})
// 	require.Nil(t, err)
// 	defer db.Close()

// 	gcTS := common.Ts(1000)
// 	metaTS := &schemaMetaTS{
// 		ResolvedTS:    1000,
// 		FinishedDDLTS: 3000,
// 		SchemaVersion: 4000,
// 	}

// 	err = writeMetaData(db, gcTS, metaTS)
// 	require.Nil(t, err)

// 	dataStorage, newMetaTS, databaseMap := loadPersistentStorage(db, 1000)
// 	require.Equal(t, metaTS.ResolvedTS, newMetaTS.ResolvedTS)
// 	require.Equal(t, metaTS.FinishedDDLTS, newMetaTS.FinishedDDLTS)
// 	require.Equal(t, metaTS.SchemaVersion, newMetaTS.SchemaVersion)
// 	require.Equal(t, 0, len(databaseMap))
// 	require.Equal(t, gcTS, dataStorage.getGCTS())
// }

// func TestLoadPersistentStorageWithoutRequiredData(t *testing.T) {
// 	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
// 	err := os.RemoveAll(dbPath)
// 	require.Nil(t, err)
// 	db, err := pebble.Open(dbPath, &pebble.Options{})
// 	require.Nil(t, err)
// 	defer db.Close()

// 	gcTS := common.Ts(1000)
// 	metaTS := &schemaMetaTS{
// 		ResolvedTS:    1000,
// 		FinishedDDLTS: 3000,
// 		SchemaVersion: 4000,
// 	}

// 	err = writeMetaData(db, gcTS, metaTS)
// 	require.Nil(t, err)

// 	dataStorage, _, databaseMap := loadPersistentStorage(db, metaTS.ResolvedTS+100)
// 	require.Nil(t, dataStorage)
// 	require.Nil(t, databaseMap)
// }

// func TestBuildVersionedTableInfoStore(t *testing.T) {
// 	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
// 	err := os.RemoveAll(dbPath)
// 	require.Nil(t, err)
// 	db, err := pebble.Open(dbPath, &pebble.Options{})
// 	require.Nil(t, err)
// 	defer db.Close()

// 	gcTS := common.Ts(1000)
// 	metaTS := &schemaMetaTS{
// 		ResolvedTS:    2000,
// 		FinishedDDLTS: 3000,
// 		SchemaVersion: 4000,
// 	}

// 	err = writeMetaData(db, gcTS, metaTS)
// 	require.Nil(t, err)

// 	schemaID := 50
// 	tableID := common.TableID(99)
// 	writeSchemaSnapshot(db, gcTS, &model.DBInfo{
// 		ID:   int64(schemaID),
// 		Name: model.NewCIStr("test"),
// 	})
// 	writeTableSnapshot(db, gcTS, int64(schemaID), &model.TableInfo{
// 		ID:   int64(tableID),
// 		Name: model.NewCIStr("t"),
// 	})

// 	dataStorage, _, databaseMap := loadPersistentStorage(db, 1500)
// 	require.Equal(t, 1, len(databaseMap))
// 	require.Equal(t, "test", databaseMap[int64(schemaID)].Name)

// 	{
// 		store := newEmptyVersionedTableInfoStore(tableID)
// 		getSchemaName := func(schemaID int64) (string, error) {
// 			if _, ok := databaseMap[int64(schemaID)]; !ok {
// 				return "", fmt.Errorf("schema not found")
// 			}
// 			return databaseMap[int64(schemaID)].Name, nil
// 		}
// 		dataStorage.buildVersionedTableInfoStore(store, gcTS, metaTS.ResolvedTS, getSchemaName)
// 		store.setTableInfoInitialized()
// 		require.Equal(t, gcTS, store.getFirstVersion())
// 		tableInfo, err := store.getTableInfo(gcTS)
// 		require.Nil(t, err)
// 		require.Equal(t, "t", tableInfo.Name.O)
// 		require.Equal(t, tableID, common.TableID(tableInfo.ID))
// 	}

// 	// mock write some ddl event and load again
// 	renameVersion := uint64(1500)
// 	err = dataStorage.writeDDLEvent(DDLEvent{
// 		Job: &model.Job{
// 			Type:     model.ActionRenameTable,
// 			SchemaID: int64(schemaID),
// 			BinlogInfo: &model.HistoryInfo{
// 				SchemaVersion: 3000,
// 				TableInfo: &model.TableInfo{
// 					ID:   int64(tableID),
// 					Name: model.NewCIStr("t2"),
// 				},
// 				FinishedTS: renameVersion,
// 			},
// 			TableID: int64(tableID),
// 		},
// 	})
// 	require.Nil(t, err)

// 	dataStorage, _, _ = loadPersistentStorage(db, 1500)
// 	{
// 		store := newEmptyVersionedTableInfoStore(tableID)
// 		getSchemaName := func(schemaID int64) (string, error) {
// 			if _, ok := databaseMap[int64(schemaID)]; !ok {
// 				return "", fmt.Errorf("schema not found")
// 			}
// 			return databaseMap[int64(schemaID)].Name, nil
// 		}
// 		dataStorage.buildVersionedTableInfoStore(store, gcTS, metaTS.ResolvedTS, getSchemaName)
// 		store.setTableInfoInitialized()
// 		require.Equal(t, gcTS, store.getFirstVersion())
// 		require.Equal(t, 2, len(store.infos))
// 		tableInfo, err := store.getTableInfo(gcTS)
// 		require.Nil(t, err)
// 		require.Equal(t, "t", tableInfo.Name.O)
// 		require.Equal(t, tableID, common.TableID(tableInfo.ID))
// 		tableInfo2, err := store.getTableInfo(common.Ts(renameVersion))
// 		require.Nil(t, err)
// 		require.Equal(t, "t2", tableInfo2.Name.O)
// 	}
// }

// func TestBuildVersionedTableInfoAndApplyDDL(t *testing.T) {
// 	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
// 	err := os.RemoveAll(dbPath)
// 	require.Nil(t, err)
// 	db, err := pebble.Open(dbPath, &pebble.Options{})
// 	require.Nil(t, err)
// 	defer db.Close()

// 	gcTS := common.Ts(1000)
// 	metaTS := &schemaMetaTS{
// 		ResolvedTS:    2000,
// 		FinishedDDLTS: 3000,
// 		SchemaVersion: 4000,
// 	}

// 	err = writeMetaData(db, gcTS, metaTS)
// 	require.Nil(t, err)

// 	schemaID := 50
// 	tableID := common.TableID(99)
// 	writeSchemaSnapshot(db, gcTS, &model.DBInfo{
// 		ID:   int64(schemaID),
// 		Name: model.NewCIStr("test"),
// 	})
// 	writeTableSnapshot(db, gcTS, int64(schemaID), &model.TableInfo{
// 		ID:   int64(tableID),
// 		Name: model.NewCIStr("t"),
// 	})

// 	dataStorage, _, databaseMap := loadPersistentStorage(db, 1500)
// 	require.Equal(t, 1, len(databaseMap))
// 	require.Equal(t, "test", databaseMap[int64(schemaID)].Name)

// 	store := newEmptyVersionedTableInfoStore(tableID)
// 	getSchemaName := func(schemaID int64) (string, error) {
// 		if _, ok := databaseMap[int64(schemaID)]; !ok {
// 			return "", fmt.Errorf("schema not found")
// 		}
// 		return databaseMap[int64(schemaID)].Name, nil
// 	}
// 	dataStorage.buildVersionedTableInfoStore(store, gcTS, metaTS.ResolvedTS, getSchemaName)
// 	renameVersion := uint64(1500)
// 	store.applyDDL(&model.Job{
// 		Type:     model.ActionRenameTable,
// 		SchemaID: int64(schemaID),
// 		BinlogInfo: &model.HistoryInfo{
// 			SchemaVersion: 3000,
// 			TableInfo: &model.TableInfo{
// 				ID:   int64(tableID),
// 				Name: model.NewCIStr("t2"),
// 			},
// 			FinishedTS: renameVersion,
// 		},
// 		TableID: int64(tableID),
// 	})
// 	store.setTableInfoInitialized()
// 	require.Equal(t, gcTS, store.getFirstVersion())
// 	require.Equal(t, 2, len(store.infos))
// 	tableInfo, err := store.getTableInfo(gcTS)
// 	require.Nil(t, err)
// 	require.Equal(t, "t", tableInfo.Name.O)
// 	require.Equal(t, tableID, common.TableID(tableInfo.ID))
// 	tableInfo2, err := store.getTableInfo(common.Ts(renameVersion))
// 	require.Nil(t, err)
// 	require.Equal(t, "t2", tableInfo2.Name.O)
// }

func newPersistentStorageForTest(db *pebble.DB, gcTs common.Ts, upperBound upperBoundMeta) *persistentStorage {
	p := &persistentStorage{
		pdCli:                  nil,
		kvStorage:              nil,
		db:                     db,
		gcTs:                   gcTs,
		databaseMap:            make(map[common.SchemaID]*DatabaseInfo),
		tablesInKVSnap:         make(map[common.TableID]bool),
		tablesDDLHistory:       make(map[common.TableID][]uint64),
		tableTriggerDDLHistory: make([]uint64, 0),
		tableInfoStoreMap:      make(map[common.TableID]*versionedTableInfoStore),
		tableRegisteredCount:   make(map[common.TableID]int),
	}
	p.initializeFromDisk(upperBound)
	return p
}

func newEmptyPersistentStorageForTest(dbPath string) *persistentStorage {
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		log.Panic("create database fail")
	}
	gcTs := 0
	upperBound := upperBoundMeta{
		FinishedDDLTs: 0,
		SchemaVersion: 0,
		ResolvedTs:    0,
	}
	return newPersistentStorageForTest(db, uint64(gcTs), upperBound)
}

func TestCreateDropSchemaTableDDL(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)
	pStorage := newEmptyPersistentStorageForTest(dbPath)

	schemaID := common.SchemaID(300)
	// create db
	{
		ddlEvent := DDLEvent{
			Job: &model.Job{
				Type:     model.ActionCreateSchema,
				SchemaID: int64(schemaID),
				BinlogInfo: &model.HistoryInfo{
					SchemaVersion: 100,
					TableInfo:     nil,
					FinishedTS:    200,
				},
			},
		}
		pStorage.handleSortedDDLEvents(ddlEvent)

		require.Equal(t, 1, len(pStorage.databaseMap))
		require.Equal(t, common.Ts(200), pStorage.databaseMap[schemaID].CreateVersion)
		require.Equal(t, 1, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, uint64(200), pStorage.tableTriggerDDLHistory[0])
	}

	// create a table
	tableID := common.TableID(100)
	{
		ddlEvent := DDLEvent{
			Job: &model.Job{
				Type:     model.ActionCreateTable,
				SchemaID: int64(schemaID),
				TableID:  int64(tableID),
				BinlogInfo: &model.HistoryInfo{
					SchemaVersion: 101,
					TableInfo:     nil,
					FinishedTS:    201,
				},
			},
		}
		pStorage.handleSortedDDLEvents(ddlEvent)

		require.Equal(t, 1, len(pStorage.databaseMap[schemaID].Tables))
		require.Equal(t, 1, len(pStorage.tablesFromDDLJobs))
		require.Equal(t, 2, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, uint64(201), pStorage.tableTriggerDDLHistory[1])
		require.Equal(t, 1, len(pStorage.tablesDDLHistory))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[tableID]))
	}

	// create another table
	tableID2 := common.TableID(105)
	{
		ddlEvent := DDLEvent{
			Job: &model.Job{
				Type:     model.ActionCreateTable,
				SchemaID: int64(schemaID),
				TableID:  int64(tableID2),
				BinlogInfo: &model.HistoryInfo{
					SchemaVersion: 103,
					TableInfo:     nil,
					FinishedTS:    203,
				},
			},
		}
		pStorage.handleSortedDDLEvents(ddlEvent)

		require.Equal(t, 2, len(pStorage.databaseMap[schemaID].Tables))
		require.Equal(t, 2, len(pStorage.tablesFromDDLJobs))
		require.Equal(t, 3, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, uint64(203), pStorage.tableTriggerDDLHistory[2])
		require.Equal(t, 2, len(pStorage.tablesDDLHistory))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[tableID2]))
		require.Equal(t, uint64(203), pStorage.tablesDDLHistory[tableID2][0])
	}

	// drop a table
	{
		ddlEvent := DDLEvent{
			Job: &model.Job{
				Type:     model.ActionDropTable,
				SchemaID: int64(schemaID),
				TableID:  int64(tableID2),
				BinlogInfo: &model.HistoryInfo{
					SchemaVersion: 105,
					TableInfo:     nil,
					FinishedTS:    205,
				},
			},
		}
		pStorage.handleSortedDDLEvents(ddlEvent)

		require.Equal(t, 1, len(pStorage.databaseMap[schemaID].Tables))
		require.Equal(t, 1, len(pStorage.tablesFromDDLJobs))
		require.Equal(t, 4, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, uint64(205), pStorage.tableTriggerDDLHistory[3])
		require.Equal(t, 2, len(pStorage.tablesDDLHistory))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[tableID]))
		require.Equal(t, 2, len(pStorage.tablesDDLHistory[tableID2]))
		require.Equal(t, uint64(205), pStorage.tablesDDLHistory[tableID2][1])
	}

	// truncate a table
	tableID3 := common.TableID(112)
	{
		ddlEvent := DDLEvent{
			Job: &model.Job{
				Type:     model.ActionTruncateTable,
				SchemaID: int64(schemaID),
				TableID:  int64(tableID),
				BinlogInfo: &model.HistoryInfo{
					SchemaVersion: 107,
					TableInfo: &model.TableInfo{
						ID: int64(tableID3),
					},
					FinishedTS: 207,
				},
			},
		}
		pStorage.handleSortedDDLEvents(ddlEvent)

		require.Equal(t, 1, len(pStorage.databaseMap[schemaID].Tables))
		require.Equal(t, 1, len(pStorage.tablesFromDDLJobs))
		require.Equal(t, 4, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, 3, len(pStorage.tablesDDLHistory))
		require.Equal(t, 2, len(pStorage.tablesDDLHistory[tableID]))
		require.Equal(t, uint64(207), pStorage.tablesDDLHistory[tableID][1])
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[tableID3]))
		require.Equal(t, uint64(207), pStorage.tablesDDLHistory[tableID3][0])
	}

	// drop db
	{
		ddlEvent := DDLEvent{
			Job: &model.Job{
				Type:     model.ActionDropSchema,
				SchemaID: int64(schemaID),
				BinlogInfo: &model.HistoryInfo{
					SchemaVersion: 200,
					TableInfo:     nil,
					FinishedTS:    300,
				},
			},
		}

		pStorage.handleSortedDDLEvents(ddlEvent)

		require.Equal(t, 1, len(pStorage.databaseMap))
		require.Equal(t, common.Ts(300), pStorage.databaseMap[schemaID].DeleteVersion)
		require.Equal(t, 5, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, uint64(300), pStorage.tableTriggerDDLHistory[4])
		require.Equal(t, 3, len(pStorage.tablesDDLHistory))
		require.Equal(t, 2, len(pStorage.tablesDDLHistory[tableID]))
		require.Equal(t, 2, len(pStorage.tablesDDLHistory[tableID2]))
		require.Equal(t, 2, len(pStorage.tablesDDLHistory[tableID3]))
		require.Equal(t, uint64(300), pStorage.tablesDDLHistory[tableID3][1])
	}
}
