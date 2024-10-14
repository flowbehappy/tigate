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
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func loadPersistentStorageForTest(db *pebble.DB, gcTs uint64, upperBound UpperBoundMeta) *persistentStorage {
	p := &persistentStorage{
		pdCli:                  nil,
		kvStorage:              nil,
		db:                     db,
		gcTs:                   gcTs,
		upperBound:             upperBound,
		tableMap:               make(map[int64]*BasicTableInfo),
		partitionMap:           make(map[int64]BasicPartitionInfo),
		databaseMap:            make(map[int64]*BasicDatabaseInfo),
		tablesDDLHistory:       make(map[int64][]uint64),
		tableTriggerDDLHistory: make([]uint64, 0),
		tableInfoStoreMap:      make(map[int64]*versionedTableInfoStore),
		tableRegisteredCount:   make(map[int64]int),
	}
	p.initializeFromDisk()
	return p
}

// create an empty persistent storage at dbPath
func newEmptyPersistentStorageForTest(dbPath string) *persistentStorage {
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		log.Panic("create database fail")
	}
	gcTs := uint64(0)
	upperBound := UpperBoundMeta{
		FinishedDDLTs: 0,
		SchemaVersion: 0,
		ResolvedTs:    0,
	}
	return loadPersistentStorageForTest(db, gcTs, upperBound)
}

// create a persistent storage with initial db info and table info
func newPersistentStorageForTest(dbPath string, gcTs uint64, initialDBInfos map[int64]*model.DBInfo) *persistentStorage {
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		log.Panic("create database fail")
	}
	if len(initialDBInfos) > 0 {
		mockWriteKVSnapOnDisk(db, gcTs, initialDBInfos)
	}
	upperBound := UpperBoundMeta{
		FinishedDDLTs: 0,
		SchemaVersion: 0,
		ResolvedTs:    gcTs,
	}
	writeUpperBoundMeta(db, upperBound)
	return loadPersistentStorageForTest(db, gcTs, upperBound)
}

func mockWriteKVSnapOnDisk(db *pebble.DB, snapTs uint64, dbInfos map[int64]*model.DBInfo) {
	batch := db.NewBatch()
	defer batch.Close()
	for _, dbInfo := range dbInfos {
		writeSchemaInfoToBatch(batch, snapTs, dbInfo)
		for _, tableInfo := range dbInfo.Tables {
			tableInfoValue, err := json.Marshal(tableInfo)
			if err != nil {
				log.Panic("marshal table info fail", zap.Error(err))
			}
			writeTableInfoToBatch(batch, snapTs, dbInfo, tableInfoValue)
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		log.Panic("commit batch fail", zap.Error(err))
	}
	writeGcTs(db, snapTs)
}

func TestReadWriteMeta(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.Nil(t, err)
	defer db.Close()

	{
		gcTS := uint64(1000)
		upperBound := UpperBoundMeta{
			FinishedDDLTs: 3000,
			SchemaVersion: 4000,
			ResolvedTs:    1000,
		}

		writeGcTs(db, gcTS)
		writeUpperBoundMeta(db, upperBound)

		gcTSRead, err := readGcTs(db)
		require.Nil(t, err)
		require.Equal(t, gcTS, gcTSRead)

		upperBoundRead, err := readUpperBoundMeta(db)
		require.Nil(t, err)
		require.Equal(t, upperBound, upperBoundRead)
	}

	// update gcTs
	{
		gcTS := uint64(2000)

		writeGcTs(db, gcTS)

		gcTSRead, err := readGcTs(db)
		require.Nil(t, err)
		require.Equal(t, gcTS, gcTSRead)
	}

	// update upperbound
	{
		upperBound := UpperBoundMeta{
			FinishedDDLTs: 5000,
			SchemaVersion: 5000,
			ResolvedTs:    1000,
		}

		writeUpperBoundMeta(db, upperBound)

		upperBoundRead, err := readUpperBoundMeta(db)
		require.Nil(t, err)
		require.Equal(t, upperBound, upperBoundRead)
	}
}

func TestBuildVersionedTableInfoStore(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)

	gcTs := uint64(1000)
	schemaID := int64(50)
	tableID := int64(99)
	databaseInfo := make(map[int64]*model.DBInfo)
	databaseInfo[schemaID] = &model.DBInfo{
		ID:   schemaID,
		Name: model.NewCIStr("test"),
		Tables: []*model.TableInfo{
			{
				ID:   tableID,
				Name: model.NewCIStr("t1"),
			},
		},
	}
	pStorage := newPersistentStorageForTest(dbPath, gcTs, databaseInfo)

	require.Equal(t, 1, len(pStorage.databaseMap))
	require.Equal(t, "test", pStorage.databaseMap[schemaID].Name)

	{
		store := newEmptyVersionedTableInfoStore(tableID)
		pStorage.buildVersionedTableInfoStore(store)
		tableInfo, err := store.getTableInfo(gcTs)
		require.Nil(t, err)
		require.Equal(t, "t1", tableInfo.Name.O)
		require.Equal(t, tableID, tableInfo.ID)
	}

	// rename table
	renameVersion := uint64(1500)
	{
		job := &model.Job{
			Type:     model.ActionRenameTable,
			SchemaID: schemaID,
			TableID:  tableID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 3000,
				TableInfo: &model.TableInfo{
					ID:   tableID,
					Name: model.NewCIStr("t2"),
				},
				FinishedTS: renameVersion,
			},
		}
		err = pStorage.handleDDLJob(job)
		require.Nil(t, err)
	}

	// create another table
	tableID2 := tableID + 1
	createVersion := renameVersion + 200
	{
		job := &model.Job{
			Type:     model.ActionCreateTable,
			SchemaID: schemaID,
			TableID:  tableID2,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 3500,
				TableInfo: &model.TableInfo{
					ID:   tableID2,
					Name: model.NewCIStr("t3"),
				},
				FinishedTS: createVersion,
			},
		}
		err = pStorage.handleDDLJob(job)
		require.Nil(t, err)
	}

	upperBound := UpperBoundMeta{
		FinishedDDLTs: 3000,
		SchemaVersion: 4000,
		ResolvedTs:    2000,
	}
	pStorage = loadPersistentStorageForTest(pStorage.db, gcTs, upperBound)
	{
		store := newEmptyVersionedTableInfoStore(tableID)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 2, len(store.infos))
		tableInfo, err := store.getTableInfo(gcTs)
		require.Nil(t, err)
		require.Equal(t, "t1", tableInfo.Name.O)
		require.Equal(t, tableID, tableInfo.ID)
		tableInfo2, err := store.getTableInfo(renameVersion)
		require.Nil(t, err)
		require.Equal(t, "t2", tableInfo2.Name.O)

		renameVersion2 := uint64(3000)
		store.applyDDL(&PersistedDDLEvent{
			Type:            byte(model.ActionRenameTable),
			CurrentSchemaID: schemaID,
			CurrentTableID:  tableID,
			SchemaVersion:   3000,
			TableInfo: &model.TableInfo{
				ID:   tableID,
				Name: model.NewCIStr("t3"),
			},
			FinishedTs: renameVersion2,
		})
		tableInfo3, err := store.getTableInfo(renameVersion2)
		require.Nil(t, err)
		require.Equal(t, "t3", tableInfo3.Name.O)
	}

	{
		store := newEmptyVersionedTableInfoStore(tableID2)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 1, len(store.infos))
		tableInfo, err := store.getTableInfo(createVersion)
		require.Nil(t, err)
		require.Equal(t, "t3", tableInfo.Name.O)
		require.Equal(t, tableID2, tableInfo.ID)
	}

	// truncate table
	tableID3 := tableID2 + 1
	truncateVersion := createVersion + 200
	{
		job := &model.Job{
			Type:     model.ActionTruncateTable,
			SchemaID: schemaID,
			TableID:  tableID2,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 3600,
				TableInfo: &model.TableInfo{
					ID:   tableID3,
					Name: model.NewCIStr("t4"),
				},
				FinishedTS: truncateVersion,
			},
		}
		err = pStorage.handleDDLJob(job)
		require.Nil(t, err)
	}

	{
		store := newEmptyVersionedTableInfoStore(tableID2)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 1, len(store.infos))
		require.Equal(t, truncateVersion, store.deleteVersion)
	}

	{
		store := newEmptyVersionedTableInfoStore(tableID3)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 1, len(store.infos))
		tableInfo, err := store.getTableInfo(truncateVersion)
		require.Nil(t, err)
		require.Equal(t, "t4", tableInfo.Name.O)
		require.Equal(t, tableID3, tableInfo.ID)
	}
}

func TestBuildVersionedTableInfoStoreWithPartitionTable(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)

	gcTs := uint64(1000)
	schemaID := int64(50)
	tableID := int64(99)
	databaseInfo := make(map[int64]*model.DBInfo)
	databaseInfo[schemaID] = &model.DBInfo{
		ID:   schemaID,
		Name: model.NewCIStr("test"),
	}
	pStorage := newPersistentStorageForTest(dbPath, gcTs, databaseInfo)

	// create a partition table
	partitionID1 := tableID + 100
	partitionID2 := tableID + 200
	{
		job := &model.Job{
			Type:     model.ActionCreateTable,
			SchemaID: schemaID,
			TableID:  tableID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 2000,
				TableInfo: &model.TableInfo{
					ID:   tableID,
					Name: model.NewCIStr("t"),
					Partition: &model.PartitionInfo{
						Definitions: []model.PartitionDefinition{
							{
								ID: partitionID1,
							},
							{
								ID: partitionID2,
							},
						},
					},
				},
				FinishedTS: 1200,
			},
		}
		pStorage.handleDDLJob(job)
	}

	upperBound := UpperBoundMeta{
		FinishedDDLTs: 3000,
		SchemaVersion: 4000,
		ResolvedTs:    2000,
	}
	pStorage = loadPersistentStorageForTest(pStorage.db, gcTs, upperBound)
	{
		store := newEmptyVersionedTableInfoStore(partitionID1)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 1, len(store.infos))
	}
	{
		store := newEmptyVersionedTableInfoStore(partitionID2)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 1, len(store.infos))
	}

	// truncate the partition table
	tableID2 := tableID + 500
	partitionID3 := tableID2 + 100
	partitionID4 := tableID2 + 200
	{
		job := &model.Job{
			Type:     model.ActionTruncateTable,
			SchemaID: schemaID,
			TableID:  tableID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 2100,
				TableInfo: &model.TableInfo{
					ID:   tableID2,
					Name: model.NewCIStr("t"),
					Partition: &model.PartitionInfo{
						Definitions: []model.PartitionDefinition{
							{
								ID: partitionID3,
							},
							{
								ID: partitionID4,
							},
						},
					},
				},
				FinishedTS: 1300,
			},
		}
		pStorage.handleDDLJob(job)
	}

	{
		store := newEmptyVersionedTableInfoStore(partitionID1)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 1, len(store.infos))
		require.Equal(t, uint64(1300), store.deleteVersion)
	}
	{
		store := newEmptyVersionedTableInfoStore(partitionID2)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 1, len(store.infos))
		require.Equal(t, uint64(1300), store.deleteVersion)
	}
	{
		store := newEmptyVersionedTableInfoStore(partitionID3)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 1, len(store.infos))
	}
	{
		store := newEmptyVersionedTableInfoStore(partitionID4)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 1, len(store.infos))
	}
}

func TestHandleCreateDropSchemaTableDDL(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)
	pStorage := newEmptyPersistentStorageForTest(dbPath)

	// create db
	schemaID := int64(300)
	{
		job := &model.Job{
			Type:     model.ActionCreateSchema,
			SchemaID: schemaID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 100,
				DBInfo: &model.DBInfo{
					ID:   schemaID,
					Name: model.NewCIStr("test"),
				},
				TableInfo:  nil,
				FinishedTS: 200,
			},
		}
		pStorage.handleDDLJob(job)

		require.Equal(t, 1, len(pStorage.databaseMap))
		require.Equal(t, "test", pStorage.databaseMap[schemaID].Name)
		require.Equal(t, 1, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, uint64(200), pStorage.tableTriggerDDLHistory[0])
	}

	// create a table
	tableID := int64(100)
	{
		job := &model.Job{
			Type:     model.ActionCreateTable,
			SchemaID: schemaID,
			TableID:  tableID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 101,
				TableInfo: &model.TableInfo{
					Name: model.NewCIStr("t1"),
				},
				FinishedTS: 201,
			},
		}
		pStorage.handleDDLJob(job)

		require.Equal(t, 1, len(pStorage.databaseMap[schemaID].Tables))
		require.Equal(t, 1, len(pStorage.tableMap))
		require.Equal(t, 2, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, uint64(201), pStorage.tableTriggerDDLHistory[1])
		require.Equal(t, 1, len(pStorage.tablesDDLHistory))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[tableID]))
	}

	// create another table
	tableID2 := int64(105)
	{
		job := &model.Job{
			Type:     model.ActionCreateTable,
			SchemaID: schemaID,
			TableID:  tableID2,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 103,
				TableInfo: &model.TableInfo{
					Name: model.NewCIStr("t2"),
				},

				FinishedTS: 203,
			},
		}
		pStorage.handleDDLJob(job)

		require.Equal(t, 2, len(pStorage.databaseMap[schemaID].Tables))
		require.Equal(t, 2, len(pStorage.tableMap))
		require.Equal(t, 3, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, uint64(203), pStorage.tableTriggerDDLHistory[2])
		require.Equal(t, 2, len(pStorage.tablesDDLHistory))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[tableID2]))
		require.Equal(t, uint64(203), pStorage.tablesDDLHistory[tableID2][0])
	}

	// drop a table
	{
		job := &model.Job{
			Type:     model.ActionDropTable,
			SchemaID: schemaID,
			TableID:  tableID2,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 105,
				TableInfo:     nil,
				FinishedTS:    205,
			},
		}
		pStorage.handleDDLJob(job)

		require.Equal(t, 1, len(pStorage.databaseMap[schemaID].Tables))
		require.Equal(t, 1, len(pStorage.tableMap))
		require.Equal(t, 4, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, uint64(205), pStorage.tableTriggerDDLHistory[3])
		require.Equal(t, 2, len(pStorage.tablesDDLHistory))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[tableID]))
		require.Equal(t, 2, len(pStorage.tablesDDLHistory[tableID2]))
		require.Equal(t, uint64(205), pStorage.tablesDDLHistory[tableID2][1])
	}

	// truncate a table
	tableID3 := int64(112)
	{
		job := &model.Job{
			Type:     model.ActionTruncateTable,
			SchemaID: schemaID,
			TableID:  tableID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 107,
				TableInfo: &model.TableInfo{
					ID: tableID3,
				},
				FinishedTS: 207,
			},
		}
		pStorage.handleDDLJob(job)

		require.Equal(t, 1, len(pStorage.databaseMap[schemaID].Tables))
		require.Equal(t, 1, len(pStorage.tableMap))
		require.Equal(t, 4, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, 3, len(pStorage.tablesDDLHistory))
		require.Equal(t, 2, len(pStorage.tablesDDLHistory[tableID]))
		require.Equal(t, uint64(207), pStorage.tablesDDLHistory[tableID][1])
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[tableID3]))
		require.Equal(t, uint64(207), pStorage.tablesDDLHistory[tableID3][0])
	}

	// drop db
	{
		job := &model.Job{
			Type:     model.ActionDropSchema,
			SchemaID: schemaID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 200,
				DBInfo: &model.DBInfo{
					ID:   schemaID,
					Name: model.NewCIStr("test"),
				},
				TableInfo:  nil,
				FinishedTS: 300,
			},
		}

		pStorage.handleDDLJob(job)

		require.Equal(t, 0, len(pStorage.databaseMap))
		require.Equal(t, 0, len(pStorage.tableMap))
		require.Equal(t, 5, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, uint64(300), pStorage.tableTriggerDDLHistory[4])
		require.Equal(t, 3, len(pStorage.tablesDDLHistory))
		require.Equal(t, 2, len(pStorage.tablesDDLHistory[tableID]))
		require.Equal(t, 2, len(pStorage.tablesDDLHistory[tableID2]))
		require.Equal(t, 2, len(pStorage.tablesDDLHistory[tableID3]))
		require.Equal(t, uint64(300), pStorage.tablesDDLHistory[tableID3][1])
	}
}

func TestPartitionTableDDL(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)

	gcTs := uint64(100)
	schemaID := int64(300)
	databaseInfo := make(map[int64]*model.DBInfo)
	databaseInfo[schemaID] = &model.DBInfo{
		ID:   schemaID,
		Name: model.NewCIStr("test"),
	}
	pStorage := newPersistentStorageForTest(dbPath, gcTs, databaseInfo)

	// create a partition table
	tableID := int64(100)
	partitionID1 := tableID + 100
	partitionID2 := tableID + 200
	partitionID3 := tableID + 300
	{
		job := &model.Job{
			Type:     model.ActionCreateTable,
			SchemaID: schemaID,
			TableID:  tableID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 101,
				TableInfo: &model.TableInfo{
					ID:   tableID,
					Name: model.NewCIStr("t"),
					Partition: &model.PartitionInfo{
						Definitions: []model.PartitionDefinition{
							{
								ID: partitionID1,
							},
							{
								ID: partitionID2,
							},
							{
								ID: partitionID3,
							},
						},
					},
				},
				FinishedTS: 201,
			},
		}
		pStorage.handleDDLJob(job)

		require.Equal(t, 1, len(pStorage.databaseMap[schemaID].Tables))
		require.Equal(t, 1, len(pStorage.tableMap))
		require.Equal(t, 3, len(pStorage.partitionMap[tableID]))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[partitionID1]))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[partitionID2]))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[partitionID3]))
	}

	// truncate the partition table
	tableID2 := tableID + 400
	partitionID4 := tableID2 + 100
	partitionID5 := tableID2 + 200
	partitionID6 := tableID2 + 300
	{
		job := &model.Job{
			Type:     model.ActionTruncateTable,
			SchemaID: schemaID,
			TableID:  tableID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 107,
				TableInfo: &model.TableInfo{
					ID:   tableID2,
					Name: model.NewCIStr("t"),
					Partition: &model.PartitionInfo{
						Definitions: []model.PartitionDefinition{
							{
								ID: partitionID4,
							},
							{
								ID: partitionID5,
							},
							{
								ID: partitionID6,
							},
						},
					},
				},
				FinishedTS: 207,
			},
		}
		pStorage.handleDDLJob(job)

		require.Equal(t, 1, len(pStorage.databaseMap[schemaID].Tables))
		require.Equal(t, 1, len(pStorage.tableMap))
		require.Equal(t, 1, len(pStorage.partitionMap))
		require.Equal(t, 3, len(pStorage.partitionMap[tableID2]))
		require.Equal(t, 2, len(pStorage.tablesDDLHistory[partitionID1]))
		require.Equal(t, 2, len(pStorage.tablesDDLHistory[partitionID2]))
		require.Equal(t, 2, len(pStorage.tablesDDLHistory[partitionID3]))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[partitionID4]))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[partitionID5]))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[partitionID6]))
	}

	// add partition 7
	partitionID7 := partitionID6 + 100
	{
		job := &model.Job{
			Type:     model.ActionAddTablePartition,
			SchemaID: schemaID,
			TableID:  tableID2,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 109,
				TableInfo: &model.TableInfo{
					ID:   tableID2,
					Name: model.NewCIStr("t"),
					Partition: &model.PartitionInfo{
						Definitions: []model.PartitionDefinition{
							{
								ID: partitionID4,
							},
							{
								ID: partitionID5,
							},
							{
								ID: partitionID6,
							},
							{
								ID: partitionID7,
							},
						},
					},
				},
				FinishedTS: 209,
			},
		}
		pStorage.handleDDLJob(job)

		require.Equal(t, 4, len(pStorage.partitionMap[tableID2]))
		require.Equal(t, 2, len(pStorage.tablesDDLHistory[partitionID4]))
		require.Equal(t, 2, len(pStorage.tablesDDLHistory[partitionID5]))
		require.Equal(t, 2, len(pStorage.tablesDDLHistory[partitionID6]))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[partitionID7]))
	}

	// drop patition 4
	{
		job := &model.Job{
			Type:     model.ActionDropTablePartition,
			SchemaID: schemaID,
			TableID:  tableID2,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 111,
				TableInfo: &model.TableInfo{
					ID:   tableID2,
					Name: model.NewCIStr("t"),
					Partition: &model.PartitionInfo{
						Definitions: []model.PartitionDefinition{
							{
								ID: partitionID5,
							},
							{
								ID: partitionID6,
							},
							{
								ID: partitionID7,
							},
						},
					},
				},
				FinishedTS: 211,
			},
		}
		pStorage.handleDDLJob(job)

		require.Equal(t, 3, len(pStorage.partitionMap[tableID2]))
		require.Equal(t, 3, len(pStorage.tablesDDLHistory[partitionID4]))
		require.Equal(t, 3, len(pStorage.tablesDDLHistory[partitionID5]))
		require.Equal(t, 3, len(pStorage.tablesDDLHistory[partitionID6]))
		require.Equal(t, 2, len(pStorage.tablesDDLHistory[partitionID7]))
	}

	// truncate partition 5 -> 8
	partitionID8 := partitionID7 + 100
	{
		job := &model.Job{
			Type:     model.ActionTruncateTablePartition,
			SchemaID: schemaID,
			TableID:  tableID2,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 113,
				TableInfo: &model.TableInfo{
					ID:   tableID2,
					Name: model.NewCIStr("t"),
					Partition: &model.PartitionInfo{
						Definitions: []model.PartitionDefinition{
							{
								ID: partitionID6,
							},
							{
								ID: partitionID7,
							},
							{
								ID: partitionID8,
							},
						},
					},
				},
				FinishedTS: 213,
			},
		}
		pStorage.handleDDLJob(job)

		require.Equal(t, 3, len(pStorage.partitionMap[tableID2]))
		require.Equal(t, 3, len(pStorage.tablesDDLHistory[partitionID4]))
		require.Equal(t, 4, len(pStorage.tablesDDLHistory[partitionID5]))
		require.Equal(t, 4, len(pStorage.tablesDDLHistory[partitionID6]))
		require.Equal(t, 3, len(pStorage.tablesDDLHistory[partitionID7]))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[partitionID8]))
	}

	// // TODO: test reorganize partition(becuase its logic is same as truncate partition, ignore it now, add test after verify truncate partition)

	// drop the partition table
	{
		job := &model.Job{
			Type:     model.ActionDropTable,
			SchemaID: schemaID,
			TableID:  tableID2,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 240,
				TableInfo: &model.TableInfo{
					ID:   tableID2,
					Name: model.NewCIStr("t"),
					Partition: &model.PartitionInfo{
						Definitions: []model.PartitionDefinition{
							{
								ID: partitionID6,
							},
							{
								ID: partitionID7,
							},
							{
								ID: partitionID8,
							},
						},
					},
				},
				FinishedTS: 300,
			},
		}
		pStorage.handleDDLJob(job)

		require.Equal(t, 0, len(pStorage.databaseMap[schemaID].Tables))
		require.Equal(t, 0, len(pStorage.tableMap))
		require.Equal(t, 0, len(pStorage.partitionMap))
		require.Equal(t, 3, len(pStorage.tablesDDLHistory[partitionID4]))
		require.Equal(t, 4, len(pStorage.tablesDDLHistory[partitionID5]))
		require.Equal(t, 5, len(pStorage.tablesDDLHistory[partitionID6]))
		require.Equal(t, 4, len(pStorage.tablesDDLHistory[partitionID7]))
		require.Equal(t, 2, len(pStorage.tablesDDLHistory[partitionID8]))
	}

	{
		ddlEvents, err := pStorage.fetchTableTriggerDDLEvents(nil, 200, 300)
		require.Nil(t, err)
		require.Equal(t, 2, len(ddlEvents))
		// create table event
		verifyTableIsAdded(t, ddlEvents[0], partitionID1, schemaID)
		verifyTableIsAdded(t, ddlEvents[0], partitionID2, schemaID)
		verifyTableIsAdded(t, ddlEvents[0], partitionID3, schemaID)

		// drop table event
		verifyTableIsBlocked(t, ddlEvents[1], partitionID6)
		verifyTableIsBlocked(t, ddlEvents[1], partitionID7)
		verifyTableIsBlocked(t, ddlEvents[1], partitionID8)
		verifyTableIsDropped(t, ddlEvents[1], partitionID6)
		verifyTableIsDropped(t, ddlEvents[1], partitionID7)
		verifyTableIsDropped(t, ddlEvents[1], partitionID8)
	}

	// partition 1, 2, 3 should be same
	{
		// don't fetch the create table events
		ddlEvents, err := pStorage.fetchTableDDLEvents(partitionID1, nil, 201, 300)
		require.Nil(t, err)
		require.Equal(t, 1, len(ddlEvents))
		// truncate table event
		verifyTableIsBlocked(t, ddlEvents[0], partitionID1)
		verifyTableIsBlocked(t, ddlEvents[0], partitionID2)
		verifyTableIsBlocked(t, ddlEvents[0], partitionID3)
		verifyTableIsDropped(t, ddlEvents[0], partitionID1)
		verifyTableIsDropped(t, ddlEvents[0], partitionID2)
		verifyTableIsDropped(t, ddlEvents[0], partitionID3)
		verifyTableIsAdded(t, ddlEvents[0], partitionID4, schemaID)
		verifyTableIsAdded(t, ddlEvents[0], partitionID5, schemaID)
		verifyTableIsAdded(t, ddlEvents[0], partitionID6, schemaID)
	}

	// partition 4: test add partition/drop partition
	{
		ddlEvents, err := pStorage.fetchTableDDLEvents(partitionID4, nil, 207, 300)
		require.Nil(t, err)
		require.Equal(t, 2, len(ddlEvents))
		// add partition event for partition 7
		require.Equal(t, model.ActionAddTablePartition, model.ActionType(ddlEvents[0].Type))
		verifyTableIsBlocked(t, ddlEvents[0], partitionID4)
		verifyTableIsBlocked(t, ddlEvents[0], partitionID5)
		verifyTableIsBlocked(t, ddlEvents[0], partitionID6)
		verifyTableIsAdded(t, ddlEvents[0], partitionID7, schemaID)
		// drop partition event
		require.Equal(t, model.ActionDropTablePartition, model.ActionType(ddlEvents[1].Type))
		verifyTableIsBlocked(t, ddlEvents[1], partitionID4)
		verifyTableIsBlocked(t, ddlEvents[1], partitionID5)
		verifyTableIsBlocked(t, ddlEvents[1], partitionID6)
		verifyTableIsBlocked(t, ddlEvents[1], partitionID7)
		verifyTableIsDropped(t, ddlEvents[1], partitionID4)
	}

	// partition 5: test truncate partition
	{
		ddlEvents, err := pStorage.fetchTableDDLEvents(partitionID5, nil, 207, 300)
		require.Nil(t, err)
		require.Equal(t, 3, len(ddlEvents))
		// add partition event for partition 7
		require.Equal(t, model.ActionAddTablePartition, model.ActionType(ddlEvents[0].Type))
		// drop partition event for partition 4
		require.Equal(t, model.ActionDropTablePartition, model.ActionType(ddlEvents[1].Type))
		// truncate partition event
		require.Equal(t, model.ActionTruncateTablePartition, model.ActionType(ddlEvents[2].Type))
		verifyTableIsBlocked(t, ddlEvents[2], partitionID5)
		verifyTableIsBlocked(t, ddlEvents[2], partitionID6)
		verifyTableIsBlocked(t, ddlEvents[2], partitionID7)
		verifyTableIsDropped(t, ddlEvents[2], partitionID5)
		verifyTableIsAdded(t, ddlEvents[2], partitionID8, schemaID)
	}

	// partiont 6 should be same
	{
		ddlEvents, err := pStorage.fetchTableDDLEvents(partitionID6, nil, 207, 300)
		require.Nil(t, err)
		require.Equal(t, 4, len(ddlEvents))
		require.Equal(t, model.ActionAddTablePartition, model.ActionType(ddlEvents[0].Type))
		require.Equal(t, model.ActionDropTablePartition, model.ActionType(ddlEvents[1].Type))
		require.Equal(t, model.ActionTruncateTablePartition, model.ActionType(ddlEvents[2].Type))
		require.Equal(t, model.ActionDropTable, model.ActionType(ddlEvents[3].Type))
	}

	// partition 7
	{
		ddlEvents, err := pStorage.fetchTableDDLEvents(partitionID7, nil, 209, 300)
		require.Nil(t, err)
		require.Equal(t, 3, len(ddlEvents))
		require.Equal(t, model.ActionDropTablePartition, model.ActionType(ddlEvents[0].Type))
		require.Equal(t, model.ActionTruncateTablePartition, model.ActionType(ddlEvents[1].Type))
		require.Equal(t, model.ActionDropTable, model.ActionType(ddlEvents[2].Type))
	}

	// partition 8
	{
		ddlEvents, err := pStorage.fetchTableDDLEvents(partitionID8, nil, 209, 300)
		require.Nil(t, err)
		require.Equal(t, 2, len(ddlEvents))
		require.Equal(t, model.ActionTruncateTablePartition, model.ActionType(ddlEvents[0].Type))
		require.Equal(t, model.ActionDropTable, model.ActionType(ddlEvents[1].Type))
	}

	// test drop schema can clear partition info
	{
		pStorage.handleDDLJob(&model.Job{
			Type:     model.ActionCreateTable,
			SchemaID: schemaID,
			TableID:  tableID + 1000,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 255,
				TableInfo: &model.TableInfo{
					ID:   tableID + 1000,
					Name: model.NewCIStr("t100"),
					Partition: &model.PartitionInfo{
						Definitions: []model.PartitionDefinition{
							{
								ID: partitionID4 + 500,
							},
							{
								ID: partitionID5 + 500,
							},
						},
					},
				},
				FinishedTS: 355,
			},
		})
		require.Less(t, 0, len(pStorage.partitionMap))

		job := &model.Job{
			Type:     model.ActionDropSchema,
			SchemaID: schemaID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 260,
				DBInfo: &model.DBInfo{
					ID:   schemaID,
					Name: model.NewCIStr("test"),
				},
				TableInfo:  nil,
				FinishedTS: 360,
			},
		}
		pStorage.handleDDLJob(job)
		require.Equal(t, 0, len(pStorage.databaseMap))
		require.Equal(t, 0, len(pStorage.partitionMap))
	}
}

func TestExchangePartitionTable(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)

	gcTs := uint64(100)
	schemaID1 := int64(300)
	schemaID2 := schemaID1 + 100
	databaseInfo := make(map[int64]*model.DBInfo)
	databaseInfo[schemaID1] = &model.DBInfo{
		ID:   schemaID1,
		Name: model.NewCIStr("test"),
	}
	databaseInfo[schemaID2] = &model.DBInfo{
		ID:   schemaID2,
		Name: model.NewCIStr("test2"),
	}
	pStorage := newPersistentStorageForTest(dbPath, gcTs, databaseInfo)

	// create a partition table
	tableID := int64(100)
	partitionID1 := tableID + 100
	partitionID2 := tableID + 200
	partitionID3 := tableID + 300
	{
		job := &model.Job{
			Type:     model.ActionCreateTable,
			SchemaID: schemaID1,
			TableID:  tableID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 101,
				TableInfo: &model.TableInfo{
					ID:   tableID,
					Name: model.NewCIStr("t"),
					Partition: &model.PartitionInfo{
						Definitions: []model.PartitionDefinition{
							{
								ID: partitionID1,
							},
							{
								ID: partitionID2,
							},
							{
								ID: partitionID3,
							},
						},
					},
				},
				FinishedTS: 201,
			},
		}
		pStorage.handleDDLJob(job)
	}

	// create a normal table
	tableID2 := tableID + 2000
	{
		job := &model.Job{
			Type:     model.ActionCreateTable,
			SchemaID: schemaID2,
			TableID:  tableID2,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 103,
				TableInfo: &model.TableInfo{
					ID:   tableID,
					Name: model.NewCIStr("t2"),
				},
				FinishedTS: 203,
			},
		}
		pStorage.handleDDLJob(job)
	}

	{
		require.Equal(t, 2, len(pStorage.tableMap))
		require.Equal(t, 3, len(pStorage.partitionMap[tableID]))
	}

	// exchange table with partition 3
	{
		job := &model.Job{
			Type:     model.ActionExchangeTablePartition,
			SchemaID: schemaID2,
			TableID:  tableID2,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 105,
				TableInfo: &model.TableInfo{
					ID:   tableID,
					Name: model.NewCIStr("t"),
					Partition: &model.PartitionInfo{
						Definitions: []model.PartitionDefinition{
							{
								ID: partitionID1,
							},
							{
								ID: partitionID2,
							},
							{
								ID: tableID2,
							},
						},
					},
				},
				FinishedTS: 205,
			},
		}
		pStorage.handleDDLJob(job)
	}

	{
		// verify databaseMap is updated successfully
		_, ok := pStorage.databaseMap[schemaID2].Tables[partitionID3]
		require.True(t, ok)
		_, ok = pStorage.databaseMap[schemaID2].Tables[tableID2]
		require.False(t, ok)
		// verify tableMap is updated successfully
		require.Equal(t, 2, len(pStorage.tableMap))
		require.Equal(t, schemaID2, pStorage.tableMap[partitionID3].SchemaID)
		_, ok = pStorage.tableMap[tableID2]
		require.False(t, ok)
		// verify partitionMap is updated successfully
		require.Equal(t, 3, len(pStorage.partitionMap[tableID]))
		_, ok = pStorage.partitionMap[tableID][tableID2]
		require.True(t, ok)
		_, ok = pStorage.partitionMap[tableID][partitionID3]
		require.False(t, ok)
	}

	// verify build table info store
	{
		store := newEmptyVersionedTableInfoStore(partitionID3)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 2, len(store.infos))
		require.Equal(t, "t", store.infos[0].info.Name.O)
		require.Equal(t, "t2", store.infos[1].info.Name.O)
	}

	{
		store := newEmptyVersionedTableInfoStore(tableID2)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 2, len(store.infos))
		require.Equal(t, "t2", store.infos[0].info.Name.O)
		require.Equal(t, "t", store.infos[1].info.Name.O)
	}

	// verify ddl events are set correctly
	{
		ddlEvents, err := pStorage.fetchTableDDLEvents(partitionID3, nil, 201, 205)
		require.Nil(t, err)
		require.Equal(t, 1, len(ddlEvents))
		verifyTableIsBlocked(t, ddlEvents[0], partitionID3)
		verifyTableIsBlocked(t, ddlEvents[0], tableID2)

		ddlEvents, err = pStorage.fetchTableDDLEvents(tableID2, nil, 203, 205)
		require.Nil(t, err)
		require.Equal(t, 1, len(ddlEvents))
	}

	// test filter: normal table is filtered out
	{
		filterConfig := &config.FilterConfig{
			Rules: []string{"test.t"},
		}
		tableFilter, err := filter.NewFilter(filterConfig, "", false)
		require.Nil(t, err)
		ddlEvents, err := pStorage.fetchTableDDLEvents(partitionID3, tableFilter, 201, 205)
		require.Nil(t, err)
		require.Equal(t, 1, len(ddlEvents))
		require.Nil(t, ddlEvents[0].BlockedTables)

		verifyTableIsDropped(t, ddlEvents[0], partitionID3)

		verifyTableIsAdded(t, ddlEvents[0], tableID2, schemaID1)
	}

	// test filter: partition table is filtered out
	{
		filterConfig := &config.FilterConfig{
			Rules: []string{"test2.t2"},
		}
		tableFilter, err := filter.NewFilter(filterConfig, "", false)
		require.Nil(t, err)
		ddlEvents, err := pStorage.fetchTableDDLEvents(tableID2, tableFilter, 203, 205)
		require.Nil(t, err)
		require.Equal(t, 1, len(ddlEvents))
		require.Nil(t, ddlEvents[0].BlockedTables)

		verifyTableIsDropped(t, ddlEvents[0], tableID2)

		verifyTableIsAdded(t, ddlEvents[0], partitionID3, schemaID2)
	}
}

func TestAlterBetweenPartitionTableAndNonPartitionTable(t *testing.T) {

}

func verifyTableIsBlocked(t *testing.T, event common.DDLEvent, tableID int64) {
	require.Equal(t, common.InfluenceTypeNormal, event.BlockedTables.InfluenceType)
	for _, id := range event.BlockedTables.TableIDs {
		if id == tableID {
			return
		}
	}
	log.Info("blocked tables",
		zap.Any("type", event.Type),
		zap.Any("blocked tables", event.BlockedTables),
		zap.Int64("tableID", tableID))
	require.True(t, false)
}

func verifyTableIsDropped(t *testing.T, event common.DDLEvent, tableID int64) {
	require.Equal(t, common.InfluenceTypeNormal, event.NeedDroppedTables.InfluenceType)
	for _, id := range event.NeedDroppedTables.TableIDs {
		if id == tableID {
			return
		}
	}
	require.True(t, false)
}

func verifyTableIsAdded(t *testing.T, event common.DDLEvent, tableID int64, schemaID int64) {
	for _, table := range event.NeedAddedTables {
		if table.TableID == tableID && table.SchemaID == schemaID {
			return
		}
	}
	require.True(t, false)
}

func TestHandleRenameTable(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)

	gcTs := uint64(500)
	schemaID1 := int64(300)
	schemaID2 := int64(305)

	databaseInfo := make(map[int64]*model.DBInfo)
	databaseInfo[schemaID1] = &model.DBInfo{
		ID:   schemaID1,
		Name: model.NewCIStr("test"),
	}
	databaseInfo[schemaID2] = &model.DBInfo{
		ID:   schemaID2,
		Name: model.NewCIStr("test2"),
	}
	pStorage := newPersistentStorageForTest(dbPath, gcTs, databaseInfo)

	// create a table
	tableID := int64(100)
	{
		job := &model.Job{
			Type:     model.ActionCreateTable,
			SchemaID: schemaID1,
			TableID:  tableID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 501,
				TableInfo: &model.TableInfo{
					ID:   tableID,
					Name: model.NewCIStr("t1"),
				},
				FinishedTS: 601,
			},
		}
		pStorage.handleDDLJob(job)
		require.Equal(t, 2, len(pStorage.databaseMap))
		require.Equal(t, 1, len(pStorage.databaseMap[schemaID1].Tables))
		require.Equal(t, 0, len(pStorage.databaseMap[schemaID2].Tables))
		require.Equal(t, schemaID1, pStorage.tableMap[tableID].SchemaID)
		require.Equal(t, "t1", pStorage.tableMap[tableID].Name)
	}

	// rename table to a different db
	{
		job := &model.Job{
			Type:     model.ActionRenameTable,
			SchemaID: schemaID2,
			TableID:  tableID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 505,
				TableInfo: &model.TableInfo{
					ID:   tableID,
					Name: model.NewCIStr("t2"),
				},
				FinishedTS: 605,
			},
		}
		pStorage.handleDDLJob(job)
		require.Equal(t, 2, len(pStorage.databaseMap))
		require.Equal(t, 0, len(pStorage.databaseMap[schemaID1].Tables))
		require.Equal(t, 1, len(pStorage.databaseMap[schemaID2].Tables))
		require.Equal(t, schemaID2, pStorage.tableMap[tableID].SchemaID)
		require.Equal(t, "t2", pStorage.tableMap[tableID].Name)
	}

	{
		ddlEvents, err := pStorage.fetchTableDDLEvents(tableID, nil, 601, 700)
		require.Nil(t, err)
		require.Equal(t, 1, len(ddlEvents))
		// rename table event
		require.Equal(t, uint64(605), ddlEvents[0].FinishedTs)
		verifyTableIsBlocked(t, ddlEvents[0], tableID)
		verifyTableIsBlocked(t, ddlEvents[0], heartbeatpb.DDLSpan.TableID)

		require.Equal(t, tableID, ddlEvents[0].UpdatedSchemas[0].TableID)
		require.Equal(t, schemaID1, ddlEvents[0].UpdatedSchemas[0].OldSchemaID)
		require.Equal(t, schemaID2, ddlEvents[0].UpdatedSchemas[0].NewSchemaID)
	}

	// test filter: after rename, the table is filtered out
	{
		filterConfig := &config.FilterConfig{
			Rules: []string{"test.*"},
		}
		tableFilter, err := filter.NewFilter(filterConfig, "", false)
		require.Nil(t, err)
		ddlEvents, err := pStorage.fetchTableDDLEvents(tableID, tableFilter, 601, 700)
		require.Nil(t, err)
		require.Equal(t, 1, len(ddlEvents))
		verifyTableIsBlocked(t, ddlEvents[0], tableID)
		verifyTableIsBlocked(t, ddlEvents[0], heartbeatpb.DDLSpan.TableID)

		verifyTableIsDropped(t, ddlEvents[0], tableID)

		require.Nil(t, ddlEvents[0].NeedAddedTables)

		require.Equal(t, 0, len(ddlEvents[0].TableNameChange.AddName))
		require.Equal(t, "test", ddlEvents[0].TableNameChange.DropName[0].SchemaName)
		require.Equal(t, "t1", ddlEvents[0].TableNameChange.DropName[0].TableName)
	}

	// test filter: before rename, the table is filtered out, so only table trigger can get the event
	{
		filterConfig := &config.FilterConfig{
			Rules: []string{"test2.*"},
		}
		tableFilter, err := filter.NewFilter(filterConfig, "", false)
		require.Nil(t, err)
		triggerDDLEvents, err := pStorage.fetchTableTriggerDDLEvents(tableFilter, 601, 10)
		require.Nil(t, err)
		require.Equal(t, 1, len(triggerDDLEvents))
		require.Nil(t, triggerDDLEvents[0].BlockedTables)
		require.Nil(t, triggerDDLEvents[0].NeedDroppedTables)

		verifyTableIsAdded(t, triggerDDLEvents[0], tableID, schemaID2)

		require.Equal(t, "test2", triggerDDLEvents[0].TableNameChange.AddName[0].SchemaName)
		require.Equal(t, "t2", triggerDDLEvents[0].TableNameChange.AddName[0].TableName)
		require.Equal(t, 0, len(triggerDDLEvents[0].TableNameChange.DropName))
	}

	// test filter: the table is always filtered out
	{
		// check table trigger events cannot get the event
		filterConfig := &config.FilterConfig{
			Rules: []string{"test3.*"},
		}
		tableFilter, err := filter.NewFilter(filterConfig, "", false)
		require.Nil(t, err)
		triggerDDLEvents, err := pStorage.fetchTableTriggerDDLEvents(tableFilter, 601, 10)
		require.Nil(t, err)
		require.Equal(t, 0, len(triggerDDLEvents))
	}
}

func TestHandleRenamePartitionTable(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)

	gcTs := uint64(500)
	schemaID1 := int64(300)
	schemaID2 := int64(305)

	databaseInfo := make(map[int64]*model.DBInfo)
	databaseInfo[schemaID1] = &model.DBInfo{
		ID:   schemaID1,
		Name: model.NewCIStr("test"),
	}
	databaseInfo[schemaID2] = &model.DBInfo{
		ID:   schemaID2,
		Name: model.NewCIStr("test2"),
	}
	pStorage := newPersistentStorageForTest(dbPath, gcTs, databaseInfo)

	// create a table
	tableID := int64(100)
	partitionID1 := tableID + 100
	partitionID2 := tableID + 200
	partitionID3 := tableID + 300
	{
		job := &model.Job{
			Type:     model.ActionCreateTable,
			SchemaID: schemaID1,
			TableID:  tableID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 501,
				TableInfo: &model.TableInfo{
					ID:   tableID,
					Name: model.NewCIStr("t1"),
					Partition: &model.PartitionInfo{
						Definitions: []model.PartitionDefinition{
							{
								ID: partitionID1,
							},
							{
								ID: partitionID2,
							},
							{
								ID: partitionID3,
							},
						},
					},
				},
				FinishedTS: 601,
			},
		}
		pStorage.handleDDLJob(job)
	}

	// rename table to a different db
	{
		job := &model.Job{
			Type:     model.ActionRenameTable,
			SchemaID: schemaID2,
			TableID:  tableID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 505,
				TableInfo: &model.TableInfo{
					ID:   tableID,
					Name: model.NewCIStr("t2"),
					Partition: &model.PartitionInfo{
						Definitions: []model.PartitionDefinition{
							{
								ID: partitionID1,
							},
							{
								ID: partitionID2,
							},
							{
								ID: partitionID3,
							},
						},
					},
				},
				FinishedTS: 605,
			},
		}
		pStorage.handleDDLJob(job)
	}

	{
		ddlEvents, err := pStorage.fetchTableDDLEvents(partitionID1, nil, 601, 700)
		require.Nil(t, err)
		require.Equal(t, 1, len(ddlEvents))
		// rename table event
		require.Equal(t, uint64(605), ddlEvents[0].FinishedTs)
		verifyTableIsBlocked(t, ddlEvents[0], partitionID1)
		verifyTableIsBlocked(t, ddlEvents[0], partitionID2)
		verifyTableIsBlocked(t, ddlEvents[0], partitionID3)
		verifyTableIsBlocked(t, ddlEvents[0], heartbeatpb.DDLSpan.TableID)

		require.Equal(t, 3, len(ddlEvents[0].UpdatedSchemas))
	}

	// test filter: after rename, the table is filtered out
	{
		filterConfig := &config.FilterConfig{
			Rules: []string{"test.*"},
		}
		tableFilter, err := filter.NewFilter(filterConfig, "", false)
		require.Nil(t, err)
		ddlEvents, err := pStorage.fetchTableDDLEvents(partitionID1, tableFilter, 601, 700)
		require.Nil(t, err)
		require.Equal(t, 1, len(ddlEvents))
		verifyTableIsBlocked(t, ddlEvents[0], partitionID1)
		verifyTableIsBlocked(t, ddlEvents[0], partitionID2)
		verifyTableIsBlocked(t, ddlEvents[0], partitionID3)
		verifyTableIsBlocked(t, ddlEvents[0], heartbeatpb.DDLSpan.TableID)

		verifyTableIsDropped(t, ddlEvents[0], partitionID1)
		verifyTableIsDropped(t, ddlEvents[0], partitionID2)
		verifyTableIsDropped(t, ddlEvents[0], partitionID3)

		require.Nil(t, ddlEvents[0].NeedAddedTables)

		require.Equal(t, 0, len(ddlEvents[0].TableNameChange.AddName))
		require.Equal(t, "test", ddlEvents[0].TableNameChange.DropName[0].SchemaName)
		require.Equal(t, "t1", ddlEvents[0].TableNameChange.DropName[0].TableName)
	}

	// test filter: before rename, the table is filtered out, so only table trigger can get the event
	{
		filterConfig := &config.FilterConfig{
			Rules: []string{"test2.*"},
		}
		tableFilter, err := filter.NewFilter(filterConfig, "", false)
		require.Nil(t, err)
		triggerDDLEvents, err := pStorage.fetchTableTriggerDDLEvents(tableFilter, 601, 10)
		require.Nil(t, err)
		require.Equal(t, 1, len(triggerDDLEvents))
		require.Nil(t, triggerDDLEvents[0].BlockedTables)
		require.Nil(t, triggerDDLEvents[0].NeedDroppedTables)

		verifyTableIsAdded(t, triggerDDLEvents[0], partitionID1, schemaID2)
		verifyTableIsAdded(t, triggerDDLEvents[0], partitionID2, schemaID2)
		verifyTableIsAdded(t, triggerDDLEvents[0], partitionID3, schemaID2)

		require.Equal(t, "test2", triggerDDLEvents[0].TableNameChange.AddName[0].SchemaName)
		require.Equal(t, "t2", triggerDDLEvents[0].TableNameChange.AddName[0].TableName)
		require.Equal(t, 0, len(triggerDDLEvents[0].TableNameChange.DropName))
	}
}

func TestCreateTables(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)

	gcTs := uint64(500)
	schemaID := int64(300)

	databaseInfo := make(map[int64]*model.DBInfo)
	databaseInfo[schemaID] = &model.DBInfo{
		ID:   schemaID,
		Name: model.NewCIStr("test"),
	}
	pStorage := newPersistentStorageForTest(dbPath, gcTs, databaseInfo)

	// create tables
	tableID1 := int64(100)
	tableID2 := tableID1 + 100
	tableID3 := tableID1 + 200
	{
		job := &model.Job{
			Type:     model.ActionCreateTables,
			SchemaID: schemaID,
			Query:    "sql1;sql2;sql3",
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 501,
				MultipleTableInfos: []*model.TableInfo{
					{
						ID:   tableID1,
						Name: model.NewCIStr("t1"),
					},
					{
						ID:   tableID2,
						Name: model.NewCIStr("t2"),
					},
					{
						ID:   tableID3,
						Name: model.NewCIStr("t3"),
					},
				},
				FinishedTS: 601,
			},
		}
		pStorage.handleDDLJob(job)
	}

	{
		require.Equal(t, 3, len(pStorage.databaseMap[schemaID].Tables))
		require.Equal(t, 3, len(pStorage.tableMap))
	}

	{
		store := newEmptyVersionedTableInfoStore(tableID1)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 1, len(store.infos))
		require.Equal(t, "t1", store.infos[0].info.Name.O)
	}

	{
		store := newEmptyVersionedTableInfoStore(tableID2)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 1, len(store.infos))
		require.Equal(t, "t2", store.infos[0].info.Name.O)
	}

	{
		store := newEmptyVersionedTableInfoStore(tableID3)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 1, len(store.infos))
		require.Equal(t, "t3", store.infos[0].info.Name.O)
	}

	{
		ddlEvents, err := pStorage.fetchTableTriggerDDLEvents(nil, 600, 601)
		require.Nil(t, err)
		require.Equal(t, 1, len(ddlEvents))

		verifyTableIsAdded(t, ddlEvents[0], tableID1, schemaID)
		verifyTableIsAdded(t, ddlEvents[0], tableID2, schemaID)
		verifyTableIsAdded(t, ddlEvents[0], tableID3, schemaID)
	}

	// filter t2 and t3
	{
		filterConfig := &config.FilterConfig{
			Rules: []string{"test.t1"},
		}
		tableFilter, err := filter.NewFilter(filterConfig, "", false)
		require.Nil(t, err)
		ddlEvents, err := pStorage.fetchTableTriggerDDLEvents(tableFilter, 600, 601)
		require.Nil(t, err)
		require.Equal(t, 1, len(ddlEvents))

		verifyTableIsAdded(t, ddlEvents[0], tableID1, schemaID)
		require.Equal(t, 1, len(ddlEvents[0].NeedAddedTables))
	}

	// all filtered out
	{
		filterConfig := &config.FilterConfig{
			Rules: []string{"test2.*"},
		}
		tableFilter, err := filter.NewFilter(filterConfig, "", false)
		require.Nil(t, err)
		ddlEvents, err := pStorage.fetchTableTriggerDDLEvents(tableFilter, 600, 601)
		require.Nil(t, err)
		require.Equal(t, 0, len(ddlEvents))
	}
}

func TestCreateTablesForPartitionTable(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)

	gcTs := uint64(500)
	schemaID := int64(300)

	databaseInfo := make(map[int64]*model.DBInfo)
	databaseInfo[schemaID] = &model.DBInfo{
		ID:   schemaID,
		Name: model.NewCIStr("test"),
	}
	pStorage := newPersistentStorageForTest(dbPath, gcTs, databaseInfo)

	// create tables
	tableID1 := int64(100)
	tableID2 := tableID1 + 100
	tableID3 := tableID1 + 200
	partitionID1 := tableID1 + 1000
	partitionID2 := partitionID1 + 100
	partitionID3 := partitionID1 + 200
	partitionID4 := partitionID1 + 300
	partitionID5 := partitionID1 + 400
	partitionID6 := partitionID1 + 500
	{
		job := &model.Job{
			Type:     model.ActionCreateTables,
			SchemaID: schemaID,
			Query:    "sql1;sql2;sql3",
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 501,
				MultipleTableInfos: []*model.TableInfo{
					{
						ID:   tableID1,
						Name: model.NewCIStr("t1"),
						Partition: &model.PartitionInfo{
							Definitions: []model.PartitionDefinition{
								{
									ID: partitionID1,
								},
								{
									ID: partitionID2,
								},
							},
						},
					},
					{
						ID:   tableID2,
						Name: model.NewCIStr("t2"),
						Partition: &model.PartitionInfo{
							Definitions: []model.PartitionDefinition{
								{
									ID: partitionID3,
								},
								{
									ID: partitionID4,
								},
							},
						},
					},
					{
						ID:   tableID3,
						Name: model.NewCIStr("t3"),
						Partition: &model.PartitionInfo{
							Definitions: []model.PartitionDefinition{
								{
									ID: partitionID5,
								},
								{
									ID: partitionID6,
								},
							},
						},
					},
				},
				FinishedTS: 601,
			},
		}
		pStorage.handleDDLJob(job)
	}

	{
		require.Equal(t, 3, len(pStorage.databaseMap[schemaID].Tables))
		require.Equal(t, 3, len(pStorage.tableMap))
		require.Equal(t, 3, len(pStorage.partitionMap))
	}

	{
		store := newEmptyVersionedTableInfoStore(partitionID1)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 1, len(store.infos))
		require.Equal(t, "t1", store.infos[0].info.Name.O)
	}

	{
		store := newEmptyVersionedTableInfoStore(partitionID2)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 1, len(store.infos))
		require.Equal(t, "t1", store.infos[0].info.Name.O)
	}

	{
		store := newEmptyVersionedTableInfoStore(partitionID3)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 1, len(store.infos))
		require.Equal(t, "t2", store.infos[0].info.Name.O)
	}

	{
		store := newEmptyVersionedTableInfoStore(partitionID4)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 1, len(store.infos))
		require.Equal(t, "t2", store.infos[0].info.Name.O)
	}

	{
		store := newEmptyVersionedTableInfoStore(partitionID5)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 1, len(store.infos))
		require.Equal(t, "t3", store.infos[0].info.Name.O)
	}

	{
		store := newEmptyVersionedTableInfoStore(partitionID6)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 1, len(store.infos))
		require.Equal(t, "t3", store.infos[0].info.Name.O)
	}

	{
		ddlEvents, err := pStorage.fetchTableTriggerDDLEvents(nil, 600, 601)
		require.Nil(t, err)
		require.Equal(t, 1, len(ddlEvents))

		verifyTableIsAdded(t, ddlEvents[0], partitionID1, schemaID)
		verifyTableIsAdded(t, ddlEvents[0], partitionID2, schemaID)
		verifyTableIsAdded(t, ddlEvents[0], partitionID3, schemaID)
		verifyTableIsAdded(t, ddlEvents[0], partitionID4, schemaID)
		verifyTableIsAdded(t, ddlEvents[0], partitionID5, schemaID)
		verifyTableIsAdded(t, ddlEvents[0], partitionID6, schemaID)
	}

	// filter t2 and t3
	{
		filterConfig := &config.FilterConfig{
			Rules: []string{"test.t1"},
		}
		tableFilter, err := filter.NewFilter(filterConfig, "", false)
		require.Nil(t, err)
		ddlEvents, err := pStorage.fetchTableTriggerDDLEvents(tableFilter, 600, 601)
		require.Nil(t, err)
		require.Equal(t, 1, len(ddlEvents))

		verifyTableIsAdded(t, ddlEvents[0], partitionID1, schemaID)
		verifyTableIsAdded(t, ddlEvents[0], partitionID2, schemaID)
		require.Equal(t, 2, len(ddlEvents[0].NeedAddedTables))
	}

	// all filtered out
	{
		filterConfig := &config.FilterConfig{
			Rules: []string{"test2.*"},
		}
		tableFilter, err := filter.NewFilter(filterConfig, "", false)
		require.Nil(t, err)
		ddlEvents, err := pStorage.fetchTableTriggerDDLEvents(tableFilter, 600, 601)
		require.Nil(t, err)
		require.Equal(t, 0, len(ddlEvents))
	}
}

func TestFetchDDLEventsBasic(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)
	pStorage := newEmptyPersistentStorageForTest(dbPath)

	// create db
	schemaID := int64(300)
	schemaName := "test"
	{
		ddlEvent := &model.Job{
			Type:     model.ActionCreateSchema,
			SchemaID: schemaID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 100,
				DBInfo: &model.DBInfo{
					ID:   schemaID,
					Name: model.NewCIStr(schemaName),
				},
				TableInfo:  nil,
				FinishedTS: 200,
			},
		}
		pStorage.handleDDLJob(ddlEvent)
	}

	// create a table
	tableID := int64(100)
	{
		ddlEvent := &model.Job{
			Type:     model.ActionCreateTable,
			SchemaID: schemaID,
			TableID:  tableID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 501,
				TableInfo: &model.TableInfo{
					ID:   tableID,
					Name: model.NewCIStr("t1"),
				},
				FinishedTS: 601,
			},
		}
		pStorage.handleDDLJob(ddlEvent)
	}

	// rename table
	{
		ddlEvent := &model.Job{
			Type:     model.ActionRenameTable,
			SchemaID: schemaID,
			TableID:  tableID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 505,
				TableInfo: &model.TableInfo{
					ID:   tableID,
					Name: model.NewCIStr("t2"),
				},
				FinishedTS: 605,
			},
		}
		pStorage.handleDDLJob(ddlEvent)
	}

	// truncate table
	tableID2 := int64(105)
	{
		ddlEvent := &model.Job{
			Type:     model.ActionTruncateTable,
			SchemaID: schemaID,
			TableID:  tableID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 507,
				TableInfo: &model.TableInfo{
					ID:   tableID2,
					Name: model.NewCIStr("t2"),
				},
				FinishedTS: 607,
			},
		}
		pStorage.handleDDLJob(ddlEvent)
	}

	// create another table
	tableID3 := int64(200)
	{
		ddlEvent := &model.Job{
			Type:     model.ActionCreateTable,
			SchemaID: schemaID,
			TableID:  tableID3,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 509,
				TableInfo: &model.TableInfo{
					ID:   tableID,
					Name: model.NewCIStr("t3"),
				},
				FinishedTS: 609,
			},
		}
		pStorage.handleDDLJob(ddlEvent)
	}

	// drop newly created table
	{
		ddlEvent := &model.Job{
			Type:     model.ActionDropTable,
			SchemaID: schemaID,
			TableID:  tableID3,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 511,
				TableInfo: &model.TableInfo{
					ID:   tableID,
					Name: model.NewCIStr("t3"),
				},
				FinishedTS: 611,
			},
		}
		pStorage.handleDDLJob(ddlEvent)
	}

	// drop db
	{
		ddlEvent := &model.Job{
			Type:     model.ActionDropSchema,
			SchemaID: schemaID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 600,
				DBInfo: &model.DBInfo{
					ID:   schemaID,
					Name: model.NewCIStr(schemaName),
				},
				TableInfo:  nil,
				FinishedTS: 700,
			},
		}
		pStorage.handleDDLJob(ddlEvent)
	}

	// fetch table ddl events
	{
		ddlEvents, err := pStorage.fetchTableDDLEvents(tableID, nil, 601, 700)
		require.Nil(t, err)
		require.Equal(t, 2, len(ddlEvents))
		// rename table event
		require.Equal(t, uint64(605), ddlEvents[0].FinishedTs)
		// truncate table event
		require.Equal(t, uint64(607), ddlEvents[1].FinishedTs)
		require.Equal(t, "test", ddlEvents[1].SchemaName)
		require.Equal(t, "t2", ddlEvents[1].TableName)
		require.Equal(t, common.InfluenceTypeNormal, ddlEvents[1].NeedDroppedTables.InfluenceType)
		require.Equal(t, 1, len(ddlEvents[1].NeedDroppedTables.TableIDs))
		require.Equal(t, tableID, ddlEvents[1].NeedDroppedTables.TableIDs[0])
		require.Equal(t, 1, len(ddlEvents[1].NeedAddedTables))
		require.Equal(t, schemaID, ddlEvents[1].NeedAddedTables[0].SchemaID)
		require.Equal(t, tableID2, ddlEvents[1].NeedAddedTables[0].TableID)
	}

	// fetch table ddl events for another table
	{
		// TODO: test return error if start ts is smaller than 607
		ddlEvents, err := pStorage.fetchTableDDLEvents(tableID2, nil, 607, 700)
		require.Nil(t, err)
		require.Equal(t, 1, len(ddlEvents))
		// drop db event
		require.Equal(t, uint64(700), ddlEvents[0].FinishedTs)
		require.Equal(t, common.InfluenceTypeDB, ddlEvents[0].NeedDroppedTables.InfluenceType)
		require.Equal(t, schemaID, ddlEvents[0].NeedDroppedTables.SchemaID)
	}

	// fetch table ddl events again
	{
		ddlEvents, err := pStorage.fetchTableDDLEvents(tableID3, nil, 609, 700)
		require.Nil(t, err)
		require.Equal(t, 1, len(ddlEvents))
		// drop table event
		require.Equal(t, uint64(611), ddlEvents[0].FinishedTs)
		require.Equal(t, common.InfluenceTypeNormal, ddlEvents[0].NeedDroppedTables.InfluenceType)
		require.Equal(t, 1, len(ddlEvents[0].NeedDroppedTables.TableIDs))
		require.Equal(t, tableID3, ddlEvents[0].NeedDroppedTables.TableIDs[0])
	}

	// fetch all table trigger ddl events
	{
		tableTriggerDDLEvents, err := pStorage.fetchTableTriggerDDLEvents(nil, 0, 10)
		require.Nil(t, err)
		require.Equal(t, 6, len(tableTriggerDDLEvents))
		// create db event
		require.Equal(t, uint64(200), tableTriggerDDLEvents[0].FinishedTs)
		// create table event
		require.Equal(t, uint64(601), tableTriggerDDLEvents[1].FinishedTs)
		require.Equal(t, 1, len(tableTriggerDDLEvents[1].NeedAddedTables))
		require.Equal(t, schemaID, tableTriggerDDLEvents[1].NeedAddedTables[0].SchemaID)
		require.Equal(t, tableID, tableTriggerDDLEvents[1].NeedAddedTables[0].TableID)
		require.Equal(t, schemaName, tableTriggerDDLEvents[1].TableNameChange.AddName[0].SchemaName)
		require.Equal(t, "t1", tableTriggerDDLEvents[1].TableNameChange.AddName[0].TableName)
		// rename table event
		require.Equal(t, uint64(605), tableTriggerDDLEvents[2].FinishedTs)
		// create table event
		require.Equal(t, uint64(609), tableTriggerDDLEvents[3].FinishedTs)
		// drop table event
		require.Equal(t, uint64(611), tableTriggerDDLEvents[4].FinishedTs)
		require.Equal(t, common.InfluenceTypeNormal, tableTriggerDDLEvents[4].NeedDroppedTables.InfluenceType)
		require.Equal(t, tableID3, tableTriggerDDLEvents[4].NeedDroppedTables.TableIDs[0])
		require.Equal(t, schemaName, tableTriggerDDLEvents[4].TableNameChange.DropName[0].SchemaName)
		require.Equal(t, "t3", tableTriggerDDLEvents[4].TableNameChange.DropName[0].TableName)
		require.Equal(t, common.InfluenceTypeNormal, tableTriggerDDLEvents[4].BlockedTables.InfluenceType)
		require.Equal(t, 2, len(tableTriggerDDLEvents[4].BlockedTables.TableIDs))
		require.Equal(t, tableID3, tableTriggerDDLEvents[4].BlockedTables.TableIDs[0])
		// TODO: don't count on the order
		require.Equal(t, heartbeatpb.DDLSpan.TableID, tableTriggerDDLEvents[4].BlockedTables.TableIDs[1])
		// drop db event
		require.Equal(t, uint64(700), tableTriggerDDLEvents[5].FinishedTs)
		require.Equal(t, schemaName, tableTriggerDDLEvents[5].TableNameChange.DropDatabaseName)
	}

	// fetch partial table trigger ddl events
	{
		tableTriggerDDLEvents, err := pStorage.fetchTableTriggerDDLEvents(nil, 0, 2)
		require.Nil(t, err)
		require.Equal(t, 2, len(tableTriggerDDLEvents))
		require.Equal(t, uint64(200), tableTriggerDDLEvents[0].FinishedTs)
		require.Equal(t, uint64(601), tableTriggerDDLEvents[1].FinishedTs)
	}

	// TODO: test filter
}

func TestGCPersistStorage(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)

	schemaID := int64(300)
	gcTs := uint64(600)
	tableID1 := int64(100)
	tableID2 := int64(200)

	databaseInfo := make(map[int64]*model.DBInfo)
	databaseInfo[schemaID] = &model.DBInfo{
		ID:   schemaID,
		Name: model.NewCIStr("test"),
		Tables: []*model.TableInfo{
			{
				ID:   tableID1,
				Name: model.NewCIStr("t1"),
			},
			{
				ID:   tableID2,
				Name: model.NewCIStr("t2"),
			},
		},
	}
	pStorage := newPersistentStorageForTest(dbPath, gcTs, databaseInfo)

	// create table t3
	tableID3 := int64(500)
	{
		ddlEvent := &model.Job{
			Type:     model.ActionCreateTable,
			SchemaID: schemaID,
			TableID:  tableID3,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 501,
				TableInfo: &model.TableInfo{
					ID:   tableID3,
					Name: model.NewCIStr("t3"),
				},
				FinishedTS: 602,
			},
		}
		pStorage.handleDDLJob(ddlEvent)
	}

	// drop table t2
	{
		ddlEvent := &model.Job{
			Type:     model.ActionDropTable,
			SchemaID: schemaID,
			TableID:  tableID2,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 503,
				TableInfo:     nil,
				FinishedTS:    603,
			},
		}
		pStorage.handleDDLJob(ddlEvent)
	}

	// rename table t1
	{
		ddlEvent := &model.Job{
			Type:     model.ActionRenameTable,
			SchemaID: schemaID,
			TableID:  tableID1,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 505,
				TableInfo: &model.TableInfo{
					ID:   tableID1,
					Name: model.NewCIStr("t1_r"),
				},
				FinishedTS: 605,
			},
		}
		pStorage.handleDDLJob(ddlEvent)
	}

	// write upper bound
	newUpperBound := UpperBoundMeta{
		FinishedDDLTs: 700,
		SchemaVersion: 509,
		ResolvedTs:    705,
	}
	{
		writeUpperBoundMeta(pStorage.db, newUpperBound)
	}

	// mock gc
	newGcTs1 := uint64(601)
	{
		databaseInfo := make(map[int64]*model.DBInfo)
		databaseInfo[schemaID] = &model.DBInfo{
			ID:   schemaID,
			Name: model.NewCIStr("test"),
			Tables: []*model.TableInfo{
				{
					ID:   tableID1,
					Name: model.NewCIStr("t1"),
				},
				{
					ID:   tableID2,
					Name: model.NewCIStr("t2"),
				},
			},
		}
		mockWriteKVSnapOnDisk(pStorage.db, newGcTs1, databaseInfo)

		require.Equal(t, 3, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, 3, len(pStorage.tablesDDLHistory))
		pStorage.cleanObsoleteDataInMemory(newGcTs1)
		require.Equal(t, 3, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, 3, len(pStorage.tablesDDLHistory))
	}

	// mock gc again with a register table
	pStorage.registerTable(tableID1, newGcTs1+1)
	newGcTs2 := uint64(603)
	{
		databaseInfo := make(map[int64]*model.DBInfo)
		databaseInfo[schemaID] = &model.DBInfo{
			ID:   schemaID,
			Name: model.NewCIStr("test"),
			Tables: []*model.TableInfo{
				{
					ID:   tableID1,
					Name: model.NewCIStr("t1"),
				},
				{
					ID:   tableID3,
					Name: model.NewCIStr("t3"),
				},
			},
		}
		mockWriteKVSnapOnDisk(pStorage.db, newGcTs2, databaseInfo)

		require.Equal(t, 3, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, 3, len(pStorage.tablesDDLHistory))
		pStorage.cleanObsoleteDataInMemory(newGcTs2)
		require.Equal(t, 1, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, uint64(605), pStorage.tableTriggerDDLHistory[0])
		require.Equal(t, 1, len(pStorage.tablesDDLHistory))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[tableID1]))
		tableInfoT1, err := pStorage.getTableInfo(tableID1, newGcTs2)
		require.Nil(t, err)
		require.Equal(t, "t1", tableInfoT1.Name.O)
		tableInfoT1, err = pStorage.getTableInfo(tableID1, 606)
		require.Nil(t, err)
		require.Equal(t, "t1_r", tableInfoT1.Name.O)
	}

	pStorage = loadPersistentStorageForTest(pStorage.db, newGcTs2, newUpperBound)
	{
		require.Equal(t, newGcTs2, pStorage.gcTs)
		require.Equal(t, newUpperBound, pStorage.upperBound)
		require.Equal(t, 1, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, uint64(605), pStorage.tableTriggerDDLHistory[0])
		require.Equal(t, 1, len(pStorage.tablesDDLHistory))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[tableID1]))
	}

	// TODO: test obsolete data can be removed
}

func TestGetAllPhysicalTables(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)

	schemaID := int64(300)
	gcTs := uint64(600)
	tableID1 := int64(100)
	tableID2 := tableID1 + 100

	databaseInfo := make(map[int64]*model.DBInfo)
	databaseInfo[schemaID] = &model.DBInfo{
		ID:   schemaID,
		Name: model.NewCIStr("test"),
		Tables: []*model.TableInfo{
			{
				ID:   tableID1,
				Name: model.NewCIStr("t1"),
			},
			{
				ID:   tableID2,
				Name: model.NewCIStr("t2"),
			},
		},
	}
	pStorage := newPersistentStorageForTest(dbPath, gcTs, databaseInfo)

	// create table t3
	tableID3 := tableID2 + 100
	{
		job := &model.Job{
			Type:     model.ActionCreateTable,
			SchemaID: schemaID,
			TableID:  tableID3,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 501,
				TableInfo: &model.TableInfo{
					ID:   tableID3,
					Name: model.NewCIStr("t3"),
				},

				FinishedTS: 601,
			},
		}
		pStorage.handleDDLJob(job)
	}

	// drop table t2
	{
		job := &model.Job{
			Type:     model.ActionDropTable,
			SchemaID: schemaID,
			TableID:  tableID2,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 503,
				TableInfo:     nil,
				FinishedTS:    603,
			},
		}
		pStorage.handleDDLJob(job)
	}

	// create partition table t4
	tableID4 := tableID3 + 100
	partitionID1 := tableID4 + 100
	partitionID2 := tableID4 + 200
	partitionID3 := tableID4 + 300
	{
		job := &model.Job{
			Type:     model.ActionCreateTable,
			SchemaID: schemaID,
			TableID:  tableID4,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 503,
				TableInfo: &model.TableInfo{
					ID:   tableID4,
					Name: model.NewCIStr("t4"),
					Partition: &model.PartitionInfo{
						Definitions: []model.PartitionDefinition{
							{
								ID: partitionID1,
							},
							{
								ID: partitionID2,
							},
							{
								ID: partitionID3,
							},
						},
					},
				},
				FinishedTS: 609,
			},
		}
		pStorage.handleDDLJob(job)
	}

	// drop partition table t4
	{
		job := &model.Job{
			Type:     model.ActionDropTable,
			SchemaID: schemaID,
			TableID:  tableID4,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 505,
				TableInfo: &model.TableInfo{
					ID:   tableID4,
					Name: model.NewCIStr("t4"),
					Partition: &model.PartitionInfo{
						Definitions: []model.PartitionDefinition{
							{
								ID: partitionID1,
							},
							{
								ID: partitionID2,
							},
							{
								ID: partitionID3,
							},
						},
					},
				},
				FinishedTS: 611,
			},
		}
		pStorage.handleDDLJob(job)
	}

	{
		allPhysicalTables, err := pStorage.getAllPhysicalTables(600, nil)
		require.Nil(t, err)
		require.Equal(t, 2, len(allPhysicalTables))
	}

	{
		allPhysicalTables, err := pStorage.getAllPhysicalTables(601, nil)
		require.Nil(t, err)
		require.Equal(t, 3, len(allPhysicalTables))
	}

	{
		allPhysicalTables, err := pStorage.getAllPhysicalTables(603, nil)
		require.Nil(t, err)
		require.Equal(t, 2, len(allPhysicalTables))
	}

	{
		allPhysicalTables, err := pStorage.getAllPhysicalTables(605, nil)
		require.Nil(t, err)
		require.Equal(t, 2, len(allPhysicalTables))
	}

	{
		allPhysicalTables, err := pStorage.getAllPhysicalTables(609, nil)
		require.Nil(t, err)
		require.Equal(t, 5, len(allPhysicalTables))
	}

	{
		allPhysicalTables, err := pStorage.getAllPhysicalTables(611, nil)
		require.Nil(t, err)
		require.Equal(t, 2, len(allPhysicalTables))
	}
}
