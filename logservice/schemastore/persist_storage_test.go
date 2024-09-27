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
		databaseMap:            make(map[int64]*BasicDatabaseInfo),
		tablesDDLHistory:       make(map[int64][]uint64),
		tableTriggerDDLHistory: make([]uint64, 0),
		tableInfoStoreMap:      make(map[int64]*versionedTableInfoStore),
		tableRegisteredCount:   make(map[int64]int),
	}
	p.initializeFromDisk()
	return p
}

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
		store.applyDDL(PersistedDDLEvent{
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
		require.Equal(t, "test2", ddlEvents[0].SchemaName)
		require.Equal(t, "t2", ddlEvents[0].TableName)
		require.Equal(t, common.InfluenceTypeNormal, ddlEvents[0].BlockedTables.InfluenceType)
		require.Equal(t, schemaID1, ddlEvents[0].BlockedTables.SchemaID)
		require.Equal(t, tableID, ddlEvents[0].BlockedTables.TableIDs[0])
		// TODO: don't count on the order
		require.Equal(t, heartbeatpb.DDLSpan.TableID, ddlEvents[0].BlockedTables.TableIDs[1])
		require.Equal(t, tableID, ddlEvents[0].NeedDroppedTables.TableIDs[0])

		require.Equal(t, tableID, ddlEvents[0].NeedAddedTables[0].TableID)

		require.Equal(t, "test2", ddlEvents[0].TableNameChange.AddName[0].SchemaName)
		require.Equal(t, "t2", ddlEvents[0].TableNameChange.AddName[0].TableName)
		require.Equal(t, "test", ddlEvents[0].TableNameChange.DropName[0].SchemaName)
		require.Equal(t, "t1", ddlEvents[0].TableNameChange.DropName[0].TableName)
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
		require.Equal(t, common.InfluenceTypeNormal, ddlEvents[0].BlockedTables.InfluenceType)
		require.Equal(t, schemaID1, ddlEvents[0].BlockedTables.SchemaID)
		require.Equal(t, tableID, ddlEvents[0].BlockedTables.TableIDs[0])
		// TODO: don't count on the order
		require.Equal(t, heartbeatpb.DDLSpan.TableID, ddlEvents[0].BlockedTables.TableIDs[1])
		require.Equal(t, tableID, ddlEvents[0].NeedDroppedTables.TableIDs[0])

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

		require.Equal(t, tableID, triggerDDLEvents[0].NeedAddedTables[0].TableID)

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
		require.Equal(t, schemaID, ddlEvents[1].NeedDroppedTables.SchemaID)
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
		require.Equal(t, schemaID, ddlEvents[0].NeedDroppedTables.SchemaID)
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
		require.Equal(t, schemaID, tableTriggerDDLEvents[4].NeedDroppedTables.SchemaID)
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
		pStorage.cleanObseleteDataInMemory(newGcTs1)
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
		pStorage.cleanObseleteDataInMemory(newGcTs2)
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
}
