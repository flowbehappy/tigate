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
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func loadPersistentStorageForTest(db *pebble.DB, gcTs uint64, upperBound upperBoundMeta) *persistentStorage {
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
	upperBound := upperBoundMeta{
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
	upperBound := upperBoundMeta{
		FinishedDDLTs: 0,
		SchemaVersion: 0,
		ResolvedTs:    gcTs,
	}
	writeUpperBoundMeta(db, upperBound)
	return loadPersistentStorageForTest(db, gcTs, upperBound)
}

func mockWriteKVSnapOnDisk(db *pebble.DB, snapTs uint64, dbInfos map[int64]*model.DBInfo) map[int64]*BasicTableInfo {
	batch := db.NewBatch()
	defer batch.Close()
	tablesInKVSnap := make(map[int64]*BasicTableInfo)
	for _, dbInfo := range dbInfos {
		writeSchemaInfoToBatch(batch, snapTs, dbInfo)
		for _, tableInfo := range dbInfo.Tables {
			tablesInKVSnap[tableInfo.ID] = &BasicTableInfo{
				SchemaID: dbInfo.ID,
				Name:     tableInfo.Name.O,
				InKVSnap: true,
			}
			tableInfoValue, err := json.Marshal(tableInfo)
			if err != nil {
				log.Panic("marshal table info fail", zap.Error(err))
			}
			writeTableInfoToBatch(batch, snapTs, dbInfo.ID, tableInfoValue)
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		log.Panic("commit batch fail", zap.Error(err))
	}
	writeGcTs(db, snapTs)
	return tablesInKVSnap
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
		upperBound := upperBoundMeta{
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
		upperBound := upperBoundMeta{
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
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionRenameTable),
			SchemaID:      schemaID,
			TableID:       tableID,
			SchemaVersion: 3000,
			TableInfo: &model.TableInfo{
				ID:   tableID,
				Name: model.NewCIStr("t2"),
			},
			FinishedTs: renameVersion,
		}
		err = pStorage.handleSortedDDLEvents(ddlEvent)
		require.Nil(t, err)
	}

	upperBound := upperBoundMeta{
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
			Type:          byte(model.ActionRenameTable),
			SchemaID:      schemaID,
			TableID:       tableID,
			SchemaVersion: 3000,
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
}

func TestHandleCreateDropSchemaTableDDL(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)
	pStorage := newEmptyPersistentStorageForTest(dbPath)

	// create db
	schemaID := int64(300)
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionCreateSchema),
			SchemaID:      schemaID,
			SchemaVersion: 100,
			DBInfo: &model.DBInfo{
				ID:   schemaID,
				Name: model.NewCIStr("test"),
			},
			TableInfo:  nil,
			FinishedTs: 200,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)

		require.Equal(t, 1, len(pStorage.databaseMap))
		require.Equal(t, "test", pStorage.databaseMap[schemaID].Name)
		require.Equal(t, 1, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, uint64(200), pStorage.tableTriggerDDLHistory[0])
	}

	// create a table
	tableID := int64(100)
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionCreateTable),
			SchemaID:      schemaID,
			TableID:       tableID,
			SchemaVersion: 101,
			TableInfo: &model.TableInfo{
				Name: model.NewCIStr("t1"),
			},
			FinishedTs: 201,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)

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
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionCreateTable),
			SchemaID:      schemaID,
			TableID:       tableID2,
			SchemaVersion: 103,
			TableInfo: &model.TableInfo{
				Name: model.NewCIStr("t2"),
			},
			FinishedTs: 203,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)

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
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionDropTable),
			SchemaID:      schemaID,
			TableID:       tableID2,
			SchemaVersion: 105,
			TableInfo:     nil,
			FinishedTs:    205,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)

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
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionTruncateTable),
			SchemaID:      schemaID,
			TableID:       tableID,
			SchemaVersion: 107,
			TableInfo: &model.TableInfo{
				ID: tableID3,
			},
			FinishedTs: 207,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)

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
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionDropSchema),
			SchemaID:      schemaID,
			SchemaVersion: 200,
			DBInfo: &model.DBInfo{
				ID:   schemaID,
				Name: model.NewCIStr("test"),
			},
			TableInfo:  nil,
			FinishedTs: 300,
		}

		pStorage.handleSortedDDLEvents(ddlEvent)

		require.Equal(t, 0, len(pStorage.databaseMap))
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
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionCreateTable),
			SchemaID:      schemaID1,
			TableID:       tableID,
			SchemaVersion: 501,
			TableInfo: &model.TableInfo{
				ID:   tableID,
				Name: model.NewCIStr("t1"),
			},
			FinishedTs: 601,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
		require.Equal(t, 2, len(pStorage.databaseMap))
		require.Equal(t, 1, len(pStorage.databaseMap[schemaID1].Tables))
		require.Equal(t, 0, len(pStorage.databaseMap[schemaID2].Tables))
		require.Equal(t, false, pStorage.tableMap[tableID].InKVSnap)
		require.Equal(t, schemaID1, pStorage.tableMap[tableID].SchemaID)
		require.Equal(t, "t1", pStorage.tableMap[tableID].Name)
	}

	// rename table
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionRenameTable),
			SchemaID:      schemaID2,
			TableID:       tableID,
			SchemaVersion: 505,
			TableInfo: &model.TableInfo{
				ID:   tableID,
				Name: model.NewCIStr("t2"),
			},
			FinishedTs: 605,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
		require.Equal(t, 2, len(pStorage.databaseMap))
		require.Equal(t, 0, len(pStorage.databaseMap[schemaID1].Tables))
		require.Equal(t, 1, len(pStorage.databaseMap[schemaID2].Tables))
		require.Equal(t, false, pStorage.tableMap[tableID].InKVSnap)
		require.Equal(t, schemaID2, pStorage.tableMap[tableID].SchemaID)
		require.Equal(t, "t2", pStorage.tableMap[tableID].Name)
	}
}

func TestFetchDDLEvents(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)
	pStorage := newEmptyPersistentStorageForTest(dbPath)

	// create db
	schemaID := int64(300)
	schemaName := "test"
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionCreateSchema),
			SchemaID:      schemaID,
			SchemaVersion: 100,
			DBInfo: &model.DBInfo{
				ID:   schemaID,
				Name: model.NewCIStr(schemaName),
			},
			TableInfo:  nil,
			FinishedTs: 200,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
	}

	// create a table
	tableID := int64(100)
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionCreateTable),
			SchemaID:      schemaID,
			TableID:       tableID,
			SchemaVersion: 501,
			TableInfo: &model.TableInfo{
				ID:   tableID,
				Name: model.NewCIStr("t1"),
			},
			FinishedTs: 601,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
	}

	// rename table
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionRenameTable),
			SchemaID:      schemaID,
			TableID:       tableID,
			SchemaVersion: 505,
			TableInfo: &model.TableInfo{
				ID:   tableID,
				Name: model.NewCIStr("t2"),
			},
			FinishedTs: 605,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
	}

	// truncate table
	tableID2 := int64(105)
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionTruncateTable),
			SchemaID:      schemaID,
			TableID:       tableID,
			SchemaVersion: 507,
			TableInfo: &model.TableInfo{
				ID:   tableID2,
				Name: model.NewCIStr("t2"),
			},
			FinishedTs: 607,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
	}

	// create another table
	tableID3 := int64(200)
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionCreateTable),
			SchemaID:      schemaID,
			TableID:       tableID3,
			SchemaVersion: 509,
			TableInfo: &model.TableInfo{
				ID:   tableID,
				Name: model.NewCIStr("t3"),
			},
			FinishedTs: 609,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
	}

	// drop newly created table
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionDropTable),
			SchemaID:      schemaID,
			TableID:       tableID3,
			SchemaVersion: 511,
			TableInfo: &model.TableInfo{
				ID:   tableID,
				Name: model.NewCIStr("t3"),
			},
			FinishedTs: 611,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
	}

	// drop db
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionDropSchema),
			SchemaID:      schemaID,
			SchemaVersion: 600,
			DBInfo: &model.DBInfo{
				ID:   schemaID,
				Name: model.NewCIStr(schemaName),
			},
			TableInfo:  nil,
			FinishedTs: 700,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
	}

	// fetch table ddl events
	{
		ddlEvents, err := pStorage.fetchTableDDLEvents(tableID, 601, 700)
		require.Nil(t, err)
		require.Equal(t, 2, len(ddlEvents))
		// rename table event
		require.Equal(t, uint64(605), ddlEvents[0].FinishedTs)
		// truncate table event
		require.Equal(t, uint64(607), ddlEvents[1].FinishedTs)
		require.Equal(t, "test", ddlEvents[1].SchemaName)
		require.Equal(t, "t2", ddlEvents[1].TableName)
		require.Equal(t, common.Normal, ddlEvents[1].NeedDroppedTables.InfluenceType)
		require.Equal(t, schemaID, ddlEvents[1].NeedDroppedTables.SchemaID)
		require.Equal(t, "test", ddlEvents[1].NeedDroppedTables.SchemaName)
		require.Equal(t, 1, len(ddlEvents[1].NeedDroppedTables.TableIDs))
		require.Equal(t, tableID, ddlEvents[1].NeedDroppedTables.TableIDs[0])
		require.Equal(t, 1, len(ddlEvents[1].NeedDroppedTables.TableNames))
		require.Equal(t, "t2", ddlEvents[1].NeedDroppedTables.TableNames[0])
		require.Equal(t, 1, len(ddlEvents[1].NeedAddedTables))
		require.Equal(t, schemaID, ddlEvents[1].NeedAddedTables[0].SchemaID)
		require.Equal(t, "test", ddlEvents[1].NeedAddedTables[0].SchemaName)
		require.Equal(t, tableID2, ddlEvents[1].NeedAddedTables[0].TableID)
		require.Equal(t, "t2", ddlEvents[1].NeedAddedTables[0].TableName)
	}

	// fetch table ddl events for another table
	{
		// TODO: test return error if start ts is smaller than 607
		ddlEvents, err := pStorage.fetchTableDDLEvents(tableID2, 607, 700)
		require.Nil(t, err)
		require.Equal(t, 1, len(ddlEvents))
		// drop db event
		require.Equal(t, uint64(700), ddlEvents[0].FinishedTs)
		require.Equal(t, common.DB, ddlEvents[0].NeedDroppedTables.InfluenceType)
		require.Equal(t, schemaID, ddlEvents[0].NeedDroppedTables.SchemaID)
		require.Equal(t, "test", ddlEvents[0].NeedDroppedTables.SchemaName)
	}

	// fetch table ddl events again
	{
		ddlEvents, err := pStorage.fetchTableDDLEvents(tableID3, 609, 700)
		require.Nil(t, err)
		require.Equal(t, 1, len(ddlEvents))
		// drop table event
		require.Equal(t, uint64(611), ddlEvents[0].FinishedTs)
		require.Equal(t, common.Normal, ddlEvents[0].NeedDroppedTables.InfluenceType)
		require.Equal(t, 1, len(ddlEvents[0].NeedDroppedTables.TableIDs))
		require.Equal(t, tableID3, ddlEvents[0].NeedDroppedTables.TableIDs[0])
		require.Equal(t, 1, len(ddlEvents[0].NeedDroppedTables.TableNames))
		require.Equal(t, "t3", ddlEvents[0].NeedDroppedTables.TableNames[0])
		require.Equal(t, schemaID, ddlEvents[0].NeedDroppedTables.SchemaID)
		require.Equal(t, "test", ddlEvents[0].NeedDroppedTables.SchemaName)
	}

	// fetch all table trigger ddl events
	{
		filteConfig := &config.FilterConfig{}
		eventFilter, err := filter.NewFilter(filteConfig, "", false)
		require.Nil(t, err)
		tableTriggerDDLEvents, err := pStorage.fetchTableTriggerDDLEvents(eventFilter, 0, 10)
		require.Nil(t, err)
		require.Equal(t, 6, len(tableTriggerDDLEvents))
		// create db event
		require.Equal(t, uint64(200), tableTriggerDDLEvents[0].FinishedTs)
		// create table event
		require.Equal(t, uint64(601), tableTriggerDDLEvents[1].FinishedTs)
		require.Equal(t, 1, len(tableTriggerDDLEvents[1].NeedAddedTables))
		require.Equal(t, schemaID, tableTriggerDDLEvents[1].NeedAddedTables[0].SchemaID)
		require.Equal(t, schemaName, tableTriggerDDLEvents[1].NeedAddedTables[0].SchemaName)
		require.Equal(t, tableID, tableTriggerDDLEvents[1].NeedAddedTables[0].TableID)
		require.Equal(t, "t1", tableTriggerDDLEvents[1].NeedAddedTables[0].TableName)
		// rename table event
		require.Equal(t, uint64(605), tableTriggerDDLEvents[2].FinishedTs)
		// create table event
		require.Equal(t, uint64(609), tableTriggerDDLEvents[3].FinishedTs)
		// drop table event
		require.Equal(t, uint64(611), tableTriggerDDLEvents[4].FinishedTs)
		require.Equal(t, common.Normal, tableTriggerDDLEvents[4].NeedDroppedTables.InfluenceType)
		require.Equal(t, schemaID, tableTriggerDDLEvents[4].NeedDroppedTables.SchemaID)
		require.Equal(t, schemaName, tableTriggerDDLEvents[4].NeedDroppedTables.SchemaName)
		require.Equal(t, tableID3, tableTriggerDDLEvents[4].NeedDroppedTables.TableIDs[0])
		require.Equal(t, "t3", tableTriggerDDLEvents[4].NeedDroppedTables.TableNames[0])
		// drop db event
		require.Equal(t, uint64(700), tableTriggerDDLEvents[5].FinishedTs)
	}

	// fetch partial table trigger ddl events
	{
		filteConfig := &config.FilterConfig{}
		eventFilter, err := filter.NewFilter(filteConfig, "", false)
		require.Nil(t, err)
		tableTriggerDDLEvents, err := pStorage.fetchTableTriggerDDLEvents(eventFilter, 0, 2)
		require.Nil(t, err)
		require.Equal(t, 2, len(tableTriggerDDLEvents))
		require.Equal(t, uint64(200), tableTriggerDDLEvents[0].FinishedTs)
		require.Equal(t, uint64(601), tableTriggerDDLEvents[1].FinishedTs)
	}

	// TODO: test filter
}

func TestGC(t *testing.T) {
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
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionCreateTable),
			SchemaID:      schemaID,
			TableID:       tableID3,
			SchemaVersion: 501,
			TableInfo: &model.TableInfo{
				ID:   tableID3,
				Name: model.NewCIStr("t3"),
			},
			FinishedTs: 601,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
	}

	// drop table t2
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionDropTable),
			SchemaID:      schemaID,
			TableID:       tableID2,
			SchemaVersion: 503,
			TableInfo:     nil,
			FinishedTs:    603,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
	}

	// rename table t1
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionRenameTable),
			SchemaID:      schemaID,
			TableID:       tableID1,
			SchemaVersion: 505,
			TableInfo: &model.TableInfo{
				ID:   tableID1,
				Name: model.NewCIStr("t1_r"),
			},
			FinishedTs: 605,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
	}

	// write upper bound
	newUpperBound := upperBoundMeta{
		FinishedDDLTs: 700,
		SchemaVersion: 509,
		ResolvedTs:    705,
	}
	{
		writeUpperBoundMeta(pStorage.db, newUpperBound)
	}

	pStorage.registerTable(tableID1, gcTs+1)

	// mock gc
	newGcTs := uint64(603)
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
		tablesInKVSnap := mockWriteKVSnapOnDisk(pStorage.db, newGcTs, databaseInfo)

		require.Equal(t, 3, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, 3, len(pStorage.tablesDDLHistory))
		require.Equal(t, false, pStorage.tableMap[tableID3].InKVSnap)
		pStorage.cleanObseleteDataInMemory(newGcTs, tablesInKVSnap)
		require.Equal(t, 1, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, uint64(605), pStorage.tableTriggerDDLHistory[0])
		require.Equal(t, 1, len(pStorage.tablesDDLHistory))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[tableID1]))
		require.Equal(t, true, pStorage.tableMap[tableID3].InKVSnap)
		tableInfoT1, err := pStorage.getTableInfo(tableID1, newGcTs)
		require.Nil(t, err)
		require.Equal(t, "t1", tableInfoT1.Name.O)
		tableInfoT1, err = pStorage.getTableInfo(tableID1, 606)
		require.Nil(t, err)
		require.Equal(t, "t1_r", tableInfoT1.Name.O)
	}

	pStorage = loadPersistentStorageForTest(pStorage.db, newGcTs, newUpperBound)
	{
		require.Equal(t, newGcTs, pStorage.gcTs)
		require.Equal(t, newUpperBound, pStorage.upperBound)
		require.Equal(t, 1, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, uint64(605), pStorage.tableTriggerDDLHistory[0])
		require.Equal(t, 1, len(pStorage.tablesDDLHistory))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[tableID1]))
		require.Equal(t, true, pStorage.tableMap[tableID3].InKVSnap)
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
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionCreateTable),
			SchemaID:      schemaID,
			TableID:       tableID3,
			SchemaVersion: 501,
			TableInfo: &model.TableInfo{
				ID:   tableID3,
				Name: model.NewCIStr("t3"),
			},
			FinishedTs: 601,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
	}

	// drop table t2
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionDropTable),
			SchemaID:      schemaID,
			TableID:       tableID2,
			SchemaVersion: 503,
			TableInfo:     nil,
			FinishedTs:    603,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
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
