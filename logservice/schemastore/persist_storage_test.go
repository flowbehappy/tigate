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
)

func newPersistentStorageForTest(db *pebble.DB, gcTs common.Ts, upperBound upperBoundMeta) *persistentStorage {
	p := &persistentStorage{
		pdCli:                  nil,
		kvStorage:              nil,
		db:                     db,
		gcTs:                   gcTs,
		databaseMap:            make(map[common.SchemaID]*DatabaseInfo),
		tablesBasicInfo:        make(map[common.TableID]*VersionedTableBasicInfo),
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

func TestReadWriteMeta(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.Nil(t, err)
	defer db.Close()

	{
		gcTS := common.Ts(1000)
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
		gcTS := common.Ts(2000)

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
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.Nil(t, err)
	defer db.Close()

	gcTs := common.Ts(1000)
	upperBound := upperBoundMeta{
		FinishedDDLTs: 3000,
		SchemaVersion: 4000,
		ResolvedTs:    2000,
	}
	writeGcTs(db, gcTs)
	writeUpperBoundMeta(db, upperBound)

	schemaID := common.SchemaID(50)
	tableID := common.TableID(99)
	{
		batch := db.NewBatch()
		writeSchemaInfoToBatch(batch, gcTs, &model.DBInfo{
			ID:   int64(schemaID),
			Name: model.NewCIStr("test"),
		})
		tableInfo := &model.TableInfo{
			ID:   int64(tableID),
			Name: model.NewCIStr("t"),
		}
		tableInfoValue, err := json.Marshal(tableInfo)
		require.Nil(t, err)
		writeTableInfoToBatch(batch, gcTs, int64(schemaID), tableInfoValue)
		err = batch.Commit(pebble.Sync)
		require.Nil(t, err)
	}

	pStorage := newPersistentStorageForTest(db, gcTs, upperBound)
	require.Equal(t, 1, len(pStorage.databaseMap))
	require.Equal(t, "test", pStorage.databaseMap[schemaID].Name)

	{
		store := newEmptyVersionedTableInfoStore(tableID)
		pStorage.buildVersionedTableInfoStore(store)
		tableInfo, err := store.getTableInfo(gcTs)
		require.Nil(t, err)
		require.Equal(t, "t", tableInfo.Name.O)
		require.Equal(t, tableID, common.TableID(tableInfo.ID))
	}

	// rename table
	renameVersion := uint64(1500)
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionRenameTable),
			SchemaID:      int64(schemaID),
			TableID:       int64(tableID),
			SchemaVersion: 3000,
			TableInfo: &model.TableInfo{
				ID:   int64(tableID),
				Name: model.NewCIStr("t2"),
			},
			FinishedTs: renameVersion,
		}
		err = pStorage.handleSortedDDLEvents(ddlEvent)
		require.Nil(t, err)
	}

	pStorage = newPersistentStorageForTest(db, gcTs, upperBound)
	{
		store := newEmptyVersionedTableInfoStore(tableID)
		pStorage.buildVersionedTableInfoStore(store)
		require.Equal(t, 2, len(store.infos))
		tableInfo, err := store.getTableInfo(gcTs)
		require.Nil(t, err)
		require.Equal(t, "t", tableInfo.Name.O)
		require.Equal(t, tableID, common.TableID(tableInfo.ID))
		tableInfo2, err := store.getTableInfo(common.Ts(renameVersion))
		require.Nil(t, err)
		require.Equal(t, "t2", tableInfo2.Name.O)

		renameVersion2 := uint64(3000)
		store.applyDDL(PersistedDDLEvent{
			Type:          byte(model.ActionRenameTable),
			SchemaID:      int64(schemaID),
			TableID:       int64(tableID),
			SchemaVersion: 3000,
			TableInfo: &model.TableInfo{
				ID:   int64(tableID),
				Name: model.NewCIStr("t3"),
			},
			FinishedTs: renameVersion2,
		})
		tableInfo3, err := store.getTableInfo(common.Ts(renameVersion2))
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
	schemaID := common.SchemaID(300)
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionCreateSchema),
			SchemaID:      int64(schemaID),
			SchemaVersion: 100,
			TableInfo:     nil,
			FinishedTs:    200,
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
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionCreateTable),
			SchemaID:      int64(schemaID),
			TableID:       int64(tableID),
			SchemaVersion: 101,
			TableInfo: &model.TableInfo{
				Name: model.NewCIStr("t1"),
			},
			FinishedTs: 201,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)

		require.Equal(t, 1, len(pStorage.databaseMap[schemaID].Tables))
		require.Equal(t, 1, len(pStorage.tablesBasicInfo))
		require.Equal(t, 2, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, uint64(201), pStorage.tableTriggerDDLHistory[1])
		require.Equal(t, 1, len(pStorage.tablesDDLHistory))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[tableID]))
	}

	// create another table
	tableID2 := common.TableID(105)
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionCreateTable),
			SchemaID:      int64(schemaID),
			TableID:       int64(tableID2),
			SchemaVersion: 103,
			TableInfo: &model.TableInfo{
				Name: model.NewCIStr("t2"),
			},
			FinishedTs: 203,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)

		require.Equal(t, 2, len(pStorage.databaseMap[schemaID].Tables))
		require.Equal(t, 2, len(pStorage.tablesBasicInfo))
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
			SchemaID:      int64(schemaID),
			TableID:       int64(tableID2),
			SchemaVersion: 105,
			TableInfo:     nil,
			FinishedTs:    205,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)

		require.Equal(t, 1, len(pStorage.databaseMap[schemaID].Tables))
		require.Equal(t, 1, len(pStorage.tablesBasicInfo))
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
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionTruncateTable),
			SchemaID:      int64(schemaID),
			TableID:       int64(tableID),
			SchemaVersion: 107,
			TableInfo: &model.TableInfo{
				ID: int64(tableID3),
			},
			FinishedTs: 207,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)

		require.Equal(t, 1, len(pStorage.databaseMap[schemaID].Tables))
		require.Equal(t, 1, len(pStorage.tablesBasicInfo))
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
			SchemaID:      int64(schemaID),
			SchemaVersion: 200,
			TableInfo:     nil,
			FinishedTs:    300,
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

func TestHandleRenameTable(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)
	pStorage := newEmptyPersistentStorageForTest(dbPath)

	// create db1
	schemaID1 := common.SchemaID(300)
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionCreateSchema),
			SchemaID:      int64(schemaID1),
			SchemaVersion: 100,
			TableInfo:     nil,
			FinishedTs:    200,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
	}

	// create db2
	schemaID2 := common.SchemaID(305)
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionCreateSchema),
			SchemaID:      int64(schemaID2),
			SchemaVersion: 101,
			TableInfo:     nil,
			FinishedTs:    201,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
	}

	// create a table
	tableID := common.TableID(100)
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionCreateTable),
			SchemaID:      int64(schemaID1),
			TableID:       int64(tableID),
			SchemaVersion: 501,
			TableInfo: &model.TableInfo{
				ID:   int64(tableID),
				Name: model.NewCIStr("t1"),
			},
			FinishedTs: 601,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
		require.Equal(t, 2, len(pStorage.databaseMap))
		require.Equal(t, 1, len(pStorage.databaseMap[schemaID1].Tables))
		require.Equal(t, 0, len(pStorage.databaseMap[schemaID2].Tables))
		require.Equal(t, common.Ts(601), pStorage.tablesBasicInfo[tableID].CreateVersion)
		require.Equal(t, common.Ts(601), pStorage.tablesBasicInfo[tableID].SchemaIDs[0].CreateVersion)
		require.Equal(t, schemaID1, pStorage.tablesBasicInfo[tableID].SchemaIDs[0].SchemaID)
		require.Equal(t, common.Ts(601), pStorage.tablesBasicInfo[tableID].Names[0].CreateVersion)
		require.Equal(t, "t1", pStorage.tablesBasicInfo[tableID].Names[0].Name)
	}

	// rename table
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionRenameTable),
			SchemaID:      int64(schemaID2),
			TableID:       int64(tableID),
			SchemaVersion: 505,
			TableInfo: &model.TableInfo{
				ID:   int64(tableID),
				Name: model.NewCIStr("t2"),
			},
			FinishedTs: 605,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
		require.Equal(t, 2, len(pStorage.databaseMap))
		require.Equal(t, 0, len(pStorage.databaseMap[schemaID1].Tables))
		require.Equal(t, 1, len(pStorage.databaseMap[schemaID2].Tables))
		require.Equal(t, common.Ts(605), pStorage.tablesBasicInfo[tableID].SchemaIDs[1].CreateVersion)
		require.Equal(t, schemaID2, pStorage.tablesBasicInfo[tableID].SchemaIDs[1].SchemaID)
		require.Equal(t, schemaID2, getSchemaID(pStorage.tablesBasicInfo, tableID, common.Ts(605)))
		require.Equal(t, schemaID1, getSchemaID(pStorage.tablesBasicInfo, tableID, common.Ts(604)))
		require.Equal(t, common.Ts(605), pStorage.tablesBasicInfo[tableID].Names[1].CreateVersion)
		require.Equal(t, "t2", pStorage.tablesBasicInfo[tableID].Names[1].Name)
		require.Equal(t, "t2", getTableName(pStorage.tablesBasicInfo, tableID, common.Ts(605)))
		require.Equal(t, "t1", getTableName(pStorage.tablesBasicInfo, tableID, common.Ts(604)))
	}
}

func TestFetchDDLEvents(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)
	pStorage := newEmptyPersistentStorageForTest(dbPath)

	// create db
	schemaID := common.SchemaID(300)
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionCreateSchema),
			SchemaID:      int64(schemaID),
			SchemaVersion: 100,
			TableInfo:     nil,
			FinishedTs:    200,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
	}

	// create a table
	tableID := common.TableID(100)
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionCreateTable),
			SchemaID:      int64(schemaID),
			TableID:       int64(tableID),
			SchemaVersion: 501,
			TableInfo: &model.TableInfo{
				ID:   int64(tableID),
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
			SchemaID:      int64(schemaID),
			TableID:       int64(tableID),
			SchemaVersion: 505,
			TableInfo: &model.TableInfo{
				ID:   int64(tableID),
				Name: model.NewCIStr("t2"),
			},
			FinishedTs: 605,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
	}

	// truncate table
	tableID2 := common.TableID(105)
	{
		ddlEvent := PersistedDDLEvent{
			Type:          byte(model.ActionTruncateTable),
			SchemaID:      int64(schemaID),
			TableID:       int64(tableID),
			SchemaVersion: 507,
			TableInfo: &model.TableInfo{
				ID: int64(tableID2),
			},
			FinishedTs: 607,
		}
		pStorage.handleSortedDDLEvents(ddlEvent)
	}

	// fetch table ddl events
	{
		ddlEvents := pStorage.fetchTableDDLEvents(tableID, 601, 607)
		require.Equal(t, 2, len(ddlEvents))
		require.Equal(t, uint64(605), ddlEvents[0].FinishedTs)
		require.Equal(t, uint64(607), ddlEvents[1].FinishedTs)
	}

	// fetch all table trigger ddl events
	{
		filteConfig := &config.FilterConfig{}
		eventFilter, err := filter.NewFilter(filteConfig, "", false)
		require.Nil(t, err)
		tableTriggerDDLEvents := pStorage.fetchTableTriggerDDLEvents(eventFilter, 0, 10)
		require.Equal(t, 3, len(tableTriggerDDLEvents))
		require.Equal(t, uint64(200), tableTriggerDDLEvents[0].FinishedTs)
		require.Equal(t, uint64(601), tableTriggerDDLEvents[1].FinishedTs)
		require.Equal(t, uint64(605), tableTriggerDDLEvents[2].FinishedTs)
	}

	// fetch partial table trigger ddl events
	{
		filteConfig := &config.FilterConfig{}
		eventFilter, err := filter.NewFilter(filteConfig, "", false)
		require.Nil(t, err)
		tableTriggerDDLEvents := pStorage.fetchTableTriggerDDLEvents(eventFilter, 0, 2)
		require.Equal(t, 2, len(tableTriggerDDLEvents))
		require.Equal(t, uint64(200), tableTriggerDDLEvents[0].FinishedTs)
		require.Equal(t, uint64(601), tableTriggerDDLEvents[1].FinishedTs)
	}

	// TODO: test filter
}
