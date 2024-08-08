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
	writeTSToBatch(batch, metaTSKey(), metaTS.resolvedTS, metaTS.finishedDDLTS, metaTS.schemaVersion)
	return batch.Commit(pebble.Sync)
}

func writeSchemaSnapshot(db *pebble.DB, snapTS common.Ts, dbInfo *model.DBInfo) error {
	batch := db.NewBatch()
	schemaKey, err := snapshotSchemaKey(snapTS, int64(dbInfo.ID))
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
	tableKey, err := snapshotTableKey(snapTS, common.TableID(tableInfo.ID))
	if err != nil {
		log.Fatal("generate table key failed", zap.Error(err))
	}
	tableValue, err := json.Marshal(tableInfo)
	if err != nil {
		log.Fatal("marshal table failed", zap.Error(err))
	}
	batch.Set(tableKey, tableValue, pebble.NoSync)
	indexKey, err := indexSnapshotKey(common.TableID(tableInfo.ID), snapTS, int64(schemaID))
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
		resolvedTS:    1000,
		finishedDDLTS: 3000,
		schemaVersion: 4000,
	}

	err = writeMetaData(db, gcTS, metaTS)
	require.Nil(t, err)

	dataStorage, newMetaTS, databaseMap := loadPersistentStorage(db, 1000)
	require.Equal(t, metaTS.resolvedTS, newMetaTS.resolvedTS)
	require.Equal(t, metaTS.finishedDDLTS, newMetaTS.finishedDDLTS)
	require.Equal(t, metaTS.schemaVersion, newMetaTS.schemaVersion)
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
		resolvedTS:    1000,
		finishedDDLTS: 3000,
		schemaVersion: 4000,
	}

	err = writeMetaData(db, gcTS, metaTS)
	require.Nil(t, err)

	dataStorage, _, databaseMap := loadPersistentStorage(db, metaTS.resolvedTS+100)
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
		resolvedTS:    2000,
		finishedDDLTS: 3000,
		schemaVersion: 4000,
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
		ID:   int64(tableID),
		Name: model.NewCIStr("t"),
	})

	dataStorage, _, databaseMap := loadPersistentStorage(db, 1500)
	require.Equal(t, 1, len(databaseMap))
	require.Equal(t, "test", databaseMap[int64(schemaID)].Name)

	{
		store := newEmptyVersionedTableInfoStore(tableID)
		getSchemaName := func(schemaID int64) (string, error) {
			if _, ok := databaseMap[int64(schemaID)]; !ok {
				return "", fmt.Errorf("schema not found")
			}
			return databaseMap[int64(schemaID)].Name, nil
		}
		dataStorage.buildVersionedTableInfoStore(store, gcTS, metaTS.resolvedTS, getSchemaName)
		store.setTableInfoInitialized()
		require.Equal(t, gcTS, store.getFirstVersion())
		tableInfo, err := store.getTableInfo(gcTS)
		require.Nil(t, err)
		require.Equal(t, "t", tableInfo.Name.O)
		require.Equal(t, tableID, common.TableID(tableInfo.ID))
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
					ID:   int64(tableID),
					Name: model.NewCIStr("t2"),
				},
				FinishedTS: renameVersion,
			},
			TableID: int64(tableID),
		},
	})
	require.Nil(t, err)

	dataStorage, _, _ = loadPersistentStorage(db, 1500)
	{
		store := newEmptyVersionedTableInfoStore(tableID)
		getSchemaName := func(schemaID int64) (string, error) {
			if _, ok := databaseMap[int64(schemaID)]; !ok {
				return "", fmt.Errorf("schema not found")
			}
			return databaseMap[int64(schemaID)].Name, nil
		}
		dataStorage.buildVersionedTableInfoStore(store, gcTS, metaTS.resolvedTS, getSchemaName)
		store.setTableInfoInitialized()
		require.Equal(t, gcTS, store.getFirstVersion())
		require.Equal(t, 2, len(store.infos))
		tableInfo, err := store.getTableInfo(gcTS)
		require.Nil(t, err)
		require.Equal(t, "t", tableInfo.Name.O)
		require.Equal(t, tableID, common.TableID(tableInfo.ID))
		tableInfo2, err := store.getTableInfo(common.Ts(renameVersion))
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
		resolvedTS:    2000,
		finishedDDLTS: 3000,
		schemaVersion: 4000,
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
		ID:   int64(tableID),
		Name: model.NewCIStr("t"),
	})

	dataStorage, _, databaseMap := loadPersistentStorage(db, 1500)
	require.Equal(t, 1, len(databaseMap))
	require.Equal(t, "test", databaseMap[int64(schemaID)].Name)

	store := newEmptyVersionedTableInfoStore(tableID)
	getSchemaName := func(schemaID int64) (string, error) {
		if _, ok := databaseMap[int64(schemaID)]; !ok {
			return "", fmt.Errorf("schema not found")
		}
		return databaseMap[int64(schemaID)].Name, nil
	}
	dataStorage.buildVersionedTableInfoStore(store, gcTS, metaTS.resolvedTS, getSchemaName)
	renameVersion := uint64(1500)
	store.applyDDL(&model.Job{
		Type:     model.ActionRenameTable,
		SchemaID: int64(schemaID),
		BinlogInfo: &model.HistoryInfo{
			SchemaVersion: 3000,
			TableInfo: &model.TableInfo{
				ID:   int64(tableID),
				Name: model.NewCIStr("t2"),
			},
			FinishedTS: renameVersion,
		},
		TableID: int64(tableID),
	})
	store.setTableInfoInitialized()
	require.Equal(t, gcTS, store.getFirstVersion())
	require.Equal(t, 2, len(store.infos))
	tableInfo, err := store.getTableInfo(gcTS)
	require.Nil(t, err)
	require.Equal(t, "t", tableInfo.Name.O)
	require.Equal(t, tableID, common.TableID(tableInfo.ID))
	tableInfo2, err := store.getTableInfo(common.Ts(renameVersion))
	require.Nil(t, err)
	require.Equal(t, "t2", tableInfo2.Name.O)
}
