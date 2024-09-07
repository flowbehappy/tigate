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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// The parent folder to store schema data
const dataDir = "schema_store"

// persistentStorage stores the following kinds of data on disk:
//  1. table info and database info from upstream snapshot
//  2. incremental ddl jobs
//  3. metadata which describes the valid data range on disk
type persistentStorage struct {
	gcRunning atomic.Bool

	pdCli pd.Client

	kvStorage kv.Storage

	db *pebble.DB

	mu sync.RWMutex

	// the current gcTs on disk
	gcTs uint64

	// all table ids is the current kv snapshot
	tablesInKVSnap map[common.TableID]bool

	// schemaID -> database info
	// it contains all databases and deleted databases
	// will only be removed when its delete version is smaller than gc ts
	databaseMap map[common.SchemaID]*DatabaseInfo

	// table id -> a sorted list of finished ts for the table's ddl events
	tablesDDLHistory map[common.TableID][]uint64

	tableTriggerDDLHistory []uint64
}

type upperBoundMeta struct {
	FinishedDDLTS common.Ts `json:"finished_ddl_ts"`
	SchemaVersion int64     `json:"schema_version"`
	ResolvedTS    common.Ts `json:"resolved_ts"`
}

func newPersistentStorage(
	ctx context.Context,
	root string,
	pdCli pd.Client,
	storage kv.Storage,
) (*persistentStorage, upperBoundMeta) {
	gcSafePoint, err := pdCli.UpdateServiceGCSafePoint(ctx, "cdc-new-store", 0, 0)
	if err != nil {
		log.Panic("get ts failed", zap.Error(err))
	}

	dbPath := fmt.Sprintf("%s/%s", root, dataDir)
	// FIXME: avoid remove
	if err := os.RemoveAll(dbPath); err != nil {
		log.Panic("fail to remove path")
	}

	// TODO: update pebble options
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		log.Fatal("open db failed", zap.Error(err))
	}

	// check whether the data on disk is reusable
	isDataReusable := true
	gcTs, err := readGcTs(db)
	// TODO: distiguish non-exist key with other io errors
	if err != nil {
		isDataReusable = false
	}
	if gcSafePoint < gcTs {
		log.Panic("gc safe point should never go back")
	}
	upperBound, err := readUpperBoundMeta(db)
	if err != nil {
		isDataReusable = false
	}
	if gcSafePoint >= upperBound.ResolvedTS {
		isDataReusable = false
	}

	// initialize persistent storage
	dataStorage := &persistentStorage{
		pdCli:                  pdCli,
		kvStorage:              storage,
		db:                     db,
		gcTs:                   gcTs,
		databaseMap:            make(map[common.SchemaID]*DatabaseInfo),
		tablesInKVSnap:         make(map[common.TableID]bool),
		tablesDDLHistory:       make(map[common.TableID][]uint64),
		tableTriggerDDLHistory: make([]uint64, 0),
	}
	if isDataReusable {
		dataStorage.initializeFromDisk(upperBound)
	} else {
		upperBound = dataStorage.initializeFromKVStorage(dbPath, storage, gcSafePoint)
	}

	go func() {
		dataStorage.gc(ctx)
	}()

	// TODO: start a go routine to update upper bound periodically

	return dataStorage, upperBound
}

func (p *persistentStorage) initializeFromKVStorage(dbPath string, storage kv.Storage, gcTs common.Ts) upperBoundMeta {
	// TODO: avoid recreate db if the path is empty at start
	if err := os.RemoveAll(dbPath); err != nil {
		log.Panic("fail to remove path")
	}

	var err error
	// TODO: update pebble options
	if p.db, err = pebble.Open(dbPath, &pebble.Options{}); err != nil {
		log.Fatal("open db failed", zap.Error(err))
	}
	log.Info("schema store create a fresh storage")

	var upperBound upperBoundMeta
	if p.databaseMap, p.tablesInKVSnap, upperBound, err = writeSchemaSnapshotAndMeta(p.db, storage, gcTs); err != nil {
		// TODO: retry
		log.Fatal("fail to initialize from kv snapshot")
	}
	p.gcTs = gcTs
	return upperBound
}

func (p *persistentStorage) initializeFromDisk(upperBound upperBoundMeta) {
	// TODO: cleanObseleteData?

	storageSnap := p.db.NewSnapshot()
	defer storageSnap.Close()

	var err error
	if p.tablesInKVSnap, err = loadTablesInKVSnap(storageSnap, p.gcTs); err != nil {
		log.Fatal("load tables in kv snapshot failed")
	}

	if p.databaseMap, p.tablesDDLHistory, p.tableTriggerDDLHistory, err = loadDatabaseInfoAndDDLHistory(storageSnap, p.gcTs, upperBound); err != nil {
		log.Fatal("fail to initialize from disk")
	}
}

func (p *persistentStorage) writeDDLEvent(ddlEvent DDLEvent) error {
	// TODO: do we need to ignore some ddl event?
	ddlValue, err := json.Marshal(ddlEvent)
	if err != nil {
		return err
	}
	switch ddlEvent.Job.Type {
	case model.ActionCreateSchema, model.ActionModifySchemaCharsetAndCollate, model.ActionDropSchema:
		batch := p.db.NewBatch()
		ddlKey, err := ddlJobSchemaKey(
			common.Ts(ddlEvent.Job.BinlogInfo.FinishedTS),
			int64(ddlEvent.Job.SchemaID))
		if err != nil {
			return err
		}
		batch.Set(ddlKey, ddlValue, pebble.NoSync)
		return batch.Commit(pebble.NoSync)
	case model.ActionCreateTable:
		batch := p.db.NewBatch()
		// TODO: for cross table ddl, need write two events(may be we need a table_id -> name map?)
		tableID := ddlEvent.Job.TableID
		finishedTs := ddlEvent.Job.BinlogInfo.FinishedTS
		p.mu.Lock()
		if _, ok := p.tablesDDLHistory[tableID]; ok {
			log.Panic("should not happen")
		}
		p.tablesDDLHistory[tableID] = []uint64{finishedTs}
		p.tableTriggerDDLHistory = append(p.tableTriggerDDLHistory, finishedTs)
		p.mu.Unlock()
		ddlKey, err := ddlJobTableKey(common.Ts(finishedTs), common.TableID(tableID))
		if err != nil {
			return err
		}
		batch.Set(ddlKey, ddlValue, pebble.NoSync)
		return batch.Commit(pebble.NoSync)
	default:
		log.Panic("unknown ddl type", zap.Any("ddlType", ddlEvent.Job.Type))
	}
	return nil
}

func (p *persistentStorage) buildVersionedTableInfoStore(
	store *versionedTableInfoStore,
	getSchemaName func(schemaID int64) (string, error),
) error {
	tableID := store.getTableID()
	// get snapshot from disk before get current gc ts to make sure data is not deleted by gc process
	storageSnap := p.db.NewSnapshot()
	defer storageSnap.Close()

	p.mu.RLock()
	kvSnapVersion := p.gcTs
	_, inKVSnap := p.tablesInKVSnap[tableID]
	var allDDLFinishedTs []uint64
	allDDLFinishedTs = append(allDDLFinishedTs, p.tablesDDLHistory[tableID]...)
	p.mu.RUnlock()

	if inKVSnap {
		if err := addTableInfoFromKVSnap(store, kvSnapVersion, storageSnap, getSchemaName); err != nil {
			return err
		}
	}

	for _, version := range allDDLFinishedTs {
		ddlEvent := readTableDDLEvent(storageSnap, tableID, version)
		schemaName, err := getSchemaName(int64(ddlEvent.Job.SchemaID))
		if err != nil {
			log.Fatal("get schema name failed", zap.Error(err))
		}
		ddlEvent.Job.SchemaName = schemaName

		store.applyDDLFromPersistStorage(ddlEvent.Job)
	}
	return nil
}

func addTableInfoFromKVSnap(
	store *versionedTableInfoStore,
	kvSnapVersion common.Ts,
	snap *pebble.Snapshot,
	getSchemaName func(schemaID int64) (string, error),
) error {
	schemaID, rawTableInfo := readSchemaIDAndTableInfoFromKVSnap(snap, store.getTableID(), kvSnapVersion)
	schemaName, err := getSchemaName(schemaID)
	if err != nil {
		return err
	}
	tableInfo := common.WrapTableInfo(schemaID, schemaName, uint64(kvSnapVersion), rawTableInfo)
	store.addInitialTableInfo(tableInfo)
	return nil
}

func (p *persistentStorage) gc(ctx context.Context) error {
	// if p.gcRunning.CompareAndSwap(false, true) {
	// 	return nil
	// }
	// defer p.gcRunning.Store(false)
	// p.gcTS.Store(uint64(gcTS))
	// // TODO: write snapshot(schema and table) to disk(don't need to be in the same batch) and maintain the key that need be deleted(or just write it to a delete batch)

	// // update gcTS in disk, must do it before delete any data
	// batch := p.db.NewBatch()
	// if err := writeTSToBatch(batch, gcTSKey(), gcTS); err != nil {
	// 	return err
	// }
	// if err := batch.Commit(pebble.NoSync); err != nil {
	// 	return err
	// }
	// // TODO: delete old data(including index data, so we need to read data one by one)
	// // may be write and delete in the same batch?

	for {
		select {
		case <-ctx.Done():
			return nil
			// TODO: get gc ts periodically
		}
	}
}

func (p *persistentStorage) doGc() error {
	return nil
}
