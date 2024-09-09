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
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/flowbehappy/tigate/logservice/logpuller"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/filter"
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

	// tableID -> versioned store
	// it just contains tables which is used by dispatchers
	tableInfoStoreMap map[common.TableID]*versionedTableInfoStore

	// tableID -> total registered count
	tableRegisteredCount map[common.TableID]int
}

type upperBoundMeta struct {
	FinishedDDLTs common.Ts `json:"finished_ddl_ts"`
	SchemaVersion int64     `json:"schema_version"`
	ResolvedTs    common.Ts `json:"resolved_ts"`
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
	if gcSafePoint >= upperBound.ResolvedTs {
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
		tableInfoStoreMap:      make(map[common.TableID]*versionedTableInfoStore),
		tableRegisteredCount:   make(map[common.TableID]int),
	}
	if isDataReusable {
		dataStorage.initializeFromDisk(upperBound)
	} else {
		upperBound = dataStorage.initializeFromKVStorage(dbPath, storage, gcSafePoint)
	}

	go func() {
		dataStorage.gc(ctx)
	}()

	go func() {
		dataStorage.updateUpperBound(ctx)
	}()

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

func (p *persistentStorage) handleSortedDDLEvents(ddlEvents ...DDLEvent) error {
	// TODO: ignore some ddl event

	// TODO: check ddl events are sorted

	p.mu.Lock()
	for _, event := range ddlEvents {
		updateDatabaseInfo(event.Job, p.databaseMap)
		updateDDLHistory(event.Job, p.tablesDDLHistory, p.tableTriggerDDLHistory)
		updateRegisteredTableInfoStore(event.Job, p.tableInfoStoreMap)
	}
	p.mu.Unlock()

	writeDDLEvents(p.db, ddlEvents...)
	return nil
}

// FIXME: load the info from disk
func (p *persistentStorage) getAllPhysicalTables(snapTs common.Ts, tableFilter filter.Filter) ([]common.Table, error) {
	meta := logpuller.GetSnapshotMeta(p.kvStorage, uint64(snapTs))
	dbinfos, err := meta.ListDatabases()
	if err != nil {
		log.Fatal("list databases failed", zap.Error(err))
	}

	tables := make([]common.Table, 0)

	for _, dbinfo := range dbinfos {
		if filter.IsSysSchema(dbinfo.Name.O) ||
			(tableFilter != nil && tableFilter.ShouldIgnoreSchema(dbinfo.Name.O)) {
			continue
		}
		rawTables, err := meta.GetMetasByDBID(dbinfo.ID)
		log.Info("get database", zap.Any("dbinfo", dbinfo), zap.Int("rawTablesLen", len(rawTables)))
		if err != nil {
			log.Fatal("get tables failed", zap.Error(err))
		}
		for _, rawTable := range rawTables {
			if !isTableRawKey(rawTable.Field) {
				continue
			}
			tbName := &model.TableNameInfo{}
			err := json.Unmarshal(rawTable.Value, tbName)
			if err != nil {
				log.Fatal("get table info failed", zap.Error(err))
			}
			// TODO: support ignore sequence / forcereplicate / view cases
			if tableFilter != nil && tableFilter.ShouldIgnoreTable(dbinfo.Name.O, tbName.Name.O) {
				continue
			}
			tables = append(tables, common.Table{
				SchemaID: dbinfo.ID,
				TableID:  tbName.ID,
			})
		}
	}

	return tables, nil
}

// only return when table info is initialized
func (p *persistentStorage) registerTable(tableID common.TableID) error {
	p.mu.Lock()
	p.tableRegisteredCount[tableID] += 1
	store, ok := p.tableInfoStoreMap[tableID]
	if !ok {
		store = newEmptyVersionedTableInfoStore(tableID)
		p.tableInfoStoreMap[tableID] = store
	}
	p.mu.Unlock()

	if !ok {
		return p.buildVersionedTableInfoStore(store)
	}

	store.waitTableInfoInitialized()
	return nil
}

func (p *persistentStorage) unregisterTable(tableID common.TableID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tableRegisteredCount[tableID] -= 1
	if p.tableRegisteredCount[tableID] <= 0 {
		if _, ok := p.tableInfoStoreMap[tableID]; !ok {
			return fmt.Errorf(fmt.Sprintf("table %d not found", tableID))
		}
		delete(p.tableInfoStoreMap, tableID)
	}
	return nil
}

func (p *persistentStorage) getTableInfo(tableID common.TableID, ts common.Ts) (*common.TableInfo, error) {
	p.mu.Lock()
	store, ok := p.tableInfoStoreMap[tableID]
	if !ok {
		return nil, fmt.Errorf(fmt.Sprintf("table %d not found", tableID))
	}
	p.mu.Unlock()
	return store.getTableInfo(ts)
}

func (p *persistentStorage) buildVersionedTableInfoStore(
	store *versionedTableInfoStore,
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

	getSchemaName := func(schemaID common.SchemaID) (string, error) {
		p.mu.RLock()
		defer func() {
			p.mu.RUnlock()
		}()

		databaseInfo, ok := p.databaseMap[schemaID]
		if !ok {
			return "", errors.New("database not found")
		}
		return databaseInfo.Name, nil
	}

	if inKVSnap {
		if err := addTableInfoFromKVSnap(store, kvSnapVersion, storageSnap, getSchemaName); err != nil {
			return err
		}
	}

	for _, version := range allDDLFinishedTs {
		ddlEvent := readDDLEvent(storageSnap, version)
		// TODO: check ddlEvent type
		schemaName, err := getSchemaName(common.SchemaID(ddlEvent.Job.SchemaID))
		if err != nil {
			log.Fatal("get schema name failed", zap.Error(err))
		}
		ddlEvent.Job.SchemaName = schemaName

		store.applyDDLFromPersistStorage(ddlEvent.Job)
	}
	store.setTableInfoInitialized()
	return nil
}

func addTableInfoFromKVSnap(
	store *versionedTableInfoStore,
	kvSnapVersion common.Ts,
	snap *pebble.Snapshot,
	getSchemaName func(schemaID common.SchemaID) (string, error),
) error {
	schemaID, rawTableInfo := readSchemaIDAndTableInfoFromKVSnap(snap, store.getTableID(), kvSnapVersion)
	schemaName, err := getSchemaName(schemaID)
	if err != nil {
		return err
	}
	tableInfo := common.WrapTableInfo(int64(schemaID), schemaName, uint64(kvSnapVersion), rawTableInfo)
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

func (p *persistentStorage) updateUpperBound(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
			// TODO: update upper bound periodically
		}
	}
}

func updateDatabaseInfo(
	job *model.Job,
	databaseMap map[common.SchemaID]*DatabaseInfo,
) error {
	switch job.Type {
	case model.ActionCreateSchema:
		return createSchema(job, databaseMap)
	case model.ActionModifySchemaCharsetAndCollate:
		// ignore
		return nil
	case model.ActionDropSchema:
		return dropSchema(job, databaseMap)
	case model.ActionRenameTables:
		var oldSchemaIDs, newSchemaIDs, oldTableIDs []int64
		var newTableNames, oldSchemaNames []*model.CIStr
		err := job.DecodeArgs(&oldSchemaIDs, &newSchemaIDs, &newTableNames, &oldTableIDs, &oldSchemaNames)
		if err != nil {
			return err
		}
	default:
		log.Panic("unknown ddl type", zap.Any("ddlType", job.Type))
	}

	return nil
}

func updateDDLHistory(
	job *model.Job,
	tablesDDLHistory map[common.TableID][]uint64,
	tableTriggerDDLHistory []uint64,
) error {
	// switch job.Type {
	// case model.ActionCreateSchema:
	// 	return createSchema(job, databaseMap)
	// case model.ActionModifySchemaCharsetAndCollate:
	// 	// ignore
	// 	return nil
	// case model.ActionDropSchema:
	// 	return dropSchema(job, databaseMap)
	// case model.ActionRenameTables:
	// 	var oldSchemaIDs, newSchemaIDs, oldTableIDs []int64
	// 	var newTableNames, oldSchemaNames []*model.CIStr
	// 	err := job.DecodeArgs(&oldSchemaIDs, &newSchemaIDs, &newTableNames, &oldTableIDs, &oldSchemaNames)
	// 	if err != nil {
	// 		return err
	// 	}
	// case model.ActionCreateTable:
	// 	tableID := job.TableID
	// 	finishedTs := job.BinlogInfo.FinishedTS
	// 	if _, ok := tablesDDLHistory[tableID]; ok {
	// 		log.Panic("should not happen")
	// 	}
	// 	tablesDDLHistory[tableID] = []uint64{finishedTs}
	// 	tableTriggerDDLHistory = append(tableTriggerDDLHistory, finishedTs)
	// case model.ActionCreateTables,
	// 	model.ActionCreateView,
	// 	model.ActionRecoverTable:
	// 	if err := fillSchemaName(job, databaseMap); err != nil {
	// 		return err
	// 	}
	// 	// no dispatcher should register on these kinds of tables?
	// 	// TODO: add a cache for these kinds of newly created tables because they may soon be registered?
	// 	if store, ok := tableInfoStoreMap[common.TableID(job.TableID)]; ok {
	// 		// it is possible that it is already registered if the following happens
	// 		// 1. event send to dispatcher manager
	// 		// 2. dispatcher register
	// 		// 3. begin apply ddl to schema store
	// 		store.applyDDL(job)
	// 	}
	// 	return nil
	// default:
	// 	log.Panic("unknown ddl type", zap.Any("ddlType", job.Type))
	// }

	return nil
}

func updateRegisteredTableInfoStore(
	job *model.Job,
	tableInfoStoreMap map[common.TableID]*versionedTableInfoStore,
) error {
	switch job.Type {
	// case model.ActionCreateTables,
	// 	model.ActionCreateView,
	// 	model.ActionRecoverTable:
	// 	if err := fillSchemaName(job, databaseMap); err != nil {
	// 		return err
	// 	}
	// 	// no dispatcher should register on these kinds of tables?
	// 	// TODO: add a cache for these kinds of newly created tables because they may soon be registered?
	// 	if store, ok := tableInfoStoreMap[common.TableID(job.TableID)]; ok {
	// 		// it is possible that it is already registered if the following happens
	// 		// 1. event send to dispatcher manager
	// 		// 2. dispatcher register
	// 		// 3. begin apply ddl to schema store
	// 		store.applyDDL(job)
	// 	}
	// 	return nil
	}
	return nil
}

func fillSchemaName(job *model.Job, databaseMap map[common.SchemaID]*DatabaseInfo) error {
	schemaID := common.SchemaID(job.SchemaID)
	databaseInfo, ok := databaseMap[schemaID]
	if !ok {
		log.Error("database not found", zap.Any("schemaID", schemaID))
		return errors.New("database not found")
	}
	if databaseInfo.CreateVersion > common.Ts(job.BinlogInfo.FinishedTS) {
		return errors.New("database is not created")
	}
	if databaseInfo.DeleteVersion < common.Ts(job.BinlogInfo.FinishedTS) {
		return errors.New("database is deleted")
	}
	job.SchemaName = databaseInfo.Name
	return nil
}

func createSchema(job *model.Job, databaseMap map[common.SchemaID]*DatabaseInfo) error {
	if _, ok := databaseMap[common.SchemaID(job.SchemaID)]; ok {
		return errors.New("database already exists")
	}
	databaseInfo := &DatabaseInfo{
		Name:          job.SchemaName,
		Tables:        make([]common.TableID, 0),
		CreateVersion: common.Ts(job.BinlogInfo.FinishedTS),
		DeleteVersion: math.MaxUint64,
	}
	databaseMap[common.SchemaID(job.SchemaID)] = databaseInfo
	return nil
}

func dropSchema(job *model.Job, databaseMap map[common.SchemaID]*DatabaseInfo) error {
	databaseInfo, ok := databaseMap[common.SchemaID(job.SchemaID)]
	if !ok {
		return errors.New("database not found")
	}
	if databaseInfo.DeleteVersion != math.MaxUint64 {
		return errors.New("database is already deleted")
	}
	databaseInfo.DeleteVersion = common.Ts(job.BinlogInfo.FinishedTS)
	return nil
}
