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
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
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
	pdCli pd.Client

	kvStorage kv.Storage

	db *pebble.DB

	mu sync.RWMutex

	// the current gcTs on disk
	gcTs uint64

	upperBound UpperBoundMeta

	upperBoundChanged bool

	tableMap map[int64]*BasicTableInfo

	partitionMap map[int64]BasicPartitionInfo

	// schemaID -> database info
	// it contains all databases and deleted databases
	// will only be removed when its delete version is smaller than gc ts
	databaseMap map[int64]*BasicDatabaseInfo

	// table id -> a sorted list of finished ts for the table's ddl events
	tablesDDLHistory map[int64][]uint64

	// it has two use cases:
	// 1. store the ddl events need to send to a table dispatcher
	//    Note: some ddl events in the history may never be send,
	//          for example the create table ddl, truncate table ddl(usually the first event)
	// 2. build table info store for a table
	tableTriggerDDLHistory []uint64

	// tableID -> versioned store
	// it just contains tables which is used by dispatchers
	tableInfoStoreMap map[int64]*versionedTableInfoStore

	// tableID -> total registered count
	tableRegisteredCount map[int64]int
}

func exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	log.Fatal("check path failed", zap.Error(err))
	return true
}

func openDB(dbPath string) *pebble.DB {
	opts := &pebble.Options{
		DisableWAL:   true,
		MemTableSize: 8 << 20,
	}
	opts.Levels = make([]pebble.LevelOptions, 7)
	for i := 0; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		l.BlockSize = 64 << 10       // 64 KB
		l.IndexBlockSize = 256 << 10 // 256 KB
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		l.TargetFileSize = 8 << 20 // 8 MB
		l.Compression = pebble.SnappyCompression
		l.EnsureDefaults()
	}
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		log.Fatal("open db failed", zap.Error(err))
	}
	return db
}

func newPersistentStorage(
	ctx context.Context,
	root string,
	pdCli pd.Client,
	storage kv.Storage,
) *persistentStorage {
	gcSafePoint, err := pdCli.UpdateServiceGCSafePoint(ctx, "cdc-new-store", 0, 0)
	if err != nil {
		log.Panic("get ts failed", zap.Error(err))
	}

	dbPath := fmt.Sprintf("%s/%s", root, dataDir)
	// FIXME: currently we don't try to reuse data at restart, when we need, just remove the following line
	if err := os.RemoveAll(dbPath); err != nil {
		log.Panic("fail to remove path")
	}

	dataStorage := &persistentStorage{
		pdCli:                  pdCli,
		kvStorage:              storage,
		tableMap:               make(map[int64]*BasicTableInfo),
		partitionMap:           make(map[int64]BasicPartitionInfo),
		databaseMap:            make(map[int64]*BasicDatabaseInfo),
		tablesDDLHistory:       make(map[int64][]uint64),
		tableTriggerDDLHistory: make([]uint64, 0),
		tableInfoStoreMap:      make(map[int64]*versionedTableInfoStore),
		tableRegisteredCount:   make(map[int64]int),
	}

	isDataReusable := false
	if exists(dbPath) {
		isDataReusable = true
		db := openDB(dbPath)
		// check whether the data on disk is reusable
		gcTs, err := readGcTs(db)
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

		if isDataReusable {
			dataStorage.db = db
			dataStorage.gcTs = gcTs
			dataStorage.upperBound = upperBound
			dataStorage.initializeFromDisk()
		} else {
			db.Close()
		}
	}
	if !isDataReusable {
		dataStorage.initializeFromKVStorage(dbPath, storage, gcSafePoint)
	}

	go func() {
		dataStorage.gc(ctx)
	}()

	go func() {
		dataStorage.persistUpperBoundPeriodically(ctx)
	}()

	return dataStorage
}

func (p *persistentStorage) initializeFromKVStorage(dbPath string, storage kv.Storage, gcTs uint64) {
	now := time.Now()
	if err := os.RemoveAll(dbPath); err != nil {
		log.Fatal("fail to remove path in initializeFromKVStorage")
	}
	p.db = openDB(dbPath)

	log.Info("schema store initialize from kv storage begin",
		zap.Uint64("snapTs", gcTs))

	var err error
	if p.databaseMap, p.tableMap, err = writeSchemaSnapshotAndMeta(p.db, storage, gcTs, true); err != nil {
		// TODO: retry
		log.Fatal("fail to initialize from kv snapshot")
	}
	p.gcTs = gcTs
	p.upperBound = UpperBoundMeta{
		FinishedDDLTs: 0,
		SchemaVersion: 0,
		ResolvedTs:    gcTs,
	}
	writeUpperBoundMeta(p.db, p.upperBound)
	log.Info("schema store initialize from kv storage done",
		zap.Int("databaseMapLen", len(p.databaseMap)),
		zap.Int("tableMapLen", len(p.tableMap)),
		zap.Any("duration(s)", time.Since(now).Seconds()))
}

func (p *persistentStorage) initializeFromDisk() {
	cleanObsoleteData(p.db, 0, p.gcTs)

	storageSnap := p.db.NewSnapshot()
	defer storageSnap.Close()

	var err error
	if p.databaseMap, err = loadDatabasesInKVSnap(storageSnap, p.gcTs); err != nil {
		log.Fatal("load database info from disk failed")
	}

	if p.tableMap, p.partitionMap, err = loadTablesInKVSnap(storageSnap, p.gcTs, p.databaseMap); err != nil {
		log.Fatal("load tables in kv snapshot failed")
	}

	if p.tablesDDLHistory, p.tableTriggerDDLHistory, err = loadAndApplyDDLHistory(
		storageSnap,
		p.gcTs,
		p.upperBound.FinishedDDLTs,
		p.databaseMap,
		p.tableMap,
		p.partitionMap); err != nil {
		log.Fatal("fail to initialize from disk")
	}
}

// getAllPhysicalTables returns all physical tables in the snapshot
// caller must ensure current resolve ts is larger than snapTs
func (p *persistentStorage) getAllPhysicalTables(snapTs uint64, tableFilter filter.Filter) ([]commonEvent.Table, error) {
	storageSnap := p.db.NewSnapshot()
	defer storageSnap.Close()

	p.mu.Lock()
	if snapTs < p.gcTs {
		return nil, fmt.Errorf("snapTs %d is smaller than gcTs %d", snapTs, p.gcTs)
	}
	gcTs := p.gcTs
	p.mu.Unlock()

	start := time.Now()
	defer func() {
		log.Info("getAllPhysicalTables finish",
			zap.Uint64("snapTs", snapTs),
			zap.Any("duration(s)", time.Since(start).Seconds()))
	}()
	return loadAllPhysicalTablesAtTs(storageSnap, gcTs, snapTs, tableFilter)
}

// only return when table info is initialized
func (p *persistentStorage) registerTable(tableID int64, startTs uint64) error {
	p.mu.Lock()
	if startTs < p.gcTs {
		p.mu.Unlock()
		return fmt.Errorf("startTs %d is smaller than gcTs %d", startTs, p.gcTs)
	}
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

	// Note: no need to check startTs < gcTs here again because if it is true, getTableInfo will failed later.

	return nil
}

func (p *persistentStorage) unregisterTable(tableID int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tableRegisteredCount[tableID] -= 1
	if p.tableRegisteredCount[tableID] <= 0 {
		if _, ok := p.tableInfoStoreMap[tableID]; !ok {
			return fmt.Errorf("table %d not found", tableID)
		}
		delete(p.tableInfoStoreMap, tableID)
		log.Info("unregister table",
			zap.Int64("tableID", tableID))
	}
	return nil
}

func (p *persistentStorage) getTableInfo(tableID int64, ts uint64) (*common.TableInfo, error) {
	p.mu.RLock()
	store, ok := p.tableInfoStoreMap[tableID]
	if !ok {
		p.mu.RUnlock()
		return nil, fmt.Errorf("table %d not found", tableID)
	}
	p.mu.RUnlock()
	return store.getTableInfo(ts)
}

// TODO: this may consider some shouldn't be send ddl, like create table, does it matter?
func (p *persistentStorage) getMaxEventCommitTs(tableID int64, ts uint64) uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.tablesDDLHistory[tableID]) == 0 {
		return 0
	}
	index := sort.Search(len(p.tablesDDLHistory[tableID]), func(i int) bool {
		return p.tablesDDLHistory[tableID][i] > ts
	})
	if index == 0 {
		return 0
	}
	return p.tablesDDLHistory[tableID][index-1]
}

// TODO: not all ddl in p.tablesDDLHistory should be sent to the dispatcher, verify dispatcher will set the right range
func (p *persistentStorage) fetchTableDDLEvents(tableID int64, tableFilter filter.Filter, start, end uint64) ([]commonEvent.DDLEvent, error) {
	// TODO: check a dispatcher won't fetch the ddl events that create it(create table/rename table)
	p.mu.RLock()
	// fast check
	history := p.tablesDDLHistory[tableID]
	if len(history) == 0 || start >= history[len(history)-1] {
		p.mu.RUnlock()
		return nil, nil
	}
	index := sort.Search(len(history), func(i int) bool {
		return history[i] > start
	})
	if index == len(history) {
		log.Panic("should not happen")
	}
	// copy all target ts to a new slice
	allTargetTs := make([]uint64, 0)
	for i := index; i < len(history); i++ {
		if history[i] <= end {
			allTargetTs = append(allTargetTs, history[i])
		}
	}
	p.mu.RUnlock()

	storageSnap := p.db.NewSnapshot()
	defer storageSnap.Close()

	p.mu.RLock()
	if start < p.gcTs {
		p.mu.RUnlock()
		return nil, fmt.Errorf("startTs %d is smaller than gcTs %d", start, p.gcTs)
	}
	p.mu.RUnlock()

	// TODO: if the first event is a create table ddl, return error?
	events := make([]commonEvent.DDLEvent, 0, len(allTargetTs))
	for _, ts := range allTargetTs {
		rawEvent := readPersistedDDLEvent(storageSnap, ts)
		// TODO: if ExtraSchemaName and other fields are empty, does it cause any problem?
		if tableFilter != nil &&
			tableFilter.ShouldDiscardDDL(model.ActionType(rawEvent.Type), rawEvent.CurrentSchemaName, rawEvent.CurrentTableName) &&
			tableFilter.ShouldDiscardDDL(model.ActionType(rawEvent.Type), rawEvent.PrevSchemaName, rawEvent.PrevTableName) {
			continue
		}
		events = append(events, buildDDLEvent(&rawEvent, tableFilter))
	}
	// log.Info("fetchTableDDLEvents",
	// 	zap.Int64("tableID", tableID),
	// 	zap.Uint64("start", start),
	// 	zap.Uint64("end", end),
	// 	zap.Any("history", history),
	// 	zap.Any("allTargetTs", allTargetTs))

	return events, nil
}

func (p *persistentStorage) fetchTableTriggerDDLEvents(tableFilter filter.Filter, start uint64, limit int) ([]commonEvent.DDLEvent, error) {
	// fast check
	p.mu.RLock()
	if len(p.tableTriggerDDLHistory) == 0 || start >= p.tableTriggerDDLHistory[len(p.tableTriggerDDLHistory)-1] {
		p.mu.RUnlock()
		return nil, nil
	}
	p.mu.RUnlock()

	events := make([]commonEvent.DDLEvent, 0)
	nextStartTs := start
	for {
		allTargetTs := make([]uint64, 0, limit)
		p.mu.RLock()
		// log.Debug("fetchTableTriggerDDLEvents in persistentStorage",
		// 	zap.Any("start", start),
		// 	zap.Int("limit", limit),
		// 	zap.Any("tableTriggerDDLHistory", p.tableTriggerDDLHistory))
		index := sort.Search(len(p.tableTriggerDDLHistory), func(i int) bool {
			return p.tableTriggerDDLHistory[i] > nextStartTs
		})
		// no more events to read
		if index == len(p.tableTriggerDDLHistory) {
			p.mu.RUnlock()
			return events, nil
		}
		for i := index; i < len(p.tableTriggerDDLHistory); i++ {
			allTargetTs = append(allTargetTs, p.tableTriggerDDLHistory[i])
			if len(allTargetTs) >= limit-len(events) {
				break
			}
		}
		p.mu.RUnlock()

		if len(allTargetTs) == 0 {
			return events, nil
		}

		// ensure the order: get target ts -> get storage snap -> check gc ts
		storageSnap := p.db.NewSnapshot()
		p.mu.RLock()
		if allTargetTs[0] < p.gcTs {
			p.mu.RUnlock()
			return nil, fmt.Errorf("startTs %d is smaller than gcTs %d", allTargetTs[0], p.gcTs)
		}
		p.mu.RUnlock()
		for _, ts := range allTargetTs {
			rawEvent := readPersistedDDLEvent(storageSnap, ts)
			if tableFilter != nil {
				if rawEvent.Type == byte(model.ActionCreateTables) {
					allFiltered := true
					for _, tableInfo := range rawEvent.MultipleTableInfos {
						if !tableFilter.ShouldDiscardDDL(model.ActionType(rawEvent.Type), rawEvent.CurrentSchemaName, tableInfo.Name.O) {
							allFiltered = false
							break
						}
					}
					if allFiltered {
						continue
					}
				} else {
					if tableFilter.ShouldDiscardDDL(model.ActionType(rawEvent.Type), rawEvent.CurrentSchemaName, rawEvent.CurrentTableName) &&
						tableFilter.ShouldDiscardDDL(model.ActionType(rawEvent.Type), rawEvent.PrevSchemaName, rawEvent.PrevTableName) {
						continue
					}
				}
			}
			events = append(events, buildDDLEvent(&rawEvent, tableFilter))
		}
		storageSnap.Close()
		if len(events) >= limit {
			return events, nil
		}
		nextStartTs = allTargetTs[len(allTargetTs)-1]
	}
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
	var allDDLFinishedTs []uint64
	allDDLFinishedTs = append(allDDLFinishedTs, p.tablesDDLHistory[tableID]...)
	p.mu.RUnlock()

	if err := addTableInfoFromKVSnap(store, kvSnapVersion, storageSnap); err != nil {
		return err
	}

	for _, version := range allDDLFinishedTs {
		ddlEvent := readPersistedDDLEvent(storageSnap, version)
		store.applyDDLFromPersistStorage(&ddlEvent)
	}
	store.setTableInfoInitialized()
	return nil
}

func addTableInfoFromKVSnap(
	store *versionedTableInfoStore,
	kvSnapVersion uint64,
	snap *pebble.Snapshot,
) error {
	tableInfo := readTableInfoInKVSnap(snap, store.getTableID(), kvSnapVersion)
	if tableInfo != nil {
		store.addInitialTableInfo(tableInfo)
	}
	return nil
}

func (p *persistentStorage) gc(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Minute)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			gcSafePoint, err := p.pdCli.UpdateServiceGCSafePoint(ctx, "cdc-new-store", 0, 0)
			if err != nil {
				log.Warn("get ts failed", zap.Error(err))
				continue
			}
			p.doGc(gcSafePoint)
		}
	}
}

func (p *persistentStorage) doGc(gcTs uint64) error {
	p.mu.Lock()
	if gcTs > p.upperBound.ResolvedTs {
		log.Panic("gc safe point is larger than resolvedTs",
			zap.Uint64("gcTs", gcTs),
			zap.Uint64("resolvedTs", p.upperBound.ResolvedTs))
	}
	if gcTs <= p.gcTs {
		p.mu.Unlock()
		return nil
	}
	oldGcTs := p.gcTs
	p.mu.Unlock()

	serverConfig := config.GetGlobalServerConfig()
	if !serverConfig.Debug.SchemaStore.EnableGC {
		log.Info("gc is disabled",
			zap.Uint64("gcTs", gcTs))
		return nil
	}

	start := time.Now()
	_, _, err := writeSchemaSnapshotAndMeta(p.db, p.kvStorage, gcTs, false)
	if err != nil {
		log.Warn("fail to write kv snapshot during gc",
			zap.Uint64("gcTs", gcTs))
		// TODO: return err and retry?
		return nil
	}
	log.Info("gc finish write schema snapshot",
		zap.Uint64("gcTs", gcTs),
		zap.Any("duration", time.Since(start)))

	// clean data in memory before clean data on disk
	p.cleanObsoleteDataInMemory(gcTs)
	log.Info("gc finish clean in memory data",
		zap.Uint64("gcTs", gcTs),
		zap.Any("duration", time.Since(start)))

	cleanObsoleteData(p.db, oldGcTs, gcTs)
	log.Info("gc finish",
		zap.Uint64("gcTs", gcTs),
		zap.Any("duration", time.Since(start)))

	return nil
}

func (p *persistentStorage) cleanObsoleteDataInMemory(gcTs uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gcTs = gcTs

	// clean tablesDDLHistory
	tablesToRemove := make(map[int64]interface{})
	for tableID := range p.tablesDDLHistory {
		i := sort.Search(len(p.tablesDDLHistory[tableID]), func(i int) bool {
			return p.tablesDDLHistory[tableID][i] > gcTs
		})
		if i == len(p.tablesDDLHistory[tableID]) {
			tablesToRemove[tableID] = nil
			continue
		}
		p.tablesDDLHistory[tableID] = p.tablesDDLHistory[tableID][i:]
	}
	for tableID := range tablesToRemove {
		delete(p.tablesDDLHistory, tableID)
	}

	// clean tableTriggerDDLHistory
	i := sort.Search(len(p.tableTriggerDDLHistory), func(i int) bool {
		return p.tableTriggerDDLHistory[i] > gcTs
	})
	p.tableTriggerDDLHistory = p.tableTriggerDDLHistory[i:]

	// clean tableInfoStoreMap
	// Note: tableInfoStoreMap need to keep one version before gcTs,
	//  so it has different gc logic with tablesDDLHistory
	tablesToRemove = make(map[int64]interface{})
	for tableID, store := range p.tableInfoStoreMap {
		if needRemove := store.gc(gcTs); needRemove {
			tablesToRemove[tableID] = nil
		}
	}
	for tableID := range tablesToRemove {
		delete(p.tableInfoStoreMap, tableID)
	}
}

func (p *persistentStorage) updateUpperBound(upperBound UpperBoundMeta) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.upperBound = upperBound
	p.upperBoundChanged = true
}

func (p *persistentStorage) getUpperBound() UpperBoundMeta {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.upperBound
}

func (p *persistentStorage) persistUpperBoundPeriodically(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			p.mu.Lock()
			if !p.upperBoundChanged {
				log.Warn("schema store upper bound not changed")
				p.mu.Unlock()
				continue
			}
			upperBound := p.upperBound
			p.upperBoundChanged = false
			p.mu.Unlock()

			writeUpperBoundMeta(p.db, upperBound)
		}
	}
}

func (p *persistentStorage) handleDDLJob(job *model.Job) error {
	p.mu.Lock()

	ddlEvent := buildPersistedDDLEventFromJob(job, p.databaseMap, p.tableMap, p.partitionMap)
	if shouldSkipDDL(&ddlEvent, p.databaseMap, p.tableMap) {
		p.mu.Unlock()
		return nil
	}

	p.mu.Unlock()
	// log.Info("handle resolved ddl event",
	// 	zap.Int64("schemaID", ddlEvent.CurrentSchemaID),
	// 	zap.Int64("tableID", ddlEvent.CurrentTableID),
	// 	zap.Uint64("finishedTs", ddlEvent.FinishedTs),
	// 	zap.Int64("schemaVersion", ddlEvent.SchemaVersion),
	// 	zap.String("ddlType", model.ActionType(ddlEvent.Type).String()),
	// 	zap.String("query", ddlEvent.Query))

	// Note: need write ddl event to disk before update ddl history,
	// becuase other goroutines may read ddl events from disk according to ddl history
	writePersistedDDLEvent(p.db, &ddlEvent)

	p.mu.Lock()
	var err error
	// Note: `updateDDLHistory` must be before `updateDatabaseInfoAndTableInfo`,
	// because `updateDDLHistory` will refer to the info in databaseMap and tableMap,
	// and `updateDatabaseInfoAndTableInfo` may delete some info from databaseMap and tableMap
	if p.tableTriggerDDLHistory, err = updateDDLHistory(
		&ddlEvent,
		p.databaseMap,
		p.tableMap,
		p.tablesDDLHistory,
		p.tableTriggerDDLHistory); err != nil {
		p.mu.Unlock()
		return err
	}
	if err := updateDatabaseInfoAndTableInfo(&ddlEvent, p.databaseMap, p.tableMap, p.partitionMap); err != nil {
		p.mu.Unlock()
		return err
	}
	if err := updateRegisteredTableInfoStore(&ddlEvent, p.tableInfoStoreMap); err != nil {
		p.mu.Unlock()
		return err
	}
	p.mu.Unlock()
	return nil
}

// transform ddl query based on sql mode.
func transformDDLJobQuery(job *model.Job) (string, error) {
	p := parser.New()
	// We need to use the correct SQL mode to parse the DDL query.
	// Otherwise, the parser may fail to parse the DDL query.
	// For example, it is needed to parse the following DDL query:
	//  `alter table "t" add column "c" int default 1;`
	// by adding `ANSI_QUOTES` to the SQL mode.
	p.SetSQLMode(job.SQLMode)
	stmts, _, err := p.Parse(job.Query, job.Charset, job.Collate)
	if err != nil {
		return "", errors.Trace(err)
	}
	var result string
	buildQuery := func(stmt ast.StmtNode) (string, error) {
		var sb strings.Builder
		// translate TiDB feature to special comment
		restoreFlags := format.RestoreTiDBSpecialComment
		// escape the keyword
		restoreFlags |= format.RestoreNameBackQuotes
		// upper case keyword
		restoreFlags |= format.RestoreKeyWordUppercase
		// wrap string with single quote
		restoreFlags |= format.RestoreStringSingleQuotes
		// remove placement rule
		restoreFlags |= format.SkipPlacementRuleForRestore
		// force disable ttl
		restoreFlags |= format.RestoreWithTTLEnableOff
		if err = stmt.Restore(format.NewRestoreCtx(restoreFlags, &sb)); err != nil {
			return "", errors.Trace(err)
		}
		return sb.String(), nil
	}
	if len(stmts) > 1 {
		results := make([]string, 0, len(stmts))
		for _, stmt := range stmts {
			query, err := buildQuery(stmt)
			if err != nil {
				return "", errors.Trace(err)
			}
			results = append(results, query)
		}
		result = strings.Join(results, ";")
	} else {
		result, err = buildQuery(stmts[0])
		if err != nil {
			return "", errors.Trace(err)
		}
	}

	log.Info("transform ddl query to result",
		zap.String("DDL", job.Query),
		zap.String("charset", job.Charset),
		zap.String("collate", job.Collate),
		zap.String("result", result))

	return result, nil
}

func buildPersistedDDLEventFromJob(
	job *model.Job,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
) PersistedDDLEvent {
	getSchemaName := func(schemaID int64) string {
		databaseInfo, ok := databaseMap[schemaID]
		if !ok {
			log.Panic("database not found",
				zap.Int64("schemaID", schemaID))
		}
		return databaseInfo.Name
	}
	getTableName := func(tableID int64) string {
		tableInfo, ok := tableMap[tableID]
		if !ok {
			log.Panic("table not found",
				zap.Int64("tableID", tableID))
		}
		return tableInfo.Name
	}
	getSchemaID := func(tableID int64) int64 {
		tableInfo, ok := tableMap[tableID]
		if !ok {
			log.Panic("table not found",
				zap.Int64("tableID", tableID))
		}
		return tableInfo.SchemaID
	}

	// TODO: handle err
	query, _ := transformDDLJobQuery(job)

	event := PersistedDDLEvent{
		ID:              job.ID,
		Type:            byte(job.Type),
		CurrentSchemaID: job.SchemaID,
		CurrentTableID:  job.TableID,
		Query:           query,
		SchemaVersion:   job.BinlogInfo.SchemaVersion,
		DBInfo:          job.BinlogInfo.DBInfo,
		TableInfo:       job.BinlogInfo.TableInfo,
		FinishedTs:      job.BinlogInfo.FinishedTS,
		BDRRole:         job.BDRRole,
		CDCWriteSource:  job.CDCWriteSource,
	}

	switch model.ActionType(event.Type) {
	case model.ActionCreateSchema,
		model.ActionDropSchema:
		log.Info("buildPersistedDDLEvent for create/drop schema",
			zap.Any("type", event.Type),
			zap.Int64("schemaID", event.CurrentSchemaID),
			zap.String("schemaName", event.DBInfo.Name.O))
		event.CurrentSchemaName = event.DBInfo.Name.O
	case model.ActionCreateTable, model.ActionRecoverTable:
		event.CurrentSchemaName = getSchemaName(event.CurrentSchemaID)
		event.CurrentTableName = event.TableInfo.Name.O
	case model.ActionDropTable,
		model.ActionAddColumn,
		model.ActionDropColumn,
		model.ActionAddIndex,
		model.ActionDropIndex,
		model.ActionAddForeignKey,
		model.ActionDropForeignKey:
		event.CurrentSchemaName = getSchemaName(event.CurrentSchemaID)
		event.CurrentTableName = getTableName(event.CurrentTableID)
	case model.ActionTruncateTable:
		// only table id change after truncate
		event.PrevTableID = event.CurrentTableID
		event.CurrentTableID = event.TableInfo.ID
		event.CurrentSchemaName = getSchemaName(event.CurrentSchemaID)
		event.CurrentTableName = getTableName(event.PrevTableID)
		if isPartitionTable(event.TableInfo) {
			for id := range partitionMap[event.PrevTableID] {
				event.PrevPartitions = append(event.PrevPartitions, id)
			}
		}
	case model.ActionModifyColumn,
		model.ActionRebaseAutoID:
		event.CurrentSchemaName = getSchemaName(event.CurrentSchemaID)
		event.CurrentTableName = getTableName(event.CurrentTableID)
	case model.ActionRenameTable:
		// Note: schema id/schema name/table name may be changed or not
		// table id does not change, we use it to get the table's prev schema id/name and table name
		event.PrevSchemaID = getSchemaID(event.CurrentTableID)
		event.PrevSchemaName = getSchemaName(event.PrevSchemaID)
		event.PrevTableName = getTableName(event.CurrentTableID)
		// get the table's current schema name and table name from the ddl job
		event.CurrentSchemaName = getSchemaName(event.CurrentSchemaID)
		event.CurrentTableName = event.TableInfo.Name.O
	case model.ActionSetDefaultValue,
		model.ActionShardRowID,
		model.ActionModifyTableComment,
		model.ActionRenameIndex:
		event.CurrentSchemaName = getSchemaName(event.CurrentSchemaID)
		event.CurrentTableName = getTableName(event.CurrentTableID)
	case model.ActionAddTablePartition,
		model.ActionDropTablePartition:
		event.CurrentSchemaName = getSchemaName(event.CurrentSchemaID)
		event.CurrentTableName = getTableName(event.CurrentTableID)
		for id := range partitionMap[event.CurrentTableID] {
			event.PrevPartitions = append(event.PrevPartitions, id)
		}
	case model.ActionCreateView:
		// ignore
	case model.ActionTruncateTablePartition:
		event.CurrentSchemaName = getSchemaName(event.CurrentSchemaID)
		event.CurrentTableName = getTableName(event.CurrentTableID)
		for id := range partitionMap[event.CurrentTableID] {
			event.PrevPartitions = append(event.PrevPartitions, id)
		}
	case model.ActionExchangeTablePartition:
		event.PrevSchemaID = event.CurrentSchemaID
		event.PrevTableID = event.CurrentTableID
		event.PrevSchemaName = getSchemaName(event.PrevSchemaID)
		event.PrevTableName = getTableName(event.PrevTableID)
		event.CurrentTableID = event.TableInfo.ID
		event.CurrentSchemaID = getSchemaID(event.CurrentTableID)
		event.CurrentSchemaName = getSchemaName(event.CurrentSchemaID)
		event.CurrentTableName = getTableName(event.CurrentTableID)
		for id := range partitionMap[event.CurrentTableID] {
			event.PrevPartitions = append(event.PrevPartitions, id)
		}
	case model.ActionCreateTables:
		event.CurrentSchemaName = getSchemaName(event.CurrentSchemaID)
		event.MultipleTableInfos = job.BinlogInfo.MultipleTableInfos
	case model.ActionReorganizePartition:
		event.CurrentSchemaName = getSchemaName(event.CurrentSchemaID)
		event.CurrentTableName = getTableName(event.CurrentTableID)
		for id := range partitionMap[event.CurrentTableID] {
			event.PrevPartitions = append(event.PrevPartitions, id)
		}
	default:
		log.Panic("unknown ddl type",
			zap.Any("ddlType", event.Type),
			zap.String("DDL", event.Query))
	}
	return event
}

func shouldSkipDDL(
	event *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
) bool {
	switch model.ActionType(event.Type) {
	// TODO: add some comment to explain why and when we should skip ActionCreateSchema/ActionCreateTable
	case model.ActionCreateSchema:
		if _, ok := databaseMap[event.CurrentSchemaID]; ok {
			log.Warn("database already exists. ignore DDL ",
				zap.String("DDL", event.Query),
				zap.Int64("jobID", event.ID),
				zap.Int64("schemaID", event.CurrentSchemaID),
				zap.Uint64("finishTs", event.FinishedTs),
				zap.Int64("jobSchemaVersion", event.SchemaVersion))
			return true
		}
	case model.ActionCreateTable:
		// Note: partition table's logical table id is also in tableMap
		if _, ok := tableMap[event.CurrentTableID]; ok {
			log.Warn("table already exists. ignore DDL ",
				zap.String("DDL", event.Query),
				zap.Int64("jobID", event.ID),
				zap.Int64("schemaID", event.CurrentSchemaID),
				zap.Int64("tableID", event.CurrentTableID),
				zap.Uint64("finishTs", event.FinishedTs),
				zap.Int64("jobSchemaVersion", event.SchemaVersion))
			return true
		}
	case model.ActionAlterTableAttributes,
		model.ActionAlterTablePartitionAttributes:
		// Note: these ddls seems not useful to sync to downstream?
		return true
	case model.ActionCreateTables:
		// For duplicate create tables ddl job, the tables in the job should be same, check the first table is enough
		if _, ok := tableMap[event.MultipleTableInfos[0].ID]; ok {
			log.Warn("table already exists. ignore DDL ",
				zap.String("DDL", event.Query),
				zap.Int64("jobID", event.ID),
				zap.Int64("schemaID", event.CurrentSchemaID),
				zap.Int64("tableID", event.CurrentTableID),
				zap.Uint64("finishTs", event.FinishedTs),
				zap.Int64("jobSchemaVersion", event.SchemaVersion))
			return true
		}
	}
	// Note: create tables don't need to be ignore, because we won't receive it twice
	return false
}

func isPartitionTable(tableInfo *model.TableInfo) bool {
	// tableInfo may only be nil in unit test
	return tableInfo != nil && tableInfo.Partition != nil
}

func getAllPartitionIDs(tableInfo *model.TableInfo) []int64 {
	physicalIDs := make([]int64, 0, len(tableInfo.Partition.Definitions))
	for _, partition := range tableInfo.Partition.Definitions {
		physicalIDs = append(physicalIDs, partition.ID)
	}
	return physicalIDs
}

func updateDDLHistory(
	ddlEvent *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	tablesDDLHistory map[int64][]uint64,
	tableTriggerDDLHistory []uint64,
) ([]uint64, error) {
	appendTableHistory := func(tableID int64) {
		tablesDDLHistory[tableID] = append(tablesDDLHistory[tableID], ddlEvent.FinishedTs)
	}
	appendPartitionsHistory := func(partitionIDs []int64) {
		for _, partitionID := range partitionIDs {
			tablesDDLHistory[partitionID] = append(tablesDDLHistory[partitionID], ddlEvent.FinishedTs)
		}
	}

	switch model.ActionType(ddlEvent.Type) {
	case model.ActionCreateSchema:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
	case model.ActionDropSchema:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		for tableID := range databaseMap[ddlEvent.CurrentSchemaID].Tables {
			appendTableHistory(tableID)
		}
	case model.ActionCreateTable, model.ActionRecoverTable,
		model.ActionDropTable:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		// Note: for create table, this ddl event will not be sent to table dispatchers.
		// add it to ddl history is just for building table info store.
		if isPartitionTable(ddlEvent.TableInfo) {
			// for partition table, we only care the ddl history of physical table ids.
			appendPartitionsHistory(getAllPartitionIDs(ddlEvent.TableInfo))
		} else {
			appendTableHistory(ddlEvent.CurrentTableID)
		}
	case model.ActionAddColumn,
		model.ActionDropColumn,
		model.ActionAddIndex,
		model.ActionDropIndex,
		model.ActionAddForeignKey,
		model.ActionDropForeignKey:
		if isPartitionTable(ddlEvent.TableInfo) {
			appendPartitionsHistory(getAllPartitionIDs(ddlEvent.TableInfo))
		} else {
			appendTableHistory(ddlEvent.CurrentTableID)
		}
	case model.ActionTruncateTable:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		if isPartitionTable(ddlEvent.TableInfo) {
			appendPartitionsHistory(getAllPartitionIDs(ddlEvent.TableInfo))
			appendPartitionsHistory(ddlEvent.PrevPartitions)
		} else {
			appendTableHistory(ddlEvent.CurrentTableID)
			appendTableHistory(ddlEvent.PrevTableID)
		}
	case model.ActionModifyColumn,
		model.ActionRebaseAutoID:
		if isPartitionTable(ddlEvent.TableInfo) {
			appendPartitionsHistory(getAllPartitionIDs(ddlEvent.TableInfo))
		} else {
			appendTableHistory(ddlEvent.CurrentTableID)
		}
	case model.ActionRenameTable:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		if isPartitionTable(ddlEvent.TableInfo) {
			appendPartitionsHistory(getAllPartitionIDs(ddlEvent.TableInfo))
		} else {
			appendTableHistory(ddlEvent.CurrentTableID)
		}
	case model.ActionSetDefaultValue,
		model.ActionShardRowID,
		model.ActionModifyTableComment,
		model.ActionRenameIndex:
		if isPartitionTable(ddlEvent.TableInfo) {
			appendPartitionsHistory(getAllPartitionIDs(ddlEvent.TableInfo))
		} else {
			appendTableHistory(ddlEvent.CurrentTableID)
		}
	case model.ActionAddTablePartition:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		// all partitions include newly create partitions will receive this event
		appendPartitionsHistory(getAllPartitionIDs(ddlEvent.TableInfo))
	case model.ActionDropTablePartition:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		appendPartitionsHistory(ddlEvent.PrevPartitions)
	case model.ActionCreateView:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		for tableID := range tableMap {
			appendTableHistory(tableID)
		}
	case model.ActionTruncateTablePartition:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		appendPartitionsHistory(ddlEvent.PrevPartitions)
		newCreateIDs := getCreatedIDs(ddlEvent.PrevPartitions, getAllPartitionIDs(ddlEvent.TableInfo))
		appendPartitionsHistory(newCreateIDs)
	case model.ActionExchangeTablePartition:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		droppedIDs := getDroppedIDs(ddlEvent.PrevPartitions, getAllPartitionIDs(ddlEvent.TableInfo))
		if len(droppedIDs) != 1 {
			log.Panic("exchange table partition should only drop one partition",
				zap.Int64s("droppedIDs", droppedIDs))
		}
		appendTableHistory(ddlEvent.PrevTableID)
		appendPartitionsHistory(droppedIDs)
	case model.ActionCreateTables:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		// it won't be send to table dispatchers, just for build version store
		for _, info := range ddlEvent.MultipleTableInfos {
			if isPartitionTable(info) {
				// for partition table, we only care the ddl history of physical table ids.
				appendPartitionsHistory(getAllPartitionIDs(info))
			} else {
				appendTableHistory(info.ID)
			}
		}
	case model.ActionReorganizePartition:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		appendPartitionsHistory(ddlEvent.PrevPartitions)
		newCreateIDs := getCreatedIDs(ddlEvent.PrevPartitions, getAllPartitionIDs(ddlEvent.TableInfo))
		appendPartitionsHistory(newCreateIDs)
	default:
		log.Panic("unknown ddl type",
			zap.Any("ddlType", ddlEvent.Type),
			zap.String("DDL", ddlEvent.Query))
	}

	return tableTriggerDDLHistory, nil
}

func updateDatabaseInfoAndTableInfo(
	event *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
) error {
	addTableToDB := func(schemaID int64, tableID int64) {
		databaseInfo, ok := databaseMap[schemaID]
		if !ok {
			log.Panic("database not found.",
				zap.String("DDL", event.Query),
				zap.Int64("jobID", event.ID),
				zap.Int64("schemaID", schemaID),
				zap.Int64("tableID", tableID),
				zap.Uint64("finishTs", event.FinishedTs),
				zap.Int64("jobSchemaVersion", event.SchemaVersion))
		}
		databaseInfo.Tables[tableID] = true
	}

	removeTableFromDB := func(schemaID int64, tableID int64) {
		databaseInfo, ok := databaseMap[schemaID]
		if !ok {
			log.Panic("database not found. ",
				zap.String("DDL", event.Query),
				zap.Int64("jobID", event.ID),
				zap.Int64("schemaID", schemaID),
				zap.Int64("tableID", tableID),
				zap.Uint64("finishTs", event.FinishedTs),
				zap.Int64("jobSchemaVersion", event.SchemaVersion))
		}
		delete(databaseInfo.Tables, tableID)
	}

	createTable := func(schemaID int64, tableID int64) {
		addTableToDB(schemaID, tableID)
		tableMap[tableID] = &BasicTableInfo{
			SchemaID: schemaID,
			Name:     event.TableInfo.Name.O,
		}
	}

	dropTable := func(schemaID int64, tableID int64) {
		removeTableFromDB(schemaID, tableID)
		delete(tableMap, tableID)
	}

	switch model.ActionType(event.Type) {
	case model.ActionCreateSchema:
		databaseMap[event.CurrentSchemaID] = &BasicDatabaseInfo{
			Name:   event.CurrentSchemaName,
			Tables: make(map[int64]bool),
		}
	case model.ActionDropSchema:
		for tableID := range databaseMap[event.CurrentSchemaID].Tables {
			delete(tableMap, tableID)
			// TODO: test it
			delete(partitionMap, tableID)
		}
		delete(databaseMap, event.CurrentSchemaID)
	case model.ActionCreateTable, model.ActionRecoverTable:
		createTable(event.CurrentSchemaID, event.CurrentTableID)
		if isPartitionTable(event.TableInfo) {
			partitionInfo := make(BasicPartitionInfo)
			for _, id := range getAllPartitionIDs(event.TableInfo) {
				partitionInfo[id] = nil
			}
			partitionMap[event.CurrentTableID] = partitionInfo
		}
	case model.ActionDropTable:
		dropTable(event.CurrentSchemaID, event.CurrentTableID)
		if isPartitionTable(event.TableInfo) {
			delete(partitionMap, event.CurrentTableID)
		}
	case model.ActionAddColumn,
		model.ActionDropColumn,
		model.ActionAddIndex,
		model.ActionDropIndex,
		model.ActionAddForeignKey,
		model.ActionDropForeignKey:
		// ignore
	case model.ActionTruncateTable:
		dropTable(event.CurrentSchemaID, event.PrevTableID)
		createTable(event.CurrentSchemaID, event.CurrentTableID)
		if isPartitionTable(event.TableInfo) {
			delete(partitionMap, event.PrevTableID)
			partitionInfo := make(BasicPartitionInfo)
			for _, id := range getAllPartitionIDs(event.TableInfo) {
				partitionInfo[id] = nil
			}
			partitionMap[event.CurrentTableID] = partitionInfo
		}
	case model.ActionModifyColumn,
		model.ActionRebaseAutoID:
		// ignore
	case model.ActionRenameTable:
		if event.PrevSchemaID != event.CurrentSchemaID {
			tableMap[event.CurrentTableID].SchemaID = event.CurrentSchemaID
			removeTableFromDB(event.PrevSchemaID, event.CurrentTableID)
			addTableToDB(event.CurrentSchemaID, event.CurrentTableID)
		}
		tableMap[event.CurrentTableID].Name = event.CurrentTableName
	case model.ActionSetDefaultValue,
		model.ActionShardRowID,
		model.ActionModifyTableComment,
		model.ActionRenameIndex:
		// TODO: verify can be ignored
	case model.ActionAddTablePartition:
		newCreatedIDs := getCreatedIDs(event.PrevPartitions, getAllPartitionIDs(event.TableInfo))
		for _, id := range newCreatedIDs {
			partitionMap[event.CurrentTableID][id] = nil
		}
	case model.ActionDropTablePartition:
		droppedIDs := getDroppedIDs(event.PrevPartitions, getAllPartitionIDs(event.TableInfo))
		for _, id := range droppedIDs {
			delete(partitionMap[event.CurrentTableID], id)
		}
	case model.ActionCreateView:
		// ignore
	case model.ActionTruncateTablePartition:
		physicalIDs := getAllPartitionIDs(event.TableInfo)
		droppedIDs := getDroppedIDs(event.PrevPartitions, physicalIDs)
		for _, id := range droppedIDs {
			delete(partitionMap[event.CurrentTableID], id)
		}
		newCreatedIDs := getCreatedIDs(event.PrevPartitions, physicalIDs)
		for _, id := range newCreatedIDs {
			partitionMap[event.CurrentTableID][id] = nil
		}
	case model.ActionExchangeTablePartition:
		physicalIDs := getAllPartitionIDs(event.TableInfo)
		droppedIDs := getDroppedIDs(event.PrevPartitions, physicalIDs)
		if len(droppedIDs) != 1 {
			log.Panic("exchange table partition should only drop one partition",
				zap.Int64s("droppedIDs", droppedIDs))
		}
		targetPartitionID := droppedIDs[0]
		dropTable(event.PrevSchemaID, event.PrevTableID)
		createTable(event.PrevSchemaID, targetPartitionID)
		delete(partitionMap[event.CurrentTableID], targetPartitionID)
		partitionMap[event.CurrentTableID][event.PrevTableID] = nil
	case model.ActionCreateTables:
		if event.MultipleTableInfos == nil {
			log.Panic("multiple table infos should not be nil")
		}
		for _, info := range event.MultipleTableInfos {
			addTableToDB(event.CurrentSchemaID, info.ID)
			tableMap[info.ID] = &BasicTableInfo{
				SchemaID: event.CurrentSchemaID,
				Name:     info.Name.O,
			}
			if isPartitionTable(info) {
				partitionInfo := make(BasicPartitionInfo)
				for _, id := range getAllPartitionIDs(info) {
					partitionInfo[id] = nil
				}
				partitionMap[info.ID] = partitionInfo
			}
		}

	case model.ActionReorganizePartition:
		physicalIDs := getAllPartitionIDs(event.TableInfo)
		droppedIDs := getDroppedIDs(event.PrevPartitions, physicalIDs)
		for _, id := range droppedIDs {
			delete(partitionMap[event.CurrentTableID], id)
		}
		newCreatedIDs := getCreatedIDs(event.PrevPartitions, physicalIDs)
		for _, id := range newCreatedIDs {
			partitionMap[event.CurrentTableID][id] = nil
		}
	default:
		log.Panic("unknown ddl type",
			zap.Any("ddlType", event.Type),
			zap.String("DDL", event.Query))
	}

	return nil
}

func updateRegisteredTableInfoStore(
	event *PersistedDDLEvent,
	tableInfoStoreMap map[int64]*versionedTableInfoStore,
) error {
	tryApplyDDLToStore := func() {
		if isPartitionTable(event.TableInfo) {
			allPhysicalIDs := getAllPartitionIDs(event.TableInfo)
			for _, id := range allPhysicalIDs {
				if store, ok := tableInfoStoreMap[id]; ok {
					store.applyDDL(event)
				}
			}
		} else {
			if store, ok := tableInfoStoreMap[event.CurrentTableID]; ok {
				store.applyDDL(event)
			}
		}
	}

	switch model.ActionType(event.Type) {
	case model.ActionCreateSchema,
		model.ActionDropSchema:
		// ignore
	case model.ActionCreateTable, model.ActionRecoverTable:
		if isPartitionTable(event.TableInfo) {
			allPhysicalIDs := getAllPartitionIDs(event.TableInfo)
			for _, id := range allPhysicalIDs {
				if _, ok := tableInfoStoreMap[event.CurrentTableID]; ok {
					log.Panic("newly created tables should not be registered",
						zap.Int64("tableID", id))
				}
			}
		} else {
			if _, ok := tableInfoStoreMap[event.CurrentTableID]; ok {
				log.Panic("newly created tables should not be registered",
					zap.Int64("tableID", event.CurrentTableID))
			}
		}
	case model.ActionDropTable,
		model.ActionAddColumn,
		model.ActionDropColumn:
		tryApplyDDLToStore()
	case model.ActionAddIndex,
		model.ActionDropIndex,
		model.ActionAddForeignKey,
		model.ActionDropForeignKey:
		// ignore
	case model.ActionTruncateTable:
		if isPartitionTable(event.TableInfo) {
			for _, id := range event.PrevPartitions {
				if store, ok := tableInfoStoreMap[id]; ok {
					store.applyDDL(event)
				}
			}
			allPhysicalIDs := getAllPartitionIDs(event.TableInfo)
			for _, id := range allPhysicalIDs {
				if _, ok := tableInfoStoreMap[id]; ok {
					log.Panic("newly created tables should not be registered",
						zap.Int64("tableID", event.CurrentTableID))
				}
			}
		} else {
			if store, ok := tableInfoStoreMap[event.PrevTableID]; ok {
				store.applyDDL(event)
			}
			if _, ok := tableInfoStoreMap[event.CurrentTableID]; ok {
				log.Panic("newly created tables should not be registered",
					zap.Int64("tableID", event.CurrentTableID))
			}
		}
	case model.ActionModifyColumn:
		tryApplyDDLToStore()
	case model.ActionRebaseAutoID:
		// TODO: verify can be ignored
	case model.ActionRenameTable:
		// ignore
	case model.ActionSetDefaultValue:
		tryApplyDDLToStore()
	case model.ActionShardRowID,
		model.ActionModifyTableComment,
		model.ActionRenameIndex:
		// TODO: verify can be ignored
	case model.ActionAddTablePartition:
		newCreatedIDs := getCreatedIDs(event.PrevPartitions, getAllPartitionIDs(event.TableInfo))
		for _, id := range newCreatedIDs {
			if _, ok := tableInfoStoreMap[id]; ok {
				log.Panic("newly created partitions should not be registered",
					zap.Int64("partitionID", id))
			}
		}
	case model.ActionDropTablePartition:
		droppedIDs := getDroppedIDs(event.PrevPartitions, getAllPartitionIDs(event.TableInfo))
		for _, id := range droppedIDs {
			if store, ok := tableInfoStoreMap[id]; ok {
				store.applyDDL(event)
			}
		}
	case model.ActionCreateView:
		// ignore
	case model.ActionTruncateTablePartition:
		physicalIDs := getAllPartitionIDs(event.TableInfo)
		droppedIDs := getDroppedIDs(event.PrevPartitions, physicalIDs)
		for _, id := range droppedIDs {
			if store, ok := tableInfoStoreMap[id]; ok {
				store.applyDDL(event)
			}
		}
		newCreatedIDs := getCreatedIDs(event.PrevPartitions, physicalIDs)
		for _, id := range newCreatedIDs {
			if _, ok := tableInfoStoreMap[id]; ok {
				log.Panic("newly created partitions should not be registered",
					zap.Int64("partitionID", id))
			}
		}
	case model.ActionExchangeTablePartition:
		physicalIDs := getAllPartitionIDs(event.TableInfo)
		droppedIDs := getDroppedIDs(event.PrevPartitions, physicalIDs)
		if len(droppedIDs) != 1 {
			log.Panic("exchange table partition should only drop one partition",
				zap.Int64s("droppedIDs", droppedIDs))
		}
		targetPartitionID := droppedIDs[0]
		if store, ok := tableInfoStoreMap[targetPartitionID]; ok {
			store.applyDDL(event)
		}
		if store, ok := tableInfoStoreMap[event.PrevTableID]; ok {
			store.applyDDL(event)
		}
	case model.ActionCreateTables:
		for _, info := range event.MultipleTableInfos {
			if isPartitionTable(info) {
				for _, id := range getAllPartitionIDs(info) {
					if _, ok := tableInfoStoreMap[id]; ok {
						log.Panic("newly created tables should not be registered",
							zap.Int64("tableID", id))
					}
				}
			} else {
				if _, ok := tableInfoStoreMap[info.ID]; ok {
					log.Panic("newly created tables should not be registered",
						zap.Int64("tableID", info.ID))
				}
			}

		}
	case model.ActionReorganizePartition:
		physicalIDs := getAllPartitionIDs(event.TableInfo)
		droppedIDs := getDroppedIDs(event.PrevPartitions, physicalIDs)
		for _, id := range droppedIDs {
			if store, ok := tableInfoStoreMap[id]; ok {
				store.applyDDL(event)
			}
		}
		newCreatedIDs := getCreatedIDs(event.PrevPartitions, physicalIDs)
		for _, id := range newCreatedIDs {
			if _, ok := tableInfoStoreMap[id]; ok {
				log.Panic("newly created partitions should not be registered",
					zap.Int64("partitionID", id))
			}
		}
	default:
		log.Panic("unknown ddl type",
			zap.Any("ddlType", event.Type),
			zap.String("DDL", event.Query))
	}
	return nil
}

func getCreatedIDs(oldIDs []int64, newIDs []int64) []int64 {
	oldIDsMap := make(map[int64]interface{}, len(oldIDs))
	for _, id := range oldIDs {
		oldIDsMap[id] = nil
	}
	createdIDs := make([]int64, 0)
	for _, id := range newIDs {
		if _, ok := oldIDsMap[id]; !ok {
			createdIDs = append(createdIDs, id)
		}
	}
	return createdIDs
}
func getDroppedIDs(oldIDs []int64, newIDs []int64) []int64 {
	return getCreatedIDs(newIDs, oldIDs)
}

func buildDDLEvent(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) commonEvent.DDLEvent {
	var wrapTableInfo *common.TableInfo
	if rawEvent.TableInfo != nil {
		wrapTableInfo = common.WrapTableInfo(
			rawEvent.CurrentSchemaID,
			rawEvent.CurrentSchemaName,
			rawEvent.TableInfo)
	}

	ddlEvent := commonEvent.DDLEvent{
		Type: rawEvent.Type,
		// TODO: whether the following four fields are needed
		SchemaID:   rawEvent.CurrentSchemaID,
		TableID:    rawEvent.CurrentTableID,
		SchemaName: rawEvent.CurrentSchemaName,
		TableName:  rawEvent.CurrentTableName,

		Query:      rawEvent.Query,
		TableInfo:  wrapTableInfo,
		FinishedTs: rawEvent.FinishedTs,
		TiDBOnly:   false,
	}

	switch model.ActionType(rawEvent.Type) {
	case model.ActionCreateSchema:
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{heartbeatpb.DDLSpan.TableID},
		}
	case model.ActionDropSchema:
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeDB,
			SchemaID:      rawEvent.CurrentSchemaID,
		}
		ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeDB,
			SchemaID:      rawEvent.CurrentSchemaID,
		}
		ddlEvent.TableNameChange = &commonEvent.TableNameChange{
			DropDatabaseName: rawEvent.CurrentSchemaName,
		}
	case model.ActionCreateTable, model.ActionRecoverTable:
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{heartbeatpb.DDLSpan.TableID},
		}
		if isPartitionTable(rawEvent.TableInfo) {
			physicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
			ddlEvent.NeedAddedTables = make([]commonEvent.Table, 0, len(physicalIDs))
			for _, id := range physicalIDs {
				ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
					SchemaID: rawEvent.CurrentSchemaID,
					TableID:  id,
				})
			}
			ddlEvent.BlockedTables.TableIDs = append(ddlEvent.BlockedTables.TableIDs, heartbeatpb.DDLSpan.TableID)
		} else {
			ddlEvent.NeedAddedTables = []commonEvent.Table{
				{
					SchemaID: rawEvent.CurrentSchemaID,
					TableID:  rawEvent.CurrentTableID,
				},
			}
		}
		ddlEvent.TableNameChange = &commonEvent.TableNameChange{
			AddName: []commonEvent.SchemaTableName{
				{
					SchemaName: rawEvent.CurrentSchemaName,
					TableName:  rawEvent.CurrentTableName,
				},
			},
		}
	case model.ActionDropTable:
		if isPartitionTable(rawEvent.TableInfo) {
			allPhysicalTableIDs := getAllPartitionIDs(rawEvent.TableInfo)
			allPhysicalTableIDsAndDDLSpanID := make([]int64, 0, len(rawEvent.TableInfo.Partition.Definitions)+1)
			allPhysicalTableIDsAndDDLSpanID = append(allPhysicalTableIDsAndDDLSpanID, allPhysicalTableIDs...)
			allPhysicalTableIDsAndDDLSpanID = append(allPhysicalTableIDsAndDDLSpanID, heartbeatpb.DDLSpan.TableID)
			ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      allPhysicalTableIDsAndDDLSpanID,
			}
			ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      allPhysicalTableIDs,
			}
		} else {
			ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{rawEvent.CurrentTableID, heartbeatpb.DDLSpan.TableID},
			}
			ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{rawEvent.CurrentTableID},
			}
		}
		ddlEvent.TableNameChange = &commonEvent.TableNameChange{
			DropName: []commonEvent.SchemaTableName{
				{
					SchemaName: rawEvent.CurrentSchemaName,
					TableName:  rawEvent.CurrentTableName,
				},
			},
		}
	case model.ActionAddColumn,
		model.ActionDropColumn,
		model.ActionAddIndex,
		model.ActionDropIndex,
		model.ActionAddForeignKey,
		model.ActionDropForeignKey:
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.CurrentTableID},
		}
	case model.ActionTruncateTable:
		if isPartitionTable(rawEvent.TableInfo) {
			prevPartitionsAndDDLSpanID := make([]int64, 0, len(rawEvent.PrevPartitions)+1)
			prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, rawEvent.PrevPartitions...)
			prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, heartbeatpb.DDLSpan.TableID)
			ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      prevPartitionsAndDDLSpanID,
			}
			ddlEvent.BlockedTables.TableIDs = append(ddlEvent.BlockedTables.TableIDs, heartbeatpb.DDLSpan.TableID)
			// Note: for truncate table, prev partitions must all be dropped.
			ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      rawEvent.PrevPartitions,
			}
			physicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
			ddlEvent.NeedAddedTables = make([]commonEvent.Table, 0, len(physicalIDs))
			for _, id := range physicalIDs {
				ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
					SchemaID: rawEvent.CurrentSchemaID,
					TableID:  id,
				})
			}
		} else {
			ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{rawEvent.PrevTableID},
			}
			ddlEvent.NeedAddedTables = []commonEvent.Table{
				{
					SchemaID: rawEvent.CurrentSchemaID,
					TableID:  rawEvent.CurrentTableID,
				},
			}
			ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{rawEvent.PrevTableID, heartbeatpb.DDLSpan.TableID},
			}
		}
	case model.ActionModifyColumn,
		model.ActionRebaseAutoID:
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.CurrentTableID},
		}
	case model.ActionRenameTable:
		ddlEvent.PrevSchemaName = rawEvent.PrevSchemaName
		ddlEvent.PrevTableName = rawEvent.PrevTableName
		ignorePrevTable := tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.PrevSchemaName, rawEvent.PrevTableName)
		ignoreCurrentTable := tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.CurrentSchemaName, rawEvent.CurrentTableName)
		if isPartitionTable(rawEvent.TableInfo) {
			allPhysicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
			if !ignorePrevTable {
				allPhysicalIDsAndDDLSpanID := make([]int64, 0, len(allPhysicalIDs)+1)
				allPhysicalIDsAndDDLSpanID = append(allPhysicalIDsAndDDLSpanID, allPhysicalIDs...)
				allPhysicalIDsAndDDLSpanID = append(allPhysicalIDsAndDDLSpanID, heartbeatpb.DDLSpan.TableID)
				ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
					InfluenceType: commonEvent.InfluenceTypeNormal,
					TableIDs:      allPhysicalIDsAndDDLSpanID,
				}
				if !ignoreCurrentTable {
					// check whether schema change
					if rawEvent.PrevSchemaID != rawEvent.CurrentSchemaID {
						ddlEvent.UpdatedSchemas = make([]commonEvent.SchemaIDChange, 0, len(allPhysicalIDs))
						for _, id := range allPhysicalIDs {
							ddlEvent.UpdatedSchemas = append(ddlEvent.UpdatedSchemas, commonEvent.SchemaIDChange{
								TableID:     id,
								OldSchemaID: rawEvent.PrevSchemaID,
								NewSchemaID: rawEvent.CurrentSchemaID,
							})
						}
					}
				} else {
					// the table is filtered out after rename table, we need drop the table
					ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
						InfluenceType: commonEvent.InfluenceTypeNormal,
						TableIDs:      allPhysicalIDs,
					}
					ddlEvent.TableNameChange = &commonEvent.TableNameChange{
						DropName: []commonEvent.SchemaTableName{
							{
								SchemaName: rawEvent.PrevSchemaName,
								TableName:  rawEvent.PrevTableName,
							},
						},
					}
				}
			} else if !ignoreCurrentTable {
				// the table is filtered out before rename table, we need add table here
				ddlEvent.NeedAddedTables = []commonEvent.Table{
					{
						SchemaID: rawEvent.CurrentSchemaID,
						TableID:  rawEvent.CurrentTableID,
					},
				}
				ddlEvent.NeedAddedTables = make([]commonEvent.Table, 0, len(allPhysicalIDs))
				for _, id := range allPhysicalIDs {
					ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
						SchemaID: rawEvent.CurrentSchemaID,
						TableID:  id,
					})
				}
				ddlEvent.TableNameChange = &commonEvent.TableNameChange{
					AddName: []commonEvent.SchemaTableName{
						{
							SchemaName: rawEvent.CurrentSchemaName,
							TableName:  rawEvent.CurrentTableName,
						},
					},
				}
			} else {
				// if the table is both filtered out before and after rename table, the ddl should not be fetched
				log.Panic("should not build a ignored rename table ddl",
					zap.String("DDL", rawEvent.Query),
					zap.Int64("jobID", rawEvent.ID),
					zap.Int64("schemaID", rawEvent.CurrentSchemaID),
					zap.Int64("tableID", rawEvent.CurrentTableID))
			}
		} else {
			if !ignorePrevTable {
				ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
					InfluenceType: commonEvent.InfluenceTypeNormal,
					TableIDs:      []int64{rawEvent.CurrentTableID, heartbeatpb.DDLSpan.TableID},
				}
				if !ignoreCurrentTable {
					if rawEvent.PrevSchemaID != rawEvent.CurrentSchemaID {
						ddlEvent.UpdatedSchemas = []commonEvent.SchemaIDChange{
							{
								TableID:     rawEvent.CurrentTableID,
								OldSchemaID: rawEvent.PrevSchemaID,
								NewSchemaID: rawEvent.CurrentSchemaID,
							},
						}
					}
				} else {
					// the table is filtered out after rename table, we need drop the table
					ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
						InfluenceType: commonEvent.InfluenceTypeNormal,
						TableIDs:      []int64{rawEvent.CurrentTableID},
					}
					ddlEvent.TableNameChange = &commonEvent.TableNameChange{
						DropName: []commonEvent.SchemaTableName{
							{
								SchemaName: rawEvent.PrevSchemaName,
								TableName:  rawEvent.PrevTableName,
							},
						},
					}
				}
			} else if !ignoreCurrentTable {
				// the table is filtered out before rename table, we need add table here
				ddlEvent.NeedAddedTables = []commonEvent.Table{
					{
						SchemaID: rawEvent.CurrentSchemaID,
						TableID:  rawEvent.CurrentTableID,
					},
				}
				ddlEvent.TableNameChange = &commonEvent.TableNameChange{
					AddName: []commonEvent.SchemaTableName{
						{
							SchemaName: rawEvent.CurrentSchemaName,
							TableName:  rawEvent.CurrentTableName,
						},
					},
				}
			} else {
				// if the table is both filtered out before and after rename table, the ddl should not be fetched
				log.Panic("should not build a ignored rename table ddl",
					zap.String("DDL", rawEvent.Query),
					zap.Int64("jobID", rawEvent.ID),
					zap.Int64("schemaID", rawEvent.CurrentSchemaID),
					zap.Int64("tableID", rawEvent.CurrentTableID))
			}
		}
	case model.ActionSetDefaultValue,
		model.ActionShardRowID,
		model.ActionModifyTableComment,
		model.ActionRenameIndex:
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.CurrentTableID},
		}
	case model.ActionAddTablePartition:
		prevPartitionsAndDDLSpanID := make([]int64, 0, len(rawEvent.PrevPartitions)+1)
		prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, rawEvent.PrevPartitions...)
		prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, heartbeatpb.DDLSpan.TableID)
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      prevPartitionsAndDDLSpanID,
		}
		physicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
		newCreatedIDs := getCreatedIDs(rawEvent.PrevPartitions, physicalIDs)
		ddlEvent.NeedAddedTables = make([]commonEvent.Table, 0, len(newCreatedIDs))
		for _, id := range newCreatedIDs {
			ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
				SchemaID: rawEvent.CurrentSchemaID,
				TableID:  id,
			})
		}
	case model.ActionDropTablePartition:
		prevPartitionsAndDDLSpanID := make([]int64, 0, len(rawEvent.PrevPartitions)+1)
		prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, rawEvent.PrevPartitions...)
		prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, heartbeatpb.DDLSpan.TableID)
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      prevPartitionsAndDDLSpanID,
		}
		physicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
		droppedIDs := getDroppedIDs(rawEvent.PrevPartitions, physicalIDs)
		ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      droppedIDs,
		}
	case model.ActionCreateView:
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeAll,
		}
	case model.ActionTruncateTablePartition:
		prevPartitionsAndDDLSpanID := make([]int64, 0, len(rawEvent.PrevPartitions)+1)
		prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, rawEvent.PrevPartitions...)
		prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, heartbeatpb.DDLSpan.TableID)
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      prevPartitionsAndDDLSpanID,
		}
		physicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
		newCreatedIDs := getCreatedIDs(rawEvent.PrevPartitions, physicalIDs)
		for _, id := range newCreatedIDs {
			ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
				SchemaID: rawEvent.CurrentSchemaID,
				TableID:  id,
			})
		}
		droppedIDs := getDroppedIDs(rawEvent.PrevPartitions, physicalIDs)
		ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      droppedIDs,
		}
	case model.ActionExchangeTablePartition:
		ignoreNormalTable := tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.PrevSchemaName, rawEvent.PrevTableName)
		ignorePartitionTable := tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.CurrentSchemaName, rawEvent.CurrentTableName)
		physicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
		droppedIDs := getDroppedIDs(rawEvent.PrevPartitions, physicalIDs)
		if len(droppedIDs) != 1 {
			log.Panic("exchange table partition should only drop one partition",
				zap.Int64s("droppedIDs", droppedIDs))
		}
		targetPartitionID := droppedIDs[0]
		if !ignoreNormalTable && !ignorePartitionTable {
			ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{rawEvent.PrevTableID, targetPartitionID, heartbeatpb.DDLSpan.TableID},
			}
			ddlEvent.UpdatedSchemas = []commonEvent.SchemaIDChange{
				{
					TableID:     targetPartitionID,
					OldSchemaID: rawEvent.CurrentSchemaID,
					NewSchemaID: rawEvent.PrevSchemaID,
				},
				{
					TableID:     rawEvent.PrevTableID,
					OldSchemaID: rawEvent.PrevSchemaID,
					NewSchemaID: rawEvent.CurrentSchemaID,
				},
			}
		} else if !ignoreNormalTable {
			ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{rawEvent.PrevTableID, heartbeatpb.DDLSpan.TableID},
			}
			ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{rawEvent.PrevTableID},
			}
			ddlEvent.NeedAddedTables = []commonEvent.Table{
				{
					SchemaID: rawEvent.PrevSchemaID,
					TableID:  targetPartitionID,
				},
			}
		} else if !ignorePartitionTable {
			ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{targetPartitionID, heartbeatpb.DDLSpan.TableID},
			}
			ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{targetPartitionID},
			}
			ddlEvent.NeedAddedTables = []commonEvent.Table{
				{
					SchemaID: rawEvent.CurrentSchemaID,
					TableID:  rawEvent.PrevTableID,
				},
			}
		} else {
			log.Fatal("should not happen")
		}
	case model.ActionCreateTables:
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{heartbeatpb.DDLSpan.TableID},
		}
		physicalTableCount := 0
		logicalTableCount := 0
		for _, info := range rawEvent.MultipleTableInfos {
			if tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.CurrentSchemaName, info.Name.O) {
				continue
			}
			logicalTableCount += 1
			if isPartitionTable(info) {
				physicalTableCount += len(info.Partition.Definitions)
			} else {
				physicalTableCount += 1
			}
		}
		querys := strings.Split(rawEvent.Query, ";")
		ddlEvent.NeedAddedTables = make([]commonEvent.Table, 0, physicalTableCount)
		addName := make([]commonEvent.SchemaTableName, 0, logicalTableCount)
		resultQuerys := make([]string, 0, logicalTableCount)
		for i, info := range rawEvent.MultipleTableInfos {
			if tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.CurrentSchemaName, info.Name.O) {
				continue
			}
			if isPartitionTable(info) {
				for _, partitionID := range getAllPartitionIDs(info) {
					ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
						SchemaID: rawEvent.CurrentSchemaID,
						TableID:  partitionID,
					})
				}
			} else {
				ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
					SchemaID: rawEvent.CurrentSchemaID,
					TableID:  info.ID,
				})
			}
			addName = append(addName, commonEvent.SchemaTableName{
				SchemaName: rawEvent.CurrentSchemaName,
				TableName:  info.Name.O,
			})
			resultQuerys = append(resultQuerys, querys[i])
		}
		ddlEvent.TableNameChange = &commonEvent.TableNameChange{
			AddName: addName,
		}
		ddlEvent.Query = strings.Join(resultQuerys, ";")
		if len(ddlEvent.NeedAddedTables) == 0 {
			log.Fatal("should not happen")
		}
	case model.ActionReorganizePartition:
		// same as truncate partition
		prevPartitionsAndDDLSpanID := make([]int64, 0, len(rawEvent.PrevPartitions)+1)
		prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, rawEvent.PrevPartitions...)
		prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, heartbeatpb.DDLSpan.TableID)
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      prevPartitionsAndDDLSpanID,
		}
		physicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
		newCreatedIDs := getCreatedIDs(rawEvent.PrevPartitions, physicalIDs)
		for _, id := range newCreatedIDs {
			ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
				SchemaID: rawEvent.CurrentSchemaID,
				TableID:  id,
			})
		}
		droppedIDs := getDroppedIDs(rawEvent.PrevPartitions, physicalIDs)
		ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      droppedIDs,
		}
	default:
		log.Panic("unknown ddl type",
			zap.Any("ddlType", rawEvent.Type),
			zap.String("DDL", rawEvent.Query))
	}
	return ddlEvent
}
