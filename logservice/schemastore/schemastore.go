package schemastore

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/logservice/logpuller"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type SchemaStore interface {
	common.SubModule

	// TODO: add filter
	GetAllPhysicalTables(snapTs common.Ts, filter filter.Filter) ([]common.Table, error)

	// RegisterDispatcher register the dispatcher into the schema store.
	// TODO: return a table info
	RegisterDispatcher(
		dispatcherID common.DispatcherID, span *heartbeatpb.TableSpan,
		startTS common.Ts, filter filter.Filter,
	) error

	// TODO: add interface for TableEventDispatcher

	UpdateDispatcherSendTS(dispatcherID common.DispatcherID, ts common.Ts) error

	UnregisterDispatcher(dispatcherID common.DispatcherID) error

	GetMaxFinishedDDLTS() common.Ts

	GetTableInfo(tableID common.TableID, ts common.Ts) (*common.TableInfo, error)

	// GetNextDDLEvents returns the next ddl event which finishedTs is within the range (start, end]
	GetNextDDLEvents(id common.TableID, start, end common.Ts) ([]common.DDLEvent, common.Ts, error)
	GetNextTableTriggerEvents(f filter.Filter, start common.Ts, limit int) ([]common.DDLEvent, common.Ts, error)
}

type schemaStore struct {
	ddlJobFetcher *ddlJobFetcher

	storage kv.Storage

	// store unresolved ddl event in memory, it is thread safe
	unsortedCache *ddlCache

	// store ddl event and other metadata on disk, it is thread safe
	dataStorage *persistentStorage

	eventCh chan interface{}

	// all following fields are guarded by this mutex
	// TODO: Refine the lock usage; it's prone to deadlock
	mu sync.RWMutex

	maxResolvedTS atomic.Uint64

	// max finishedTS of all applied ddl events
	finishedDDLTS uint64
	// max schemaVersion of all applied ddl events
	schemaVersion int64

	// schemaID -> database info
	// it contains all databases
	databaseMap DatabaseInfoMap

	// tableID -> versioned store
	// it just contains tables which have registered dispatchers
	tableInfoStoreMap TableInfoStoreMap

	// dispatcherID -> dispatch info
	// TODO: how to deal with table event dispatchers？
	dispatchersMap DispatcherInfoMap

	tableTriggerDispatcherMap map[common.DispatcherID]*ddlListWithFilter
}

func NewSchemaStore(
	ctx context.Context,
	root string,
	pdCli pd.Client,
	regionCache *tikv.RegionCache,
	pdClock pdutil.Clock,
	kvStorage kv.Storage,
) SchemaStore {
	gcSafePoint, err := pdCli.UpdateServiceGCSafePoint(ctx, "cdc-new-store", 0, 0)
	if err != nil {
		log.Panic("get ts failed", zap.Error(err))
	}
	dataStorage, metaTS, databaseMap := newPersistentStorage(root, kvStorage, gcSafePoint)
	log.Info("get gc safe point",
		zap.Uint64("gcSafePoint", gcSafePoint),
		zap.Any("metaTS", metaTS),
		zap.Int("databaseMapLen", len(databaseMap)))

	s := &schemaStore{
		storage:       kvStorage,
		unsortedCache: newDDLCache(),
		dataStorage:   dataStorage,
		eventCh:       make(chan interface{}, 1024),
		// TODO: fix the following two fields
		finishedDDLTS:     0,
		schemaVersion:     0,
		databaseMap:       databaseMap,
		tableInfoStoreMap: make(TableInfoStoreMap),
		dispatchersMap:    make(DispatcherInfoMap),
	}

	log.Info("new schema store",
		zap.Uint64("finishedDDLTS", s.finishedDDLTS),
		zap.Int64("schemaVersion", s.schemaVersion))
	s.ddlJobFetcher = newDDLJobFetcher(
		pdCli,
		regionCache,
		pdClock,
		kvStorage,
		metaTS.ResolvedTS,
		s.writeDDLEvent,
		s.advanceResolvedTs)
	return s
}

func (s *schemaStore) Name() string {
	return appcontext.SchemaStore
}

func (s *schemaStore) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return s.batchCommitAndUpdateWatermark(ctx)
	})
	eg.Go(func() error {
		return s.ddlJobFetcher.puller.Run(ctx)
	})
	return eg.Wait()
}

func (s *schemaStore) Close(ctx context.Context) error {
	return s.ddlJobFetcher.close(ctx)
}

func (s *schemaStore) batchCommitAndUpdateWatermark(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case data := <-s.eventCh:
			switch v := data.(type) {
			case DDLEvent:
				// TODO: fix a better way to filter system tables
				if v.Job.SchemaID == 1 {
					continue
				}
				// log.Info("write ddl event",
				// 	zap.String("schema", v.Job.SchemaName),
				// 	zap.String("table", v.Job.TableName),
				// 	zap.Uint64("startTs", v.Job.StartTS),
				// 	zap.Uint64("finishedTs", v.Job.BinlogInfo.FinishedTS),
				// 	zap.String("query", v.Job.Query))
				s.unsortedCache.addDDLEvent(v)
			case common.Ts:
				// TODO: check resolved ts is monotonically increasing
				resolvedEvents := s.unsortedCache.fetchSortedDDLEventBeforeTS(v)
				if len(resolvedEvents) == 0 {
					s.maxResolvedTS.Store(v)
					continue
				}
				log.Info("schema store resolved ts",
					zap.Any("resolvedTs", v),
					zap.Any("resolvedEventsLen", len(resolvedEvents)))
				// TODO: whether the events is ordered by finishedDDLTS and schemaVersion
				newFinishedDDLTS := resolvedEvents[len(resolvedEvents)-1].Job.BinlogInfo.FinishedTS
				newSchemaVersion := resolvedEvents[len(resolvedEvents)-1].Job.Version
				err := s.dataStorage.updateStoreMeta(v, newFinishedDDLTS, common.Ts(newSchemaVersion))
				if err != nil {
					log.Fatal("update ts failed", zap.Error(err))
				}
				s.mu.Lock()
				for _, event := range resolvedEvents {
					if event.Job.BinlogInfo.SchemaVersion <= s.schemaVersion || event.Job.BinlogInfo.FinishedTS <= s.finishedDDLTS {
						log.Info("skip already applied ddl job",
							zap.String("job", event.Job.Query),
							zap.Int64("jobSchemaVersion", event.Job.BinlogInfo.SchemaVersion),
							zap.Uint64("jobFinishTs", event.Job.BinlogInfo.FinishedTS),
							zap.Any("schemaVersion", s.schemaVersion),
							zap.Uint64("finishedDDLTS", s.finishedDDLTS))
						continue
					}
					log.Info("apply ddl job",
						zap.String("job", event.Job.Query),
						zap.Int64("jobSchemaVersion", event.Job.BinlogInfo.SchemaVersion),
						zap.Uint64("jobFinishTs", event.Job.BinlogInfo.FinishedTS))
					if err := handleResolvedDDLJob(event.Job, s.databaseMap, s.tableInfoStoreMap); err != nil {
						s.mu.Unlock()
						log.Error("handle ddl job failed", zap.Error(err))
						return err
					}
					// TODO: batch ddl event
					// TODO: write ddl event before update resolved ts
					err := s.dataStorage.writeDDLEvent(event)
					if err != nil {
						log.Fatal("write ddl event failed", zap.Error(err))
					}
					s.handleTriggerEvent(event)
					s.schemaVersion = event.Job.BinlogInfo.SchemaVersion
					s.finishedDDLTS = event.Job.BinlogInfo.FinishedTS
				}
				s.mu.Unlock()
				s.maxResolvedTS.Store(v)
			default:
				log.Fatal("unknown event type")
			}
		}
	}
}

func (s *schemaStore) GetAllPhysicalTables(snapTs common.Ts, f filter.Filter) ([]common.Table, error) {
	meta := logpuller.GetSnapshotMeta(s.storage, snapTs)
	dbinfos, err := meta.ListDatabases()
	if err != nil {
		log.Fatal("list databases failed", zap.Error(err))
	}

	tables := make([]common.Table, 0)

	for _, dbinfo := range dbinfos {
		if filter.IsSysSchema(dbinfo.Name.O) ||
			(f != nil && f.ShouldIgnoreSchema(dbinfo.Name.O)) {
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
			if f != nil && f.ShouldIgnoreTable(dbinfo.Name.O, tbName.Name.O) {
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

func (s *schemaStore) RegisterDispatcher(
	dispatcherID common.DispatcherID,
	span *heartbeatpb.TableSpan, startTS common.Ts,
	filter filter.Filter, /* only for table trigger dispatcher */
) error {
	s.mu.Lock()
	// TODO: fix me in the future
	if startTS < s.dataStorage.getGCTS() {
		s.mu.Unlock()
		log.Panic("startTs is old than gcTs",
			zap.Uint64("startTs", startTS),
			zap.Uint64("gcTs", s.dataStorage.getGCTS()))
	}

	if span.Equal(heartbeatpb.DDLSpan) {
		if _, ok := s.tableTriggerDispatcherMap[dispatcherID]; ok {
			s.mu.Unlock()
			return errors.New("table trigger dispatcher already exists")
		}
		s.tableTriggerDispatcherMap[dispatcherID] = &ddlListWithFilter{
			events: make([]DDLEvent, 0),
			filter: filter,
		}
		// TODO: restore the ddl event list from the persistent storage
		s.mu.Unlock()
		return nil
	}

	tableID := span.TableID
	s.dispatchersMap[dispatcherID] = DispatcherInfo{
		tableID: tableID,
	}
	getSchemaName := func(schemaID int64) (string, error) {
		s.mu.RLock()
		defer func() {
			s.mu.RUnlock()
		}()

		databaseInfo, ok := s.databaseMap[schemaID]
		if !ok {
			return "", errors.New("database not found")
		}
		return databaseInfo.Name, nil
	}
	// check whether there is already a versionedTableInfoStore satisfy the needs
	store, ok := s.tableInfoStoreMap[tableID]
	if !ok {
		store = newEmptyVersionedTableInfoStore(tableID)
		s.tableInfoStoreMap[tableID] = store
		store.registerDispatcher(dispatcherID, startTS)
		endTS := s.finishedDDLTS
		s.mu.Unlock()
		err := s.dataStorage.buildVersionedTableInfoStore(store, startTS, endTS, getSchemaName)
		if err != nil {
			// TODO: unregister dispatcher, make sure other wait go routines exit successfully
			return err
		}
		store.setTableInfoInitialized()
		return nil
	} else {
		// prevent old store from gc
		store.registerDispatcher(dispatcherID, startTS)
		s.mu.Unlock()
		store.waitTableInfoInitialized()
	}
	if store.getFirstVersion() <= startTS {
		return nil
	}
	endTS := store.getFirstVersion()

	// TODO: there may be multiple dispatchers build the same versionedTableInfoStore, optimize it later
	newStore := newEmptyVersionedTableInfoStore(tableID)
	err := s.dataStorage.buildVersionedTableInfoStore(newStore, startTS, endTS, getSchemaName)
	if err != nil {
		return err
	}
	newStore.setTableInfoInitialized()

	s.mu.Lock()
	defer func() {
		s.mu.Unlock()
	}()
	// check whether the data is gced again
	if startTS < s.dataStorage.getGCTS() {
		// TODO: unregister dispatcher, make sure other wait go routines exit successfully
		return errors.New("start ts is old than gc ts")
	}
	oldStore, ok := s.tableInfoStoreMap[tableID]
	if ok {
		// Note: oldStore must be initialized, no need to check again.
		// keep the store with smaller version
		if oldStore.getFirstVersion() <= newStore.getFirstVersion() {
			return nil
		} else {
			newStore.checkAndCopyTailFrom(oldStore)
			newStore.copyRegisteredDispatchers(oldStore)
			s.tableInfoStoreMap[tableID] = newStore
		}
	} else {
		log.Panic("should not happened")
	}
	return nil
}

func (s *schemaStore) UpdateDispatcherSendTS(dispatcherID common.DispatcherID, ts common.Ts) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.dispatchersMap[dispatcherID]
	if !ok {
		return errors.New("dispatcher not found")
	}
	store := s.tableInfoStoreMap[info.tableID]
	store.updateDispatcherSendTS(dispatcherID, ts)
	return nil
}

func (s *schemaStore) UnregisterDispatcher(dispatcherID common.DispatcherID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, ok := s.dispatchersMap[dispatcherID]
	if !ok {
		return errors.New("dispatcher not found")
	}
	tableID := info.tableID
	delete(s.dispatchersMap, dispatcherID)
	store := s.tableInfoStoreMap[tableID]
	removed := store.unregisterDispatcher(dispatcherID)
	if removed {
		delete(s.tableInfoStoreMap, tableID)
	}
	return nil
}

func (s *schemaStore) GetMaxFinishedDDLTS() common.Ts {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.finishedDDLTS
}

// TODO: fix the sleep
func (s *schemaStore) waitResolvedTs(tableID common.TableID, ts common.Ts) {
	start := time.Now()
	for {
		if s.maxResolvedTS.Load() >= ts {
			return
		}
		time.Sleep(time.Millisecond * 100)
		if time.Since(start) > time.Second*5 {
			log.Info("wait resolved ts slow",
				zap.Int64("tableID", tableID),
				zap.Any("ts", ts),
				zap.Uint64("maxResolvedTS", s.maxResolvedTS.Load()),
				zap.Any("time", time.Since(start)))
		}
	}
}

func (s *schemaStore) GetTableInfo(tableID common.TableID, ts common.Ts) (*common.TableInfo, error) {
	s.waitResolvedTs(tableID, ts)
	s.mu.RLock()
	defer s.mu.RUnlock()
	store, ok := s.tableInfoStoreMap[tableID]
	if !ok {
		return nil, fmt.Errorf(fmt.Sprintf("table %d not found", tableID))
	}
	store.waitTableInfoInitialized()
	return store.getTableInfo(ts)
}

func (s *schemaStore) GetNextDDLEvents(id common.TableID, start, end common.Ts) ([]common.DDLEvent, common.Ts, error) {
	return nil, end, nil
}

func (s *schemaStore) GetNextTableTriggerEvents(f filter.Filter, start common.Ts, limit int) ([]common.DDLEvent, common.Ts, error) {
	return nil, s.maxResolvedTS.Load(), nil
}

func (s *schemaStore) writeDDLEvent(ddlEvent DDLEvent) error {
	// log.Info("write ddl event", zap.Any("ddlEvent", ddlEvent))
	s.eventCh <- ddlEvent
	return nil
}

func (s *schemaStore) advanceResolvedTs(resolvedTs common.Ts) error {
	// log.Info("advance resolved ts", zap.Any("resolvedTS", resolvedTs))
	s.eventCh <- resolvedTs
	return nil
}

// TODO: run gc when calling schemaStore.run
func (s *schemaStore) doGC() error {
	// fetch gcTs from upstream
	gcTs := common.Ts(0)
	// TODO: gc databaseMap
	return s.dataStorage.gc(gcTs)
}

// handleTriggerEvent should be called with s.mu.Lock()
func (s *schemaStore) handleTriggerEvent(event DDLEvent) {
	for _, trigger := range s.tableTriggerDispatcherMap {
		if trigger.filter.ShouldDiscardDDL(event.Job.Type, event.Job.SchemaName, event.Job.TableName) {
			continue
		}
		trigger.events = append(trigger.events, event)
	}
}

func handleResolvedDDLJob(job *model.Job, databaseMap DatabaseInfoMap, tableInfoStoreMap TableInfoStoreMap) error {
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
	case model.ActionCreateTables,
		model.ActionCreateTable,
		model.ActionCreateView,
		model.ActionRecoverTable:
		if err := fillSchemaName(job, databaseMap); err != nil {
			return err
		}
		// no dispatcher should register on these kinds of tables?
		// TODO: add a cache for these kinds of newly created tables because they may soon be registered?
		if store, ok := tableInfoStoreMap[job.TableID]; ok {
			// it is possible that it is already registered if the following happens
			// 1. event send to dispatcher manager
			// 2. dispatcher register
			// 3. begin apply ddl to schema store
			store.applyDDL(job)
		}
		return nil
	default:
		if err := fillSchemaName(job, databaseMap); err != nil {
			return err
		}
		tableID := job.TableID
		store, ok := tableInfoStoreMap[tableID]
		if !ok {
			log.Warn("table not found",
				zap.Any("tableID", tableID),
				zap.Any("job", job))
			return nil
		}
		store.applyDDL(job)
	}

	return nil
}

func fillSchemaName(job *model.Job, databaseMap DatabaseInfoMap) error {
	schemaID := job.SchemaID
	databaseInfo, ok := databaseMap[schemaID]
	if !ok {
		log.Error("database not found", zap.Any("schemaID", schemaID))
		return errors.New("database not found")
	}
	if databaseInfo.CreateVersion > job.BinlogInfo.FinishedTS {
		return errors.New("database is not created")
	}
	if databaseInfo.DeleteVersion < job.BinlogInfo.FinishedTS {
		return errors.New("database is deleted")
	}
	job.SchemaName = databaseInfo.Name
	return nil
}

func createSchema(job *model.Job, databaseMap DatabaseInfoMap) error {
	if _, ok := databaseMap[job.SchemaID]; ok {
		return errors.New("database already exists")
	}
	databaseInfo := &DatabaseInfo{
		Name:          job.SchemaName,
		Tables:        make([]common.TableID, 0),
		CreateVersion: job.BinlogInfo.FinishedTS,
		DeleteVersion: math.MaxUint64,
	}
	databaseMap[job.SchemaID] = databaseInfo
	return nil
}

func dropSchema(job *model.Job, databaseMap DatabaseInfoMap) error {
	databaseInfo, ok := databaseMap[job.SchemaID]
	if !ok {
		return errors.New("database not found")
	}
	if databaseInfo.isDeleted() {
		return errors.New("database is already deleted")
	}
	databaseInfo.DeleteVersion = job.BinlogInfo.FinishedTS
	return nil
}
