package schemastore

import (
	"context"
	"errors"
	"math"
	"sync"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
)

type SchemaStore interface {
	// WriteDDLEvent
	// Note that ddlEvent won't come in order
	WriteDDLEvent(ddlEvent DDLEvent) error

	AdvanceResolvedTS(resolvedTS Timestamp) error

	DoGC(gcTS Timestamp) error

	GetAllPhysicalTables(dispatcherID DispatcherID, filter Filter, ts Timestamp) ([]TableID, error)

	// RegisterDispatcher register the dispatcher into the schema store.
	// todo: how to deal with TableEventDispatcher, use a different interface?
	RegisterDispatcher(dispatcherID DispatcherID, tableID TableID, filter Filter, ts Timestamp) error

	UpdateDispatcherSendTS(dispatcherID DispatcherID, ts Timestamp) error

	UnregisterDispatcher(dispatcherID DispatcherID) error

	GetMaxFinishedDDLTS() Timestamp

	GetTableInfo(tableID TableID, ts Timestamp) (*common.TableInfo, error)

	GetNextDDLEvent(dispatcherID DispatcherID) (*DDLEvent, Timestamp, error)
}

type schemaStore struct {
	// store unresolved ddl event in memory, it is thread safe
	unsortedCache *unsortedDDLCache

	// store ddl event and other metadata on disk, it is thread safe
	dataStorage *persistentStorage

	eventCh chan interface{}

	// all following fields are guarded by this mutex
	mu sync.Mutex

	// max finishedTS of all applied ddl events
	finishedDDLTS Timestamp
	// max schemaVersion of all applied ddl events
	schemaVersion int64

	// databaseID -> database info
	// it contains all databases
	databaseMap DatabaseInfoMap

	// tableID -> versioned store
	// it just contains tables which have registered dispatchers
	tableInfoStoreMap TableInfoStoreMap

	// dispatcherID -> dispatch info
	// TODO: how to deal with table event dispatchers？
	dispatchersMap DispatcherInfoMap
}

func NewSchemaStore(root string, storage kv.Storage, minRequiredTS Timestamp) (SchemaStore, Timestamp, error) {
	dataStorage, metaTS, databaseMap := newPersistentStorage(root, storage, minRequiredTS)

	s := &schemaStore{
		unsortedCache:     newUnSortedDDLCache(),
		dataStorage:       dataStorage,
		eventCh:           make(chan interface{}, 1024),
		finishedDDLTS:     metaTS.finishedDDLTS,
		schemaVersion:     int64(metaTS.schemaVersion),
		databaseMap:       databaseMap,
		tableInfoStoreMap: make(TableInfoStoreMap),
		dispatchersMap:    make(DispatcherInfoMap),
	}
	ctx := context.Background()
	// TODO: cancel this goroutine at exit
	go s.run(ctx)

	return s, metaTS.resolvedTS, nil
}

func (s *schemaStore) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case data := <-s.eventCh:
			switch v := data.(type) {
			case DDLEvent:
				s.unsortedCache.addDDLEvent(v)
				// TODO: batch ddl event
				err := s.dataStorage.writeDDLEvent(v)
				if err != nil {
					log.Fatal("write ddl event failed", zap.Error(err))
				}
			case Timestamp:
				// TODO: check resolved ts is monotonically increasing
				resolvedEvents := s.unsortedCache.fetchSortedDDLEventBeforeTS(v)
				if len(resolvedEvents) == 0 {
					continue
				}
				// TODO: whether the events is ordered by finishedDDLTS and schemaVersion
				newFinishedDDLTS := resolvedEvents[len(resolvedEvents)-1].Job.BinlogInfo.FinishedTS
				newSchemaVersion := resolvedEvents[len(resolvedEvents)-1].Job.Version
				err := s.dataStorage.updateStoreMeta(v, Timestamp(newFinishedDDLTS), Timestamp(newSchemaVersion))
				if err != nil {
					log.Fatal("update ts failed", zap.Error(err))
				}
				s.mu.Lock()
				defer s.mu.Unlock()
				for _, event := range resolvedEvents {
					if event.Job.Version <= s.schemaVersion || event.Job.BinlogInfo.FinishedTS <= uint64(s.finishedDDLTS) {
						log.Warn("skip already applied ddl job",
							zap.Any("job", event.Job),
							zap.Any("schemaVersion", s.schemaVersion),
							zap.Any("finishedDDLTS", s.finishedDDLTS))
						continue
					}
					if err := handleResolvedDDLJob(event.Job, s.databaseMap, s.tableInfoStoreMap); err != nil {
						return err
					}
					s.schemaVersion = event.Job.Version
					s.finishedDDLTS = Timestamp(event.Job.BinlogInfo.FinishedTS)
				}
			default:
				log.Fatal("unknown event type")
			}
		}
	}
}

func (s *schemaStore) WriteDDLEvent(ddlEvent DDLEvent) error {
	log.Info("write ddl event", zap.Any("ddlEvent", ddlEvent))
	s.eventCh <- ddlEvent
	return nil
}

func (s *schemaStore) AdvanceResolvedTS(resolvedTS Timestamp) error {
	log.Info("advance resolved ts", zap.Any("resolvedTS", resolvedTS))
	s.eventCh <- resolvedTS
	return nil
}

func (s *schemaStore) DoGC(gcTS Timestamp) error {
	// TODO: gc databaseMap
	return s.dataStorage.gc(gcTS)
}

func (s *schemaStore) GetAllPhysicalTables(dispatcherID DispatcherID, filter Filter, ts Timestamp) ([]TableID, error) {
	return nil, nil
}

func (s *schemaStore) RegisterDispatcher(
	dispatcherID DispatcherID, tableID TableID, filter Filter, startTS Timestamp,
) error {
	s.mu.Lock()
	if startTS < s.dataStorage.getGCTS() {
		return errors.New("start ts is old than gc ts")
	}
	s.dispatchersMap[dispatcherID] = DispatcherInfo{
		tableID: tableID,
		filter:  filter,
	}
	getSchemaName := func(schemaID SchemaID) (string, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		databaseInfo, ok := s.databaseMap[DatabaseID(schemaID)]
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
	defer s.mu.Unlock()
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

func (s *schemaStore) UpdateDispatcherSendTS(dispatcherID DispatcherID, ts Timestamp) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, ok := s.dispatchersMap[dispatcherID]
	if !ok {
		return errors.New("dispatcher not found")
	}
	store := s.tableInfoStoreMap[TableID(info.tableID)]
	store.updateDispatcherSendTS(dispatcherID, ts)
	return nil
}

func (s *schemaStore) UnregisterDispatcher(dispatcherID DispatcherID) error {
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

func (s *schemaStore) GetMaxFinishedDDLTS() Timestamp {
	s.mu.Lock()
	defer s.mu.Lock()
	return s.finishedDDLTS
}

func (s *schemaStore) GetTableInfo(tableID TableID, ts Timestamp) (*common.TableInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	store, ok := s.tableInfoStoreMap[tableID]
	if !ok {
		return nil, errors.New("table not found")
	}
	store.waitTableInfoInitialized()
	return store.getTableInfo(ts)
}

func (s *schemaStore) GetNextDDLEvent(dispatcherID DispatcherID) (*DDLEvent, Timestamp, error) {
	return nil, 0, nil
}

func handleResolvedDDLJob(job *model.Job, databaseMap DatabaseInfoMap, tableInfoStoreMap TableInfoStoreMap) error {
	if err := fillSchemaName(job, databaseMap); err != nil {
		return err
	}

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
		// no dispatcher should register on these kinds of tables?
		// TODO: add a cache for these kinds of newly created tables because they may soon be registered?
		if _, ok := tableInfoStoreMap[TableID(job.TableID)]; ok {
			log.Panic("should not happened")
		}
		return nil
	default:
		tableID := TableID(job.TableID)
		store, ok := tableInfoStoreMap[tableID]
		if !ok {
			return errors.New("table not found")
		}
		store.applyDDL(job)
	}

	return nil
}

func fillSchemaName(job *model.Job, databaseMap DatabaseInfoMap) error {
	databaseID := DatabaseID(job.SchemaID)
	databaseInfo, ok := databaseMap[databaseID]
	if !ok {
		return errors.New("database not found")
	}
	if databaseInfo.CreateVersion > Timestamp(job.BinlogInfo.FinishedTS) {
		return errors.New("database is not created")
	}
	if databaseInfo.DeleteVersion < Timestamp(job.BinlogInfo.FinishedTS) {
		return errors.New("database is deleted")
	}
	job.SchemaName = databaseInfo.Name
	return nil
}

func createSchema(job *model.Job, databaseMap DatabaseInfoMap) error {
	if _, ok := databaseMap[DatabaseID(job.SchemaID)]; ok {
		return errors.New("database already exists")
	}
	databaseInfo := &DatabaseInfo{
		Name:          job.SchemaName,
		Tables:        make([]TableID, 0),
		CreateVersion: Timestamp(job.BinlogInfo.FinishedTS),
		DeleteVersion: math.MaxUint64,
	}
	databaseMap[DatabaseID(job.SchemaID)] = databaseInfo
	return nil
}

func dropSchema(job *model.Job, databaseMap DatabaseInfoMap) error {
	databaseInfo, ok := databaseMap[DatabaseID(job.SchemaID)]
	if !ok {
		return errors.New("database not found")
	}
	if databaseInfo.isDeleted() {
		return errors.New("database is already deleted")
	}
	databaseInfo.DeleteVersion = Timestamp(job.BinlogInfo.FinishedTS)
	return nil
}
