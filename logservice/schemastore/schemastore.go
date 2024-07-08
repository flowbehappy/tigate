package schemastore

import (
	"context"
	"errors"
	"math"
	"sync"

	"github.com/flowbehappy/tigate/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
)

type SchemaStore interface {
	// WriteDDLEvent
	// Note that ddlEvent won't come in order
	WriteDDLEvent(ddlEvent common.DDLEvent) error

	AdvanceResolvedTs(resolvedTs common.Timestamp) error

	DoGC(resolvedTs common.Timestamp) error

	GetAllPhysicalTables(dispatcherID common.DispatcherID, filter common.Filter, ts common.Timestamp) ([]common.TableID, error)

	// RegisterDispatcher register the dispatcher into the schema store.
	// todo: how to deal with TableEventDispatcher, use a different interface?
	RegisterDispatcher(dispatcherID common.DispatcherID, tableID common.TableID, filter common.Filter, ts common.Timestamp) error

	UpdateDispatcherCheckpointTS(dispatcherID common.DispatcherID, ts common.Timestamp) error

	UnregisterDispatcher(dispatcherID common.DispatcherID) error

	ResolvedTS() common.Timestamp

	GetTableInfo(tableID common.TableID, ts common.Timestamp) (*common.TableInfo, error)

	GetNextDDLEvent(dispatcherID common.DispatcherID) (*common.DDLEvent, common.Timestamp, error)
}

type schemaStore struct {
	// thread safe
	unsortedCache *unSortedDDLCache

	// thread safe
	dataStorage *persistentStorage

	mu sync.Mutex

	gcTS       common.Timestamp
	resolvedTS common.Timestamp

	schemaVersion int64

	databaseMap common.DatabaseInfoMap

	// table_id -> versioned store
	tableInfoStoreMap common.TableInfoStoreMap

	dispatchersMap common.DispatchInfoMap

	// truncated tables?

	// how to deal with table event dispatchers？
}

func NewSchemaStore(root string, storage kv.Storage, minRequiredTS common.Timestamp) (SchemaStore, common.Timestamp, error) {
	ctx := context.Background()

	dataStorage, gcTS, resolvedTS := newPersistentStorage(root, storage, minRequiredTS)
	// todo: it looks this goroutine is not controlled by the ctx, shall we cancel it?
	go dataStorage.run(ctx)

	return &schemaStore{
		gcTS:          gcTS,
		resolvedTS:    resolvedTS,
		schemaVersion: 0,
		unsortedCache: newUnSortedDDLCache(),
		dataStorage:   dataStorage,
	}, resolvedTS, nil
}

func (s *schemaStore) WriteDDLEvent(ddlEvent common.DDLEvent) error {
	s.dataStorage.writeDDLEvent(ddlEvent)
	s.unsortedCache.addDDL(ddlEvent)
	return nil
}

func (s *schemaStore) AdvanceResolvedTs(resolvedTS common.Timestamp) error {
	resolvedEvents := s.unsortedCache.getSortedDDLEventBeforeTS(resolvedTS)
	s.mu.Lock()
	if resolvedTS < s.resolvedTS {
		log.Fatal("resolved ts should not go back", zap.Uint64("resolved ts", uint64(resolvedTS)), zap.Uint64("current resolved ts", uint64(s.resolvedTS)))
	}
	for _, event := range resolvedEvents {
		if err := handleResolvedDDLJob(event.Job, s.databaseMap, s.tableInfoStoreMap); err != nil {
			return err
		}
		// todo: check?
		s.schemaVersion = event.Job.Version
	}
	s.resolvedTS = resolvedTS
	s.mu.Unlock()
	s.dataStorage.updateResolvedTS(resolvedTS)
	return nil
}

func (s *schemaStore) DoGC(gcTS common.Timestamp) error {
	// gc databaseMap
	return nil
}

func (s *schemaStore) GetAllPhysicalTables(dispatcherID common.DispatcherID, filter common.Filter, ts common.Timestamp) ([]common.TableID, error) {
	return nil, nil
}

func (s *schemaStore) RegisterDispatcher(
	dispatcherID common.DispatcherID, tableID common.TableID, filter common.Filter, startTS common.Timestamp,
) error {
	s.mu.Lock()
	// check whether there is already a versionedTableInfoStore satisfy the needs
	if oldStore, ok := s.tableInfoStoreMap[tableID]; ok && oldStore.getStartTS() <= startTS {
		oldStore.registerDispatcher(dispatcherID, startTS)
		return nil
	}
	if startTS < s.gcTS {
		return errors.New("start ts is old than gc ts")
	}
	endTS := s.resolvedTS
	s.mu.Unlock()

	fillSchemaNameWrapper := func(job *model.Job) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if err := fillSchemaName(job, s.databaseMap); err != nil {
			return err
		}
		return nil
	}

	// build a new versionedTableInfoStore from disk
	// TODO: there may be multiple dispatchers build the same versionedTableInfoStore, optimize it later
	// empty storage?
	newTableInfoStore := s.dataStorage.buildVersionedTableInfoStore(tableID, startTS, endTS, fillSchemaNameWrapper)
	newTableInfoStore.registerDispatcher(dispatcherID, startTS)

	s.mu.Lock()
	defer s.mu.Unlock()
	// check whether the data is gced again
	if startTS < s.gcTS {
		return errors.New("start ts is old than gc ts")
	}
	oldStore, ok := s.tableInfoStoreMap[tableID]
	if ok {
		// check again whether the oldStore has a more old start ts
		if oldStore.getStartTS() <= startTS {
			oldStore.registerDispatcher(dispatcherID, startTS)
			return nil
		} else {
			newTableInfoStore.checkAndCopyTailFrom(oldStore)
			dispatchers := oldStore.getAllRegisteredDispatchers()
			for dispatcher, ts := range dispatchers {
				newTableInfoStore.registerDispatcher(dispatcher, ts)
			}
		}
	}
	s.tableInfoStoreMap[tableID] = newTableInfoStore
	s.dispatchersMap[dispatcherID] = common.DispatchInfo{
		tableID: tableID,
		filter:  filter,
	}
	return nil
}

func (s *schemaStore) UpdateDispatcherCheckpointTS(dispatcherID common.DispatcherID, ts common.Timestamp) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, ok := s.dispatchersMap[dispatcherID]
	if !ok {
		return errors.New("dispatcher not found")
	}
	store := s.tableInfoStoreMap[info.tableID]
	return store.updateDispatcherCheckpointTS(dispatcherID, ts)
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

func (s *schemaStore) ResolvedTS() common.Timestamp {
	s.mu.Lock()
	defer s.mu.Lock()
	return s.resolvedTS
}

func (s *schemaStore) GetTableInfo(tableID common.TableID, ts common.Timestamp) (*common.TableInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if store, ok := s.tableInfoStoreMap[tableID]; ok {
		return store.getTableInfo(ts)
	}
	return nil, errors.New("table not found")
}

func (s *schemaStore) GetNextDDLEvent(dispatcherID common.DispatcherID) (*common.DDLEvent, common.Timestamp, error) {
	return nil, 0, nil
}

func handleResolvedDDLJob(job *model.Job, databaseMap common.DatabaseInfoMap, tableInfoStoreMap common.TableInfoStoreMap) error {
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
		if _, ok := tableInfoStoreMap[common.TableID(job.TableID)]; ok {
			log.Panic("should not happened")
		}
		return nil
	default:
		tableID := common.TableID(job.TableID)
		store, ok := tableInfoStoreMap[tableID]
		if !ok {
			return errors.New("table not found")
		}
		store.applyDDL(job)
	}

	return nil
}

func fillSchemaName(job *model.Job, databaseMap common.DatabaseInfoMap) error {
	databaseID := common.DatabaseID(job.SchemaID)
	databaseInfo, ok := databaseMap[databaseID]
	if !ok {
		return errors.New("database not found")
	}
	if databaseInfo.CreateVersion > common.Timestamp(job.BinlogInfo.FinishedTS) {
		return errors.New("database is not created")
	}
	if databaseInfo.DeleteVersion < common.Timestamp(job.BinlogInfo.FinishedTS) {
		return errors.New("database is deleted")
	}
	job.SchemaName = databaseInfo.Name
	return nil
}

func createSchema(job *model.Job, databaseMap common.DatabaseInfoMap) error {
	if _, ok := databaseMap[common.DatabaseID(job.SchemaID)]; ok {
		return errors.New("database already exists")
	}
	databaseInfo := &common.DatabaseInfo{
		ID:            job.SchemaID,
		Name:          job.SchemaName,
		Tables:        make([]common.TableID, 0),
		CreateVersion: common.Timestamp(job.BinlogInfo.FinishedTS),
		DeleteVersion: math.MaxUint64,
	}
	databaseMap[common.DatabaseID(job.SchemaID)] = databaseInfo
	return nil
}

func dropSchema(job *model.Job, databaseMap common.DatabaseInfoMap) error {
	databaseInfo, ok := databaseMap[common.DatabaseID(job.SchemaID)]
	if !ok {
		return errors.New("database not found")
	}
	if databaseInfo.isDeleted() {
		return errors.New("database is already deleted")
	}
	databaseInfo.DeleteVersion = common.Timestamp(job.BinlogInfo.FinishedTS)
	return nil
}
