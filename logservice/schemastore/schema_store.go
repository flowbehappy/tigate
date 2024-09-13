package schemastore

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type SchemaStore interface {
	common.SubModule

	GetAllPhysicalTables(snapTs uint64, filter filter.Filter) ([]common.Table, error)

	RegisterTable(tableID int64) error

	UnregisterTable(tableID int64) error

	GetTableInfo(tableID int64, ts uint64) (*common.TableInfo, error)

	// FetchTableDDLEvents returns the next ddl events which finishedTs are within the range (start, end]
	// and it returns a timestamp which means there will be no more ddl events before(<=) it
	// TODO: add a parameter limit
	FetchTableDDLEvents(tableID int64, start, end uint64) ([]common.DDLEvent, uint64)

	FetchTableTriggerDDLEvents(tableFilter filter.Filter, start uint64, limit int) ([]common.DDLEvent, uint64)
}

type schemaStore struct {
	ddlJobFetcher *ddlJobFetcher

	// store unresolved ddl event in memory, it is thread safe
	unsortedCache *ddlCache

	// store ddl event and other metadata on disk, it is thread safe
	dataStorage *persistentStorage

	// TODO: remove it and update resolve ts periodically
	eventCh chan interface{}

	// pending resolved ts for apply
	pendingResolveTs atomic.Uint64

	// max resolvedTs of all applied ddl events
	resolvedTs atomic.Uint64

	// all following fields are guarded by this mutex
	mu sync.Mutex

	// the following two fields are used to filter out duplicate ddl events
	// max finishedTs of all applied ddl events
	finishedDDLTs uint64
	// max schemaVersion of all applied ddl events
	schemaVersion int64
}

func NewSchemaStore(
	ctx context.Context,
	root string,
	pdCli pd.Client,
	regionCache *tikv.RegionCache,
	pdClock pdutil.Clock,
	kvStorage kv.Storage,
) SchemaStore {
	dataStorage, upperBound := newPersistentStorage(ctx, root, pdCli, kvStorage)

	s := &schemaStore{
		unsortedCache: newDDLCache(),
		dataStorage:   dataStorage,
		eventCh:       make(chan interface{}, 1024),
		finishedDDLTs: upperBound.FinishedDDLTs,
		schemaVersion: upperBound.SchemaVersion,
	}
	s.pendingResolveTs.Store(upperBound.ResolvedTs)
	s.resolvedTs.Store(upperBound.ResolvedTs)

	log.Info("new schema store",
		zap.Uint64("resolvedTs", s.resolvedTs.Load()),
		zap.Uint64("finishedDDLTS", s.finishedDDLTs),
		zap.Int64("schemaVersion", s.schemaVersion))
	s.ddlJobFetcher = newDDLJobFetcher(
		pdCli,
		regionCache,
		pdClock,
		kvStorage,
		upperBound.ResolvedTs,
		s.writeDDLEvent,
		s.advanceResolvedTs)
	return s
}

func (s *schemaStore) Name() string {
	return appcontext.SchemaStore
}

func (s *schemaStore) Run(ctx context.Context) error {
	log.Info("schema store begin to run")
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return s.updateResolvedTsPeriodically(ctx)
	})
	eg.Go(func() error {
		return s.ddlJobFetcher.run(ctx)
	})
	return eg.Wait()
}

func (s *schemaStore) Close(ctx context.Context) error {
	log.Info("schema store closed")
	return s.ddlJobFetcher.close(ctx)
}

func (s *schemaStore) updateResolvedTsPeriodically(ctx context.Context) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pendingTs := s.pendingResolveTs.Load()
			if pendingTs > s.resolvedTs.Load() {
				resolvedEvents := s.unsortedCache.fetchSortedDDLEventBeforeTS(pendingTs)
				if len(resolvedEvents) != 0 {
					log.Info("schema store begin to apply resolved ddl events",
						zap.Uint64("resolvedTs", pendingTs),
						zap.Int("resolvedEventsLen", len(resolvedEvents)))

					// TODO: avoid allocate a new slice if event in resolvedEvents is all valid
					validEvents := make([]PersistedDDLEvent, 0, len(resolvedEvents))
					s.mu.Lock()
					for _, event := range resolvedEvents {
						// TODO: build persisted ddl event after filter
						if event.SchemaVersion <= s.schemaVersion || event.FinishedTs <= s.finishedDDLTs {
							log.Info("skip already applied ddl job",
								zap.String("job", event.Query),
								zap.Int64("jobSchemaVersion", event.SchemaVersion),
								zap.Uint64("jobFinishTs", event.FinishedTs),
								zap.Any("schemaVersion", s.schemaVersion),
								zap.Uint64("finishedDDLTS", s.finishedDDLTs))
							continue
						}
						validEvents = append(validEvents, event)
						// need to update the following two members for every event to filter out later duplicate events
						s.schemaVersion = event.SchemaVersion
						s.finishedDDLTs = event.FinishedTs
					}
					s.mu.Unlock()

					s.dataStorage.handleSortedDDLEvents(validEvents...)
				}
				s.resolvedTs.Store(pendingTs)
			}
		}
	}
}

func (s *schemaStore) GetAllPhysicalTables(snapTs uint64, filter filter.Filter) ([]common.Table, error) {
	return s.dataStorage.getAllPhysicalTables(snapTs, filter)
}

func (s *schemaStore) RegisterTable(tableID int64) error {
	return s.dataStorage.registerTable(tableID)
}

func (s *schemaStore) UnregisterTable(tableID int64) error {
	return s.dataStorage.unregisterTable(tableID)
}

func (s *schemaStore) GetTableInfo(tableID int64, ts uint64) (*common.TableInfo, error) {
	s.waitResolvedTs(tableID, ts)
	return s.dataStorage.getTableInfo(tableID, ts)
}

func (s *schemaStore) FetchTableDDLEvents(tableID int64, start, end uint64) ([]common.DDLEvent, uint64) {
	currentResolvedTs := s.resolvedTs.Load()
	if currentResolvedTs < end {
		end = currentResolvedTs
	}
	return s.dataStorage.fetchTableDDLEvents(tableID, start, end), end
}

func (s *schemaStore) FetchTableTriggerDDLEvents(tableFilter filter.Filter, start uint64, limit int) ([]common.DDLEvent, uint64) {
	if limit == 0 {
		log.Panic("limit cannot be 0")
	}
	// must get resolved ts first
	currentResolvedTs := s.resolvedTs.Load()
	events := s.dataStorage.fetchTableTriggerDDLEvents(tableFilter, start, limit)
	if len(events) == limit {
		return events, events[limit-1].FinishedTs
	}
	end := currentResolvedTs
	if len(events) > 0 && events[len(events)-1].FinishedTs > currentResolvedTs {
		end = events[len(events)-1].FinishedTs
	}
	return events, end
}

func (s *schemaStore) writeDDLEvent(ddlEvent PersistedDDLEvent) {
	log.Debug("write ddl event",
		zap.Int64("schemaID", ddlEvent.SchemaID),
		zap.Int64("tableID", ddlEvent.TableID),
		zap.Uint64("finishedTs", ddlEvent.FinishedTs),
		zap.String("query", ddlEvent.Query))

	// TODO: find a better way to filter out system tables
	if ddlEvent.SchemaID != 1 {
		s.unsortedCache.addDDLEvent(ddlEvent)
	}
}

func (s *schemaStore) advanceResolvedTs(resolvedTs uint64) {
	// log.Info("advance resolved ts", zap.Any("resolvedTS", resolvedTs))
	if resolvedTs < s.pendingResolveTs.Load() {
		log.Panic("resolved ts should not fallback",
			zap.Uint64("pendingResolveTs", s.pendingResolveTs.Load()),
			zap.Uint64("newResolvedTs", resolvedTs))
	}
	s.pendingResolveTs.Store(resolvedTs)
}

// TODO: use notify instead of sleep
func (s *schemaStore) waitResolvedTs(tableID int64, ts uint64) {
	start := time.Now()
	for {
		if s.resolvedTs.Load() >= uint64(ts) {
			return
		}
		time.Sleep(time.Millisecond * 100)
		if time.Since(start) > time.Second*5 {
			log.Info("wait resolved ts slow",
				zap.Int64("tableID", tableID),
				zap.Any("ts", ts),
				zap.Uint64("resolvedTS", s.resolvedTs.Load()),
				zap.Any("time", time.Since(start)))
		}
	}
}
