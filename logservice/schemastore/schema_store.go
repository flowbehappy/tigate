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

	GetAllPhysicalTables(snapTs common.Ts, filter filter.Filter) ([]common.Table, error)

	RegisterTable(tableID common.TableID) error

	UnregisterTable(tableID common.TableID) error

	GetTableInfo(tableID common.TableID, ts common.Ts) (*common.TableInfo, error)

	// GetNextDDLEvents returns the next ddl event which finishedTs is within the range (start, end]
	GetNextDDLEvents(id common.TableID, start, end common.Ts) ([]common.DDLEvent, common.Ts, error)

	GetNextTableTriggerEvents(f filter.Filter, start common.Ts, limit int) ([]common.DDLEvent, common.Ts, error)
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
					validEvents := make([]DDLEvent, 0, len(resolvedEvents))
					s.mu.Lock()
					for _, event := range resolvedEvents {
						if event.Job.BinlogInfo.SchemaVersion <= s.schemaVersion || event.Job.BinlogInfo.FinishedTS <= s.finishedDDLTs {
							log.Info("skip already applied ddl job",
								zap.String("job", event.Job.Query),
								zap.Int64("jobSchemaVersion", event.Job.BinlogInfo.SchemaVersion),
								zap.Uint64("jobFinishTs", event.Job.BinlogInfo.FinishedTS),
								zap.Any("schemaVersion", s.schemaVersion),
								zap.Uint64("finishedDDLTS", s.finishedDDLTs))
							continue
						}
						validEvents = append(validEvents, event)
						// need to update the following two members for every event to filter out later duplicate events
						s.schemaVersion = event.Job.BinlogInfo.SchemaVersion
						s.finishedDDLTs = event.Job.BinlogInfo.FinishedTS
					}
					s.mu.Unlock()

					s.dataStorage.handleSortedDDLEvents(validEvents...)
				}
				s.resolvedTs.Store(pendingTs)
			}
		}
	}
}

func (s *schemaStore) GetAllPhysicalTables(snapTs common.Ts, filter filter.Filter) ([]common.Table, error) {
	return s.dataStorage.getAllPhysicalTables(snapTs, filter)
}

func (s *schemaStore) RegisterTable(tableID common.TableID) error {
	return s.dataStorage.registerTable(tableID)
}

func (s *schemaStore) UnregisterTable(tableID common.TableID) error {
	return s.dataStorage.unregisterTable(tableID)
}

func (s *schemaStore) GetTableInfo(tableID common.TableID, ts common.Ts) (*common.TableInfo, error) {
	s.waitResolvedTs(tableID, ts)
	return s.dataStorage.getTableInfo(tableID, ts)
}

func (s *schemaStore) GetNextDDLEvents(id common.TableID, start, end common.Ts) ([]common.DDLEvent, common.Ts, error) {
	return nil, end, nil
}

func (s *schemaStore) GetNextTableTriggerEvents(f filter.Filter, start common.Ts, limit int) ([]common.DDLEvent, common.Ts, error) {
	return nil, s.resolvedTs.Load(), nil
}

func (s *schemaStore) writeDDLEvent(ddlEvent DDLEvent) {
	log.Debug("write ddl event",
		zap.String("schema", ddlEvent.Job.SchemaName),
		zap.String("table", ddlEvent.Job.TableName),
		zap.Uint64("startTs", ddlEvent.Job.StartTS),
		zap.Uint64("finishedTs", ddlEvent.Job.BinlogInfo.FinishedTS),
		zap.String("query", ddlEvent.Job.Query))

	// TODO: find a better way to filter out system tables
	if ddlEvent.Job.SchemaID != 1 {
		s.unsortedCache.addDDLEvent(ddlEvent)
	}
}

func (s *schemaStore) advanceResolvedTs(resolvedTs common.Ts) {
	// log.Info("advance resolved ts", zap.Any("resolvedTS", resolvedTs))
	if resolvedTs < s.pendingResolveTs.Load() {
		log.Panic("resolved ts should not fallback",
			zap.Uint64("pendingResolveTs", s.pendingResolveTs.Load()),
			zap.Uint64("newResolvedTs", uint64(resolvedTs)))
	}
	s.pendingResolveTs.Store(resolvedTs)
}

// TODO: use notify instead of sleep
func (s *schemaStore) waitResolvedTs(tableID common.TableID, ts common.Ts) {
	start := time.Now()
	for {
		if s.resolvedTs.Load() >= uint64(ts) {
			return
		}
		time.Sleep(time.Millisecond * 100)
		if time.Since(start) > time.Second*5 {
			log.Info("wait resolved ts slow",
				zap.Int64("tableID", int64(tableID)),
				zap.Any("ts", ts),
				zap.Uint64("resolvedTS", s.resolvedTs.Load()),
				zap.Any("time", time.Since(start)))
		}
	}
}
