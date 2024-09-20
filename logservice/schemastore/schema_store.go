package schemastore

import (
	"context"
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

	RegisterTable(tableID int64, startTs uint64) error

	UnregisterTable(tableID int64) error

	GetTableInfo(tableID int64, ts uint64) (*common.TableInfo, error)

	// FetchTableDDLEvents returns the next ddl events which finishedTs are within the range (start, end]
	// and it returns a timestamp which means there will be no more ddl events before(<=) it
	// TODO: add a parameter limit
	FetchTableDDLEvents(tableID int64, tableFilter filter.Filter, start, end uint64) ([]common.DDLEvent, uint64, error)

	FetchTableTriggerDDLEvents(tableFilter filter.Filter, start uint64, limit int) ([]common.DDLEvent, uint64, error)
}

type schemaStore struct {
	ddlJobFetcher *ddlJobFetcher

	// store unresolved ddl event in memory, it is thread safe
	unsortedCache *ddlCache

	// store ddl event and other metadata on disk, it is thread safe
	dataStorage *persistentStorage

	notifyCh chan interface{}

	// resolved ts pending for apply
	pendingResolvedTs atomic.Uint64

	// max resolvedTs of all applied ddl events
	resolvedTs atomic.Uint64

	// the following two fields are used to filter out duplicate ddl events
	// they will just be updated and read by a single goroutine, so no lock is needed

	// max finishedTs of all applied ddl events
	finishedDDLTs uint64
	// max schemaVersion of all applied ddl events
	schemaVersion int64
}

func New(
	ctx context.Context,
	root string,
	pdCli pd.Client,
	regionCache *tikv.RegionCache,
	pdClock pdutil.Clock,
	kvStorage kv.Storage,
) SchemaStore {
	dataStorage := newPersistentStorage(ctx, root, pdCli, kvStorage)
	upperBound := dataStorage.getUpperBound()

	s := &schemaStore{
		unsortedCache: newDDLCache(),
		dataStorage:   dataStorage,
		notifyCh:      make(chan interface{}, 4),
		finishedDDLTs: upperBound.FinishedDDLTs,
		schemaVersion: upperBound.SchemaVersion,
	}
	s.pendingResolvedTs.Store(upperBound.ResolvedTs)
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
	tryUpdateResolvedTs := func() {
		pendingTs := s.pendingResolvedTs.Load()
		if pendingTs <= s.resolvedTs.Load() {
			return
		}
		resolvedEvents := s.unsortedCache.fetchSortedDDLEventBeforeTS(pendingTs)
		if len(resolvedEvents) != 0 {
			log.Info("schema store begin to apply resolved ddl events",
				zap.Uint64("resolvedTs", pendingTs),
				zap.Int("resolvedEventsLen", len(resolvedEvents)))

			validEvents := make([]PersistedDDLEvent, 0, len(resolvedEvents))

			for _, event := range resolvedEvents {
				// TODO: build persisted ddl event after filter
				if event.Job.BinlogInfo.SchemaVersion <= s.schemaVersion || event.Job.BinlogInfo.FinishedTS <= s.finishedDDLTs {
					log.Info("skip already applied ddl job",
						zap.String("job", event.Job.Query),
						zap.Int64("jobSchemaVersion", event.Job.BinlogInfo.SchemaVersion),
						zap.Uint64("jobFinishTs", event.Job.BinlogInfo.FinishedTS),
						zap.Any("schemaVersion", s.schemaVersion),
						zap.Uint64("finishedDDLTS", s.finishedDDLTs))
					continue
				}
				validEvents = append(validEvents, buildPersistedDDLEventFromJob(event.Job))
				// need to update the following two members for every event to filter out later duplicate events
				s.schemaVersion = event.Job.BinlogInfo.SchemaVersion
				s.finishedDDLTs = event.Job.BinlogInfo.FinishedTS
			}
			s.dataStorage.handleSortedDDLEvents(validEvents...)
		}
		// TODO: resolved ts are updated after ddl events written to disk, do we need to optimize it?
		s.resolvedTs.Store(pendingTs)
		s.dataStorage.updateUpperBound(UpperBoundMeta{
			FinishedDDLTs: s.finishedDDLTs,
			SchemaVersion: s.schemaVersion,
			ResolvedTs:    pendingTs,
		})
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			tryUpdateResolvedTs()
		case <-s.notifyCh:
			tryUpdateResolvedTs()
		}
	}
}

func (s *schemaStore) GetAllPhysicalTables(snapTs uint64, filter filter.Filter) ([]common.Table, error) {
	s.waitResolvedTs(0, snapTs, 10*time.Second)
	return s.dataStorage.getAllPhysicalTables(snapTs, filter)
}

func (s *schemaStore) RegisterTable(tableID int64, startTs uint64) error {
	s.waitResolvedTs(tableID, startTs, 5*time.Second)
	return s.dataStorage.registerTable(tableID, startTs)
}

func (s *schemaStore) UnregisterTable(tableID int64) error {
	return s.dataStorage.unregisterTable(tableID)
}

func (s *schemaStore) GetTableInfo(tableID int64, ts uint64) (*common.TableInfo, error) {
	s.waitResolvedTs(tableID, ts, 2*time.Second)
	return s.dataStorage.getTableInfo(tableID, ts)
}

func (s *schemaStore) FetchTableDDLEvents(tableID int64, tableFilter filter.Filter, start, end uint64) ([]common.DDLEvent, uint64, error) {
	currentResolvedTs := s.resolvedTs.Load()
	if currentResolvedTs < end {
		end = currentResolvedTs
	}
	// TODO: remove the following log
	log.Debug("FetchTableDDLEvents",
		zap.Int64("tableID", tableID),
		zap.Uint64("start", start),
		zap.Uint64("end", end))
	defer log.Debug("FetchTableDDLEvents end",
		zap.Int64("tableID", tableID),
		zap.Uint64("start", start),
		zap.Uint64("end", end))
	events, err := s.dataStorage.fetchTableDDLEvents(tableID, tableFilter, start, end)
	if err != nil {
		return nil, 0, err
	}
	return events, end, nil
}

func (s *schemaStore) FetchTableTriggerDDLEvents(tableFilter filter.Filter, start uint64, limit int) ([]common.DDLEvent, uint64, error) {
	if limit == 0 {
		log.Panic("limit cannot be 0")
	}
	// TODO: remove the following log
	log.Debug("FetchTableTriggerDDLEvents",
		zap.Uint64("start", start),
		zap.Int("limit", limit))
	// must get resolved ts first
	currentResolvedTs := s.resolvedTs.Load()
	events, err := s.dataStorage.fetchTableTriggerDDLEvents(tableFilter, start, limit)
	if err != nil {
		return nil, 0, err
	}
	if len(events) == limit {
		return events, events[limit-1].FinishedTs, nil
	}
	end := currentResolvedTs
	if len(events) > 0 && events[len(events)-1].FinishedTs > currentResolvedTs {
		end = events[len(events)-1].FinishedTs
	}
	log.Debug("FetchTableTriggerDDLEvents end",
		zap.Uint64("start", start),
		zap.Int("limit", limit),
		zap.Uint64("end", end),
		zap.Any("events", events))
	return events, end, nil
}

func (s *schemaStore) writeDDLEvent(ddlEvent DDLJobWithCommitTs) {
	log.Debug("write ddl event",
		zap.Int64("schemaID", ddlEvent.Job.SchemaID),
		zap.Int64("tableID", ddlEvent.Job.TableID),
		zap.Uint64("finishedTs", ddlEvent.Job.BinlogInfo.FinishedTS),
		zap.String("query", ddlEvent.Job.Query))

	// TODO: find a better way to filter out system tables
	if ddlEvent.Job.SchemaID != 1 {
		s.unsortedCache.addDDLEvent(ddlEvent)
	}
}

func (s *schemaStore) advanceResolvedTs(resolvedTs uint64) {
	// log.Info("advance resolved ts", zap.Any("resolvedTS", resolvedTs))
	if resolvedTs < s.pendingResolvedTs.Load() {
		log.Panic("resolved ts should not fallback",
			zap.Uint64("pendingResolveTs", s.pendingResolvedTs.Load()),
			zap.Uint64("newResolvedTs", resolvedTs))
	}
	s.pendingResolvedTs.Store(resolvedTs)
	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
}

// TODO: use notify instead of sleep
func (s *schemaStore) waitResolvedTs(tableID int64, ts uint64, logInterval time.Duration) {
	start := time.Now()
	lastLogTime := time.Now()
	for {
		if s.resolvedTs.Load() >= uint64(ts) {
			return
		}
		time.Sleep(time.Millisecond * 10)
		if time.Since(lastLogTime) > logInterval {
			log.Info("wait resolved ts slow",
				zap.Int64("tableID", tableID),
				zap.Any("ts", ts),
				zap.Uint64("resolvedTS", s.resolvedTs.Load()),
				zap.Any("time", time.Since(start)))
			lastLogTime = time.Now()
		}
	}
}
