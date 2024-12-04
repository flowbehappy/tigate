package schemastore

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type ResolveTsUpdateNotifier func()

type SchemaStore interface {
	common.SubModule

	GetAllPhysicalTables(snapTs uint64, filter filter.Filter) ([]commonEvent.Table, error)

	RegisterTable(dispatcherID common.DispatcherID, tableID int64, startTs uint64, notifier ResolveTsUpdateNotifier) error

	UnregisterTable(dispatcherID common.DispatcherID, tableID int64) error

	// return table info with largest version <= ts
	GetTableInfo(tableID int64, ts uint64) (*common.TableInfo, error)

	// TODO: how to respect tableFilter
	GetTableDDLEventState(tableID int64) DDLEventState

	// FetchTableDDLEvents returns the next ddl events which finishedTs are within the range (start, end]
	// The caller must ensure end <= current resolvedTs
	// TODO: add a parameter limit
	FetchTableDDLEvents(tableID int64, tableFilter filter.Filter, start, end uint64) ([]commonEvent.DDLEvent, error)

	FetchTableTriggerDDLEvents(tableFilter filter.Filter, start uint64, limit int) ([]commonEvent.DDLEvent, uint64, error)
}

type DDLEventState struct {
	ResolvedTs       uint64
	MaxEventCommitTs uint64
}

type schemaStore struct {
	pdClock pdutil.Clock

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

	resolveTsUpdateNotifiers sync.Map

	notifyDispatcherCh chan interface{}

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
		pdClock:            pdClock,
		unsortedCache:      newDDLCache(),
		dataStorage:        dataStorage,
		notifyCh:           make(chan interface{}, 4),
		finishedDDLTs:      upperBound.FinishedDDLTs,
		schemaVersion:      upperBound.SchemaVersion,
		notifyDispatcherCh: make(chan interface{}, 4),
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
		return s.notifyDispatcherPeriodically(ctx)
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
		defer func() {
			currentPhyTs := oracle.GetPhysical(s.pdClock.CurrentTime())
			resolvedPhyTs := oracle.ExtractPhysical(pendingTs)
			resolvedLag := float64(currentPhyTs-resolvedPhyTs) / 1e3
			metrics.SchemaStoreResolvedTsLagGauge.Set(float64(resolvedLag))
		}()

		if pendingTs <= s.resolvedTs.Load() {
			return
		}
		resolvedEvents := s.unsortedCache.fetchSortedDDLEventBeforeTS(pendingTs)
		if len(resolvedEvents) != 0 {
			log.Info("schema store begin to apply resolved ddl events",
				zap.Uint64("resolvedTs", pendingTs),
				zap.Int("resolvedEventsLen", len(resolvedEvents)))

			for _, event := range resolvedEvents {
				if event.Job.BinlogInfo.FinishedTS <= s.finishedDDLTs {
					// log.Info("skip already applied ddl job",
					// 	zap.Any("type", event.Job.Type),
					// 	zap.String("job", event.Job.Query),
					// 	zap.Int64("jobSchemaVersion", event.Job.BinlogInfo.SchemaVersion),
					// 	zap.Uint64("jobFinishTs", event.Job.BinlogInfo.FinishedTS),
					// 	zap.Uint64("jobCommitTs", event.CommitTs),
					// 	zap.Any("storeSchemaVersion", s.schemaVersion),
					// 	zap.Uint64("storeFinishedDDLTS", s.finishedDDLTs))
					continue
				}
				log.Info("handle ddl job",
					zap.Int64("schemaID", event.Job.SchemaID),
					zap.Int64("tableID", event.Job.TableID),
					zap.Any("type", event.Job.Type),
					zap.String("job", event.Job.Query),
					zap.Int64("jobSchemaVersion", event.Job.BinlogInfo.SchemaVersion),
					zap.Uint64("jobFinishTs", event.Job.BinlogInfo.FinishedTS),
					zap.Uint64("jobCommitTs", event.CommitTs),
					zap.Any("storeSchemaVersion", s.schemaVersion),
					zap.Uint64("storeFinishedDDLTS", s.finishedDDLTs))

				// need to update the following two members for every event to filter out later duplicate events
				s.schemaVersion = event.Job.BinlogInfo.SchemaVersion
				s.finishedDDLTs = event.Job.BinlogInfo.FinishedTS

				s.dataStorage.handleDDLJob(event.Job)
			}
		}
		// When register a new table, it will load all ddl jobs from disk for the table,
		// so we can only update resolved ts after all ddl jobs are written to disk
		// Can we optimize it to update resolved ts more eagerly?
		s.resolvedTs.Store(pendingTs)
		select {
		case s.notifyDispatcherCh <- struct{}{}:
		default:
		}
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

func (s *schemaStore) notifyDispatcherPeriodically(ctx context.Context) error {
	// ticker := time.NewTicker(50 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.notifyDispatcherCh:
			s.resolveTsUpdateNotifiers.Range(func(_, value interface{}) bool {
				value.(ResolveTsUpdateNotifier)()
				return true
			})
		}
	}
}

func (s *schemaStore) GetAllPhysicalTables(snapTs uint64, filter filter.Filter) ([]commonEvent.Table, error) {
	s.waitResolvedTs(0, snapTs, 10*time.Second)
	return s.dataStorage.getAllPhysicalTables(snapTs, filter)
}

func (s *schemaStore) RegisterTable(
	dispatcherID common.DispatcherID,
	tableID int64,
	startTs uint64,
	notifier ResolveTsUpdateNotifier,
) error {
	metrics.SchemaStoreResolvedRegisterTableGauge.Inc()
	s.resolveTsUpdateNotifiers.Store(dispatcherID, notifier)
	s.waitResolvedTs(tableID, startTs, 5*time.Second)
	log.Info("register table",
		zap.Int64("tableID", tableID),
		zap.Uint64("startTs", startTs),
		zap.Uint64("resolvedTs", s.resolvedTs.Load()))
	return s.dataStorage.registerTable(tableID, startTs)
}

func (s *schemaStore) UnregisterTable(dispatcherID common.DispatcherID, tableID int64) error {
	metrics.SchemaStoreResolvedRegisterTableGauge.Dec()
	s.resolveTsUpdateNotifiers.Delete(dispatcherID)
	return s.dataStorage.unregisterTable(tableID)
}

func (s *schemaStore) GetTableInfo(tableID int64, ts uint64) (*common.TableInfo, error) {
	metrics.SchemaStoreGetTableInfoCounter.Inc()
	start := time.Now()
	defer func() {
		metrics.SchemaStoreGetTableInfoLagHist.Observe(time.Since(start).Seconds())
	}()
	s.waitResolvedTs(tableID, ts, 2*time.Second)
	return s.dataStorage.getTableInfo(tableID, ts)
}

func (s *schemaStore) GetTableDDLEventState(tableID int64) DDLEventState {
	resolvedTs := s.resolvedTs.Load()
	maxEventCommitTs := s.dataStorage.getMaxEventCommitTs(tableID, resolvedTs)
	return DDLEventState{
		ResolvedTs:       resolvedTs,
		MaxEventCommitTs: maxEventCommitTs,
	}
}

func (s *schemaStore) FetchTableDDLEvents(tableID int64, tableFilter filter.Filter, start, end uint64) ([]commonEvent.DDLEvent, error) {
	currentResolvedTs := s.resolvedTs.Load()
	if end > currentResolvedTs {
		log.Panic("end should not be greater than current resolved ts",
			zap.Uint64("end", end),
			zap.Uint64("currentResolvedTs", currentResolvedTs))
	}
	events, err := s.dataStorage.fetchTableDDLEvents(tableID, tableFilter, start, end)
	if err != nil {
		return nil, err
	}
	return events, nil
}

// FetchTableTriggerDDLEvents returns the next ddl events which finishedTs are within the range (start, end]
func (s *schemaStore) FetchTableTriggerDDLEvents(tableFilter filter.Filter, start uint64, limit int) ([]commonEvent.DDLEvent, uint64, error) {
	if limit == 0 {
		log.Panic("limit cannot be 0")
	}
	// TODO: remove the following log
	log.Debug("FetchTableTriggerDDLEvents",
		zap.Uint64("start", start),
		zap.Int("limit", limit))
	// must get resolved ts first
	currentResolvedTs := s.resolvedTs.Load()
	if currentResolvedTs <= start {
		return nil, currentResolvedTs, nil
	}

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
