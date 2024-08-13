package eventstore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/logservice/logpuller"
	"github.com/flowbehappy/tigate/logservice/schemastore"
	"github.com/flowbehappy/tigate/mounter"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type EventObserver func(raw *common.RawKVEntry)

type WatermarkNotifier func(watermark uint64)

type EventStore interface {
	Name() string

	Run(ctx context.Context) error

	Close(ctx context.Context) error

	// add a callback to be called when a new event is added to the store;
	// but for old data this is not feasiable? may we can just return a current watermark when register
	RegisterDispatcher(
		dispatcherID common.DispatcherID,
		span *common.TableSpan,
		startTS common.Ts,
		observer EventObserver,
		notifier WatermarkNotifier,
	) error

	UpdateDispatcherSendTS(dispatcherID common.DispatcherID, gcTS uint64) error

	UnregisterDispatcher(dispatcherID common.DispatcherID) error

	// TODO: ignore large txn now, so we can read all transactions of the same commit ts at one time
	// [startCommitTS, endCommitTS)?
	GetIterator(dataRange *common.DataRange) (EventIterator, error)
}

type EventIterator interface {
	Next() (*common.RowChangedEvent, bool, error)

	// Close closes the iterator.
	Close() error
}

type eventWithTableID struct {
	span heartbeatpb.TableSpan
	raw  *common.RawKVEntry
}

type tableState struct {
	span     heartbeatpb.TableSpan
	observer EventObserver
	notifier WatermarkNotifier

	// data before this watermark won't be needed
	watermark atomic.Uint64

	ch chan eventWithTableID
}
type eventStore struct {
	schemaStore schemastore.SchemaStore
	dbs         []*pebble.DB
	channels    []chan eventWithTableID
	puller      *logpuller.LogPuller
	// puller *puller.MultiplexingPuller

	gcManager *gcManager

	// To manage background goroutines.
	wg sync.WaitGroup

	mu sync.RWMutex
	// TODO: rename the following variables
	tables *common.SpanHashMap[common.DispatcherID]
	spans  map[common.DispatcherID]*tableState
}

const dataDir = "event_store"
const dbCount = 64

func NewEventStore(
	ctx context.Context,
	root string,
	pdCli pd.Client,
	regionCache *tikv.RegionCache,
	pdClock pdutil.Clock,
	kvStorage kv.Storage,
	schemaStore schemastore.SchemaStore,
) EventStore {
	grpcPool := logpuller.NewConnAndClientPool(
		&security.Credential{},
	)
	clientConfig := &logpuller.SharedClientConfig{
		KVClientWorkerConcurrent:     32,
		KVClientGrpcStreamConcurrent: 32,
		KVClientAdvanceIntervalInMs:  300,
	}
	client := logpuller.NewSharedClient(
		clientConfig,
		pdCli,
		grpcPool,
		regionCache,
		pdClock,
	)

	dbPath := fmt.Sprintf("%s/%s", root, dataDir)

	// FIXME: avoid remove
	err := os.RemoveAll(dbPath)
	if err != nil {
		log.Panic("fail to remove path")
	}
	dbs := make([]*pebble.DB, 0, dbCount)
	channels := make([]chan eventWithTableID, 0, dbCount)
	// TODO: update pebble options
	// TODO: close pebble db at exit
	for i := 0; i < dbCount; i++ {
		db, err := pebble.Open(fmt.Sprintf("%s/%d", dbPath, i), &pebble.Options{})
		if err != nil {
			log.Fatal("open db failed", zap.Error(err))
		}
		dbs = append(dbs, db)
		channels = append(channels, make(chan eventWithTableID, 1024))
	}

	store := &eventStore{
		schemaStore: schemaStore,
		dbs:         dbs,
		channels:    channels,

		gcManager: newGCManager(),

		tables: common.NewSpanHashMap[common.DispatcherID](),
		spans:  make(map[common.DispatcherID]*tableState),
	}

	for i := range dbs {
		go func(index int) {
			store.handleEvents(ctx, dbs[index], channels[index])
		}(i)
	}

	consume := func(ctx context.Context, raw *common.RawKVEntry, span heartbeatpb.TableSpan) error {
		if raw != nil {
			store.writeEvent(span, raw)
		}
		return nil
	}
	pullerConfig := &logpuller.LogPullerConfig{
		WorkerCount:  len(dbs),
		HashSpanFunc: common.HashTableSpan,
	}
	puller := logpuller.NewLogPuller(client, pdClock, consume, pullerConfig)
	store.puller = puller

	// conf := cdcConfig.GetGlobalServerConfig()

	// conf.KVClient.WorkerConcurrent = uint(len(dbs))
	// conf.KVClient.GrpcStreamConcurrent = 64
	// grpcPool := sharedconn.NewConnAndClientPool(&security.Credential{}, cdckv.GetGlobalGrpcMetrics())
	// client := cdckv.NewSharedClient(
	// 	model.ChangeFeedID{},
	// 	conf,
	// 	false,
	// 	pdCli,
	// 	grpcPool,
	// 	regionCache,
	// 	pdClock,
	// 	cdcTxnutil.NewLockerResolver(kvStorage.(tikv.Storage), model.ChangeFeedID{}),
	// )

	// consume := func(ctx context.Context, raw *model.RawKVEntry, spans []tablepb.Span, _ model.ShouldSplitKVEntry) error {
	// 	if len(spans) > 1 {
	// 		log.Panic("DML puller subscribes multiple spans")
	// 	}
	// 	span := heartbeatpb.TableSpan{
	// 		TableID:  uint64(spans[0].TableID),
	// 		StartKey: spans[0].StartKey,
	// 		EndKey:   spans[0].EndKey,
	// 	}
	// 	if raw != nil {
	// 		rawKV := &common.RawKVEntry{
	// 			OpType:   common.OpType(raw.OpType),
	// 			Key:      raw.Key,
	// 			Value:    raw.Value,
	// 			OldValue: raw.OldValue,
	// 			StartTs:  raw.StartTs,
	// 			CRTs:     raw.CRTs,
	// 			RegionID: raw.RegionID,
	// 		}
	// 		store.writeEvent(span, rawKV)
	// 	}
	// 	return nil
	// }

	// store.puller = puller.NewMultiplexingPuller(
	// 	model.ChangeFeedID{},
	// 	client,
	// 	pdClock,
	// 	consume,
	// 	len(dbs),
	// 	spanz.HashTableSpan,
	// 	8)

	return store
}

func (e *eventStore) Name() string {
	return appcontext.EventStore
}

func (e *eventStore) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return e.puller.Run(ctx)
	})

	eg.Go(func() error {
		return e.gcManager.run(ctx, e.deleteEvents)
	})

	return eg.Wait()
}

func (e *eventStore) Close(ctx context.Context) error {
	for _, db := range e.dbs {
		if err := db.Close(); err != nil {
			log.Error("failed to close pebble db", zap.Error(err))
		}
	}
	return nil
}

type DBBatchEvent struct {
	batch         *pebble.Batch
	batchResolved *common.SpanHashMap[uint64]
}

func (e *eventStore) getTableStat(span heartbeatpb.TableSpan) *tableState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	dispatcherID, ok := e.tables.Get(span)
	if !ok {
		return nil
	}
	ts, ok := e.spans[dispatcherID]
	if !ok {
		log.Panic("should not happen")
	}
	return ts
}

func (e *eventStore) batchCommitAndUpdateWatermark(ctx context.Context, batchCh chan *DBBatchEvent) {
	e.wg.Add(1)
	defer e.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case batchEvent := <-batchCh:
			// do batch commit
			batch := batchEvent.batch
			if !batch.Empty() {
				if err := batch.Commit(pebble.NoSync); err != nil {
					log.Panic("failed to commit pebble batch", zap.Error(err))
				}
			}

			// update resolved ts after commit successfully
			batchResolved := batchEvent.batchResolved
			batchResolved.Range(func(span heartbeatpb.TableSpan, resolved uint64) bool {
				tableState := e.getTableStat(span)
				if tableState == nil {
					log.Debug("Table is removed, skip updating resolved")
					return true
				}
				tableState.notifier(resolved)
				return true
			})
		}
	}
}

const (
	batchCommitSize     int = 16 * 1024 * 1024
	batchCommitInterval     = 20 * time.Millisecond
)

func (e *eventStore) handleEvents(ctx context.Context, db *pebble.DB, inputCh <-chan eventWithTableID) {
	e.wg.Add(1)
	defer e.wg.Done()
	// 8 is an arbitrary number
	batchCh := make(chan *DBBatchEvent, 8)
	go e.batchCommitAndUpdateWatermark(ctx, batchCh)

	ticker := time.NewTicker(batchCommitInterval / 2)
	defer ticker.Stop()

	encodeItemAndBatch := func(batch *pebble.Batch, newResolved *common.SpanHashMap[uint64], item eventWithTableID) {
		if item.raw.IsResolved() {
			newResolved.ReplaceOrInsert(item.span, item.raw.CRTs)
			return
		}
		key := EncodeKey(uint64(item.span.TableID), item.raw)
		value, err := json.Marshal(item.raw)
		if err != nil {
			log.Panic("failed to marshal event", zap.Error(err))
		}
		if err = batch.Set(key, value, pebble.NoSync); err != nil {
			log.Panic("failed to update pebble batch", zap.Error(err))
		}
	}

	// Batch item and commit until batch size is larger than batchCommitSize,
	// or the time since the last commit is larger than batchCommitInterval.
	// Only return false when the sorter is closed.
	doBatching := func() (*DBBatchEvent, bool) {
		batch := db.NewBatch()
		newResolved := common.NewSpanHashMap[uint64]()
		startToBatch := time.Now()
		for {
			select {
			case item := <-inputCh:
				encodeItemAndBatch(batch, newResolved, item)
				if len(batch.Repr()) >= batchCommitSize {
					return &DBBatchEvent{batch, newResolved}, true
				}
			case <-ctx.Done():
				return nil, false
			case <-ticker.C:
				if time.Since(startToBatch) >= batchCommitInterval {
					return &DBBatchEvent{batch, newResolved}, true
				}
			}
		}
	}

	for {
		batchEvent, ok := doBatching()
		if !ok {
			return
		}
		select {
		case <-ctx.Done():
			return
		case batchCh <- batchEvent:
		}
	}
}

func (e *eventStore) writeEvent(span heartbeatpb.TableSpan, raw *common.RawKVEntry) {
	tableState := e.getTableStat(span)
	if tableState == nil {
		log.Panic("should not happen")
		return
	}
	if !raw.IsResolved() {
		tableState.observer(raw)
	}
	tableState.ch <- eventWithTableID{span: span, raw: raw}
}

func (e *eventStore) deleteEvents(span heartbeatpb.TableSpan, startCommitTS uint64, endCommitTS uint64) error {
	dbIndex := common.HashTableSpan(span, len(e.dbs))
	db := e.dbs[dbIndex]
	start := EncodeTsKey(uint64(span.TableID), startCommitTS)
	end := EncodeTsKey(uint64(span.TableID), endCommitTS)

	return db.DeleteRange(start, end, pebble.NoSync)
}

func (e *eventStore) RegisterDispatcher(dispatcherID common.DispatcherID, tableSpan *common.TableSpan, startTS common.Ts, observer EventObserver, notifier WatermarkNotifier) error {
	span := *tableSpan.TableSpan
	log.Info("register dispatcher",
		zap.Any("dispatcherID", dispatcherID),
		zap.String("span", tableSpan.String()),
		zap.Uint64("startTS", uint64(startTS)))
	e.schemaStore.RegisterDispatcher(dispatcherID, common.TableID(span.TableID), startTS)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tables.ReplaceOrInsert(span, dispatcherID)
	tableState := &tableState{
		span:     span,
		observer: observer,
		notifier: notifier,
	}
	chIndex := common.HashTableSpan(span, len(e.channels))
	tableState.ch = e.channels[chIndex]
	tableState.watermark.Store(uint64(startTS))
	e.spans[dispatcherID] = tableState
	// realSpan := tablepb.Span{
	// 	TableID:  tablepb.TableID(span.TableID),
	// 	StartKey: span.StartKey,
	// 	EndKey:   span.EndKey,
	// }
	// e.puller.Subscribe([]tablepb.Span{realSpan}, model.Ts(startTS), "", func(_ *model.RawKVEntry) bool { return false })
	e.puller.Subscribe(span, startTS)
	return nil
}

func (e *eventStore) UpdateDispatcherSendTS(dispatcherID common.DispatcherID, sendTS uint64) error {
	e.schemaStore.UpdateDispatcherSendTS(dispatcherID, common.Ts(sendTS))
	e.mu.Lock()
	defer e.mu.Unlock()
	if tableStat, ok := e.spans[dispatcherID]; ok {
		for {
			currentWatermark := tableStat.watermark.Load()
			if sendTS <= currentWatermark {
				return nil
			}
			if tableStat.watermark.CompareAndSwap(currentWatermark, sendTS) {
				e.gcManager.addGCItem(tableStat.span, currentWatermark, sendTS)
				return nil
			}
		}
	}
	return nil
}

func (e *eventStore) UnregisterDispatcher(dispatcherID common.DispatcherID) error {
	e.schemaStore.UnregisterDispatcher(dispatcherID)
	e.mu.Lock()
	defer e.mu.Unlock()
	if tableStat, ok := e.spans[dispatcherID]; ok {
		// realSpan := tablepb.Span{
		// 	TableID:  tablepb.TableID(tableStat.span.TableID),
		// 	StartKey: tableStat.span.StartKey,
		// 	EndKey:   tableStat.span.EndKey,
		// }
		// e.puller.Unsubscribe([]tablepb.Span{realSpan})
		e.puller.Unsubscribe(tableStat.span)
		e.tables.Delete(tableStat.span)
		delete(e.spans, dispatcherID)
	}
	return nil
}

func (e *eventStore) GetIterator(dataRange *common.DataRange) (EventIterator, error) {
	// do some check
	span := *dataRange.Span.TableSpan
	tableStat := e.getTableStat(span)
	if tableStat == nil || tableStat.watermark.Load() > dataRange.StartTs {
		log.Panic("should not happen")
	}
	dbIndex := common.HashTableSpan(span, len(e.channels))
	db := e.dbs[dbIndex]
	// TODO: respect key range in span
	start := EncodeTsKey(uint64(span.TableID), dataRange.StartTs, 0)
	end := EncodeTsKey(uint64(span.TableID), dataRange.EndTs, 0)
	// TODO: use TableFilter/UseL6Filters in IterOptions
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return nil, err
	}
	iter.First()

	return &eventStoreIter{
		tableID:      common.TableID(span.TableID),
		schemaStore:  e.schemaStore,
		innerIter:    iter,
		prevStartTS:  0,
		prevCommitTS: 0,
		iterMounter:  mounter.NewMounter(time.Local), // FIXME
		startTs:      dataRange.StartTs,
		endTs:        dataRange.EndTs,
		rowCount:     0,
	}, nil
}

type eventStoreIter struct {
	tableID      common.TableID
	schemaStore  schemastore.SchemaStore
	innerIter    *pebble.Iterator
	prevStartTS  uint64
	prevCommitTS uint64
	iterMounter  mounter.Mounter

	// for debug
	startTs  uint64
	endTs    uint64
	rowCount int64
}

func (iter *eventStoreIter) Next() (*common.RowChangedEvent, bool, error) {
	if iter.innerIter == nil {
		log.Panic("iter is nil")
	}

	if !iter.innerIter.Valid() {
		return nil, false, nil
	}

	key := iter.innerIter.Key()
	value := iter.innerIter.Value()
	_, startTS, commitTS := DecodeKey(key)
	rawKV := &common.RawKVEntry{}
	if err := json.Unmarshal(value, rawKV); err != nil {
		return nil, false, err
	}

	isNewTxn := false
	if iter.prevCommitTS == 0 || (startTS != iter.prevStartTS || commitTS != iter.prevCommitTS) {
		isNewTxn = true
	}
	iter.prevCommitTS = commitTS
	iter.prevStartTS = startTS
	tableInfo, err := iter.schemaStore.GetTableInfo(iter.tableID, common.Ts(rawKV.CRTs-1))
	if err != nil {
		log.Panic("failed to get table info", zap.Error(err))
	}
	row, err := iter.iterMounter.DecodeEvent(rawKV, tableInfo)
	if err != nil {
		log.Panic("failed to decode event", zap.Error(err))
	}
	// log.Info("read event",
	// 	zap.Uint64("tableID", uint64(iter.tableID)),
	// 	zap.Any("value", row.Columns[0].Value))
	iter.rowCount++
	iter.innerIter.Next()
	return row, isNewTxn, nil
}

func (iter *eventStoreIter) Close() error {
	if iter.innerIter == nil {
		log.Info("event store close nil iter",
			zap.Uint64("tableID", uint64(iter.tableID)),
			zap.Uint64("startTs", iter.startTs),
			zap.Uint64("endTs", iter.endTs),
			zap.Int64("rowCount", iter.rowCount))
		return nil
	}

	err := iter.innerIter.Close()
	iter.innerIter = nil
	return err
}
