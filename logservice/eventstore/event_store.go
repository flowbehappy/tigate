package eventstore

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/logservice/logpuller"
	"github.com/flowbehappy/tigate/logservice/txnutil"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/flowbehappy/tigate/pkg/mounter"
	"github.com/flowbehappy/tigate/utils"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var metricEventCompressRatio = metrics.EventStoreCompressRatio

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
		span *heartbeatpb.TableSpan,
		startTS uint64,
		observer EventObserver,
		notifier WatermarkNotifier,
	) error

	UpdateDispatcherSendTs(dispatcherID common.DispatcherID, span *heartbeatpb.TableSpan, gcTS uint64) error

	UnregisterDispatcher(dispatcherID common.DispatcherID, span *heartbeatpb.TableSpan) error

	// TODO: ignore large txn now, so we can read all transactions of the same commit ts at one time
	// (startCommitTS, endCommitTS]?
	GetIterator(dispatcherID common.DispatcherID, dataRange common.DataRange) (EventIterator, error)
}

type EventIterator interface {
	Next() (*common.RawKVEntry, bool, error)

	// Close closes the iterator.
	Close() (eventCnt int64, err error)
}

type eventWithSpanState struct {
	raw   *common.RawKVEntry
	state *spanState
}

type spanState struct {
	span     heartbeatpb.TableSpan
	observer EventObserver
	notifier WatermarkNotifier

	subID logpuller.SubscriptionID

	// data before this watermark won't be needed
	dispatchers map[common.DispatcherID]*dispatcherStat

	// the resolveTs persisted in the store
	resolvedTs atomic.Uint64
	chIndex    int
}

// maybe it can be removed
type dispatcherStat struct {
	dispatcherID common.DispatcherID
	watermark    uint64
	startTS      uint64
}

func (s *spanState) minWatermark() uint64 {
	minWatermark := uint64(0)
	for _, d := range s.dispatchers {
		wm := d.watermark
		if minWatermark == 0 || wm < minWatermark {
			minWatermark = wm
		}
	}
	return minWatermark
}

type eventStore struct {
	pdClock pdutil.Clock

	dbs      []*pebble.DB
	eventChs []chan eventWithSpanState

	puller *logpuller.LogPuller

	gcManager *gcManager

	// To manage background goroutines.
	wg sync.WaitGroup

	spanStates struct {
		sync.RWMutex
		dispatcherMap   *utils.BtreeMap[*heartbeatpb.TableSpan, *spanState]
		subscriptionMap map[logpuller.SubscriptionID]*spanState
	}

	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

const dataDir = "event_store"
const dbCount = 32

func New(
	ctx context.Context,
	root string,
	pdCli pd.Client,
	regionCache *tikv.RegionCache,
	pdClock pdutil.Clock,
	kvStorage kv.Storage,
) EventStore {
	clientConfig := &logpuller.SubscriptionClientConfig{
		RegionRequestWorkerPerStore:   16,
		ChangeEventProcessorNum:       64,
		AdvanceResolvedTsIntervalInMs: 600,
	}
	client := logpuller.NewSubscriptionClient(
		logpuller.ClientIDEventStore,
		clientConfig,
		pdCli,
		regionCache,
		pdClock,
		txnutil.NewLockerResolver(kvStorage.(tikv.Storage)),
		&security.Credential{},
	)

	dbPath := fmt.Sprintf("%s/%s", root, dataDir)

	// FIXME: avoid remove
	err := os.RemoveAll(dbPath)
	if err != nil {
		log.Panic("fail to remove path")
	}
	// Create the zstd encoder
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		log.Panic("Failed to create zstd encoder", zap.Error(err))
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		log.Panic("Failed to create zstd decoder", zap.Error(err))
	}

	store := &eventStore{
		pdClock:  pdClock,
		dbs:      make([]*pebble.DB, 0, dbCount),
		eventChs: make([]chan eventWithSpanState, 0, dbCount),

		gcManager: newGCManager(),
		encoder:   encoder,
		decoder:   decoder,
	}
	// TODO: update pebble options
	// TODO: close pebble db at exit
	for i := 0; i < dbCount; i++ {
		db, err := pebble.Open(fmt.Sprintf("%s/%d", dbPath, i), &pebble.Options{
			DisableWAL:   true,
			MemTableSize: 8 << 20,
		})
		if err != nil {
			log.Fatal("open db failed", zap.Error(err))
		}
		store.dbs = append(store.dbs, db)
		store.eventChs = append(store.eventChs, make(chan eventWithSpanState, 1024))
	}
	store.spanStates.dispatcherMap = utils.NewBtreeMap[*heartbeatpb.TableSpan, *spanState](heartbeatpb.LessTableSpan)
	store.spanStates.subscriptionMap = make(map[logpuller.SubscriptionID]*spanState)

	for i := range store.dbs {
		go func(index int) {
			store.handleEvents(ctx, store.dbs[index], store.eventChs[index])
		}(i)
	}

	consume := func(ctx context.Context, raw *common.RawKVEntry, subID logpuller.SubscriptionID) error {
		if raw != nil {
			store.writeEvent(subID, raw)
		}
		return nil
	}
	puller := logpuller.NewLogPuller(client, pdClock, consume)
	store.puller = puller

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

	eg.Go(func() error {
		return e.updateMetrics(ctx)
	})

	return eg.Wait()
}

func (e *eventStore) RegisterDispatcher(
	dispatcherID common.DispatcherID, tableSpan *heartbeatpb.TableSpan, startTS uint64,
	observer EventObserver, notifier WatermarkNotifier,
) error {
	span := *tableSpan
	log.Info("register dispatcher",
		zap.Any("dispatcherID", dispatcherID),
		zap.String("span", tableSpan.String()),
		zap.Uint64("startTS", startTS))

	e.spanStates.Lock()
	if state, ok := e.spanStates.dispatcherMap.Get(&span); ok {
		state.dispatchers[dispatcherID] = &dispatcherStat{
			dispatcherID: dispatcherID,
			startTS:      startTS,
			watermark:    startTS,
		}
		e.spanStates.Unlock()
		return nil
	}

	e.spanStates.Unlock()
	log.Info("add a span in eventstore", zap.String("span", span.String()))
	subID := e.puller.Subscribe(span, startTS)
	state := &spanState{
		span:        span,
		observer:    observer,
		dispatchers: make(map[common.DispatcherID]*dispatcherStat),
		notifier:    notifier,
		subID:       subID,
		// TODO: support split table to multiple subSpans.
		// maybe share data for different subSpan is meaningless.
		chIndex: common.HashTableSpan(span, len(e.eventChs)),
	}
	// TODO: how to support different startTs for different dispatchers?
	state.resolvedTs.Store(uint64(startTS))
	state.dispatchers[dispatcherID] = &dispatcherStat{
		dispatcherID: dispatcherID,
		startTS:      startTS,
		watermark:    startTS,
	}

	e.spanStates.Lock()
	defer e.spanStates.Unlock()
	e.spanStates.dispatcherMap.ReplaceOrInsert(&span, state)
	e.spanStates.subscriptionMap[subID] = state
	return nil
}

func (e *eventStore) UpdateDispatcherSendTs(
	dispatcherID common.DispatcherID, span *heartbeatpb.TableSpan, sendTs uint64,
) error {
	e.spanStates.Lock()
	defer e.spanStates.Unlock()
	if state, ok := e.spanStates.dispatcherMap.Get(span); ok {
		if !ok {
			log.Panic("should not happen", zap.Any("dispatcherID", dispatcherID))
		}
		oldWatermark := state.dispatchers[dispatcherID].watermark
		if sendTs > oldWatermark {
			state.dispatchers[dispatcherID].watermark = sendTs
			e.gcManager.addGCItem(state.chIndex, span.TableID, oldWatermark, sendTs)
		}
	}
	return nil
}

func (e *eventStore) UnregisterDispatcher(
	dispatcherID common.DispatcherID, span *heartbeatpb.TableSpan,
) error {
	log.Info("unregister dispatcher", zap.Stringer("dispatcherID", dispatcherID))
	e.spanStates.Lock()
	defer e.spanStates.Unlock()
	state, ok := e.spanStates.dispatcherMap.Get(span)
	if !ok {
		log.Panic("deregister an unregistered span", zap.String("span", span.String()))
	}
	if _, ok := state.dispatchers[dispatcherID]; ok {
		delete(state.dispatchers, dispatcherID)
	} else {
		log.Panic("deregister an unregistered dispatcher", zap.Stringer("dispatcherID", dispatcherID))
	}

	if len(state.dispatchers) == 0 {
		// TODO: do we need unlock before puller.Unsubscribe?
		e.puller.Unsubscribe(state.subID)
		e.spanStates.dispatcherMap.Delete(span)
		delete(e.spanStates.subscriptionMap, state.subID)
		log.Info("remove a span in eventstore", zap.String("span", span.String()))
	}
	return nil
}

func (e *eventStore) GetIterator(dispatcherID common.DispatcherID, dataRange common.DataRange) (EventIterator, error) {
	// do some check
	state, ok := e.spanStates.dispatcherMap.Get(dataRange.Span)
	dispatcher, okw := state.dispatchers[dispatcherID]
	if !ok || !okw || dispatcher.watermark > dataRange.StartTs {
		log.Panic("should not happen",
			zap.Uint64("watermark", dispatcher.watermark),
			zap.Uint64("startTs", dataRange.StartTs))
	}
	span := state.span
	db := e.dbs[state.chIndex]
	// TODO: respect key range in span
	// convert endTs to inclusive: [startTs, endTs) -> (startTs, endTs]
	start := EncodeTsKey(uint64(span.TableID), dataRange.StartTs+1, 0)
	end := EncodeTsKey(uint64(span.TableID), dataRange.EndTs+1, 0)
	// TODO: use TableFilter/UseL6Filters in IterOptions
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return nil, err
	}
	iter.First()

	metrics.EventStoreScanRequestsCount.Inc()

	return &eventStoreIter{
		tableID:      span.TableID,
		innerIter:    iter,
		prevStartTS:  0,
		prevCommitTS: 0,
		iterMounter:  mounter.NewMounter(time.Local), // FIXME
		startTs:      dataRange.StartTs,
		endTs:        dataRange.EndTs,
		rowCount:     0,
		decoder:      e.decoder,
	}, nil
}

func (e *eventStore) Close(ctx context.Context) error {
	if err := e.puller.Close(ctx); err != nil {
		log.Error("failed to close log puller", zap.Error(err))
	}
	for _, db := range e.dbs {
		if err := db.Close(); err != nil {
			log.Error("failed to close pebble db", zap.Error(err))
		}
	}
	return nil
}

func (e *eventStore) updateMetrics(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			currentTime := e.pdClock.CurrentTime()
			currentPhyTs := oracle.GetPhysical(currentTime)
			minResolvedTs := uint64(0)
			e.spanStates.RLock()
			for _, tableState := range e.spanStates.subscriptionMap {
				resolvedTs := tableState.resolvedTs.Load()
				if minResolvedTs == 0 || resolvedTs < minResolvedTs {
					minResolvedTs = resolvedTs
				}
				resolvedPhyTs := oracle.ExtractPhysical(resolvedTs)
				resolvedLag := float64(currentPhyTs-resolvedPhyTs) / 1e3
				// TODO: avoid the name `watermark`
				watermark := tableState.minWatermark()
				watermarkPhyTs := oracle.ExtractPhysical(watermark)
				watermarkLag := float64(currentPhyTs-watermarkPhyTs) / 1e3
				metrics.EventStoreDispatcherResolvedTsLagHist.Observe(float64(resolvedLag))
				metrics.EventStoreDispatcherWatermarkLagHist.Observe(float64(watermarkLag))
			}
			e.spanStates.RUnlock()

			if minResolvedTs == 0 {
				continue
			}
			minResolvedPhyTs := oracle.ExtractPhysical(minResolvedTs)
			maxResolvedLag := float64(currentPhyTs-minResolvedPhyTs) / 1e3
			metrics.EventStoreMaxResolvedTsLagGauge.Set(maxResolvedLag)
		}
	}
}

type DBBatchEvent struct {
	batch *pebble.Batch

	resolvedTsBatch map[*spanState]uint64
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
			if batch != nil && !batch.Empty() {
				size := batch.Len()
				if err := batch.Commit(pebble.NoSync); err != nil {
					log.Panic("failed to commit pebble batch", zap.Error(err))
				}
				metrics.EventStoreWriteBytes.Add(float64(size))
			}

			// update resolved ts after commit successfully
			for state, resolvedTs := range batchEvent.resolvedTsBatch {
				state.resolvedTs.Store(resolvedTs)
				state.notifier(resolvedTs)
			}
		}
	}
}

const (
	batchCommitSize     int = 16 * 1024 * 1024
	batchCommitInterval     = 20 * time.Millisecond
)

func (e *eventStore) handleEvents(ctx context.Context, db *pebble.DB, inputCh <-chan eventWithSpanState) {
	e.wg.Add(1)
	defer e.wg.Done()
	// 8 is an arbitrary number
	batchCh := make(chan *DBBatchEvent, 8)
	go e.batchCommitAndUpdateWatermark(ctx, batchCh)

	ticker := time.NewTicker(batchCommitInterval / 2)
	defer ticker.Stop()

	addEvent2Batch := func(batch *pebble.Batch, item eventWithSpanState) {
		key := EncodeKey(uint64(item.state.span.TableID), item.raw)
		value := item.raw.Encode()
		compressedValue := e.encoder.EncodeAll(value, nil)
		ratio := float64(len(value)) / float64(len(compressedValue))
		metrics.EventStoreCompressRatio.Set(ratio)
		if err := batch.Set(key, compressedValue, pebble.NoSync); err != nil {
			log.Panic("failed to update pebble batch", zap.Error(err))
		}
	}

	// Batch item and commit until batch size is larger than batchCommitSize,
	// or the time since the last commit is larger than batchCommitInterval.
	// Only return false when the sorter is closed.
	doBatching := func() (*DBBatchEvent, bool) {
		var batch *pebble.Batch
		resolvedTsBatch := make(map[*spanState]uint64)
		startToBatch := time.Now()
		for {
			select {
			case item := <-inputCh:
				if item.raw.IsResolved() {
					resolvedTsBatch[item.state] = item.raw.CRTs
					continue
				}
				if batch == nil {
					batch = db.NewBatch()
				}
				addEvent2Batch(batch, item)
				if batch.Len() >= batchCommitSize {
					return &DBBatchEvent{batch, resolvedTsBatch}, true
				}
			case <-ctx.Done():
				return nil, false
			case <-ticker.C:
				if time.Since(startToBatch) >= batchCommitInterval {
					return &DBBatchEvent{batch, resolvedTsBatch}, true
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

func (e *eventStore) writeEvent(subID logpuller.SubscriptionID, raw *common.RawKVEntry) {
	e.spanStates.RLock()
	state, ok := e.spanStates.subscriptionMap[subID]
	if !ok {
		log.Panic("should not happen")
		return
	}
	e.spanStates.RUnlock()
	if !raw.IsResolved() {
		// TODO: make sure this won't block
		state.observer(raw)
	}
	e.eventChs[state.chIndex] <- eventWithSpanState{
		raw:   raw,
		state: state,
	}
}

func (e *eventStore) deleteEvents(dbIndex int, tableID int64, startCommitTS uint64, endCommitTS uint64) error {
	db := e.dbs[dbIndex]
	start := EncodeTsKey(uint64(tableID), startCommitTS)
	end := EncodeTsKey(uint64(tableID), endCommitTS)

	return db.DeleteRange(start, end, pebble.NoSync)
}

type eventStoreIter struct {
	tableID      common.TableID
	innerIter    *pebble.Iterator
	prevStartTS  uint64
	prevCommitTS uint64
	iterMounter  mounter.Mounter

	// for debug
	startTs  uint64
	endTs    uint64
	rowCount int64
	decoder  *zstd.Decoder
}

func (iter *eventStoreIter) Next() (*common.RawKVEntry, bool, error) {
	if iter.innerIter == nil {
		log.Panic("iter is nil")
	}

	if !iter.innerIter.Valid() {
		return nil, false, nil
	}

	key := iter.innerIter.Key()
	value := iter.innerIter.Value()
	decompressedValue, err := iter.decoder.DecodeAll(value, nil)
	if err != nil {
		log.Panic("failed to decompress value", zap.Error(err))
	}
	_, startTS, commitTS := DecodeKey(key)
	metrics.EventStoreScanBytes.Add(float64(len(decompressedValue)))
	rawKV := &common.RawKVEntry{}
	rawKV.Decode(decompressedValue)
	isNewTxn := false
	if iter.prevCommitTS == 0 || (startTS != iter.prevStartTS || commitTS != iter.prevCommitTS) {
		isNewTxn = true
	}
	iter.prevCommitTS = commitTS
	iter.prevStartTS = startTS
	iter.rowCount++
	iter.innerIter.Next()
	return rawKV, isNewTxn, nil
}

func (iter *eventStoreIter) Close() (int64, error) {
	if iter.innerIter == nil {
		log.Info("event store close nil iter",
			zap.Uint64("tableID", uint64(iter.tableID)),
			zap.Uint64("startTs", iter.startTs),
			zap.Uint64("endTs", iter.endTs),
			zap.Int64("rowCount", iter.rowCount))
		return 0, nil
	}

	err := iter.innerIter.Close()
	iter.innerIter = nil
	return iter.rowCount, err
}
