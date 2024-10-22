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
	"github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/pkg/metrics"
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

type EventObserver func(commitTs uint64)

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

	// TODO: maybe we can remove span
	// Currently not used, when we can get dispatcher just be dispatcherID, we can try use it again.
	// Because find by span is really time consuming, and will make latency more high.
	// GetDispatcherDMLEventState(dispatcherID common.DispatcherID, span *heartbeatpb.TableSpan) DMLEventState

	// TODO: ignore large txn now, so we can read all transactions of the same commit ts at one time
	// (startCommitTS, endCommitTS]?
	GetIterator(dispatcherID common.DispatcherID, dataRange common.DataRange) (EventIterator, error)
}

type DMLEventState struct {
	ResolvedTs       uint64
	MaxEventCommitTs uint64
}

type EventIterator interface {
	Next() (*common.RawKVEntry, bool, error)

	// Close closes the iterator.
	Close() (eventCnt int64, err error)
}

type eventType int

// TODO: better name
const (
	eventTypeBatchSignal eventType = iota
	eventTypeNormal
)

type eventWithState struct {
	eventType
	raw     *common.RawKVEntry
	subID   logpuller.SubscriptionID
	tableID int64
}

type spanState struct {
	span     heartbeatpb.TableSpan
	observer EventObserver
	notifier WatermarkNotifier

	subID logpuller.SubscriptionID

	// data before this watermark won't be needed
	dispatchers map[common.DispatcherID]*dispatcherStat

	// maxEventCommitTs atomic.Uint64

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
	eventChs []chan eventWithState

	puller *logpuller.LogPuller

	gcManager *gcManager

	closed atomic.Bool

	wgBatchSignal sync.WaitGroup

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
		eventChs: make([]chan eventWithState, 0, dbCount),

		gcManager: newGCManager(),
		encoder:   encoder,
		decoder:   decoder,
	}
	// TODO: update pebble options
	for i := 0; i < dbCount; i++ {
		opts := &pebble.Options{
			DisableWAL:   true,
			MemTableSize: 8 << 20,
		}
		// opts.Levels = make([]pebble.LevelOptions, 7)
		// for i := 0; i < len(opts.Levels); i++ {
		// 	l := &opts.Levels[i]
		// 	l.BlockSize = 64 << 10       // 64 KB
		// 	l.IndexBlockSize = 256 << 10 // 256 KB
		// 	l.FilterPolicy = bloom.FilterPolicy(10)
		// 	l.FilterType = pebble.TableFilter
		// 	l.TargetFileSize = 8 << 20 // 8 MB
		// 	// 	l.Compression = pebble.ZstdCompression // TODO: choose the right compression
		// 	l.EnsureDefaults()
		// }
		db, err := pebble.Open(fmt.Sprintf("%s/%d", dbPath, i), opts)
		if err != nil {
			log.Fatal("open db failed", zap.Error(err))
		}
		store.dbs = append(store.dbs, db)
		store.eventChs = append(store.eventChs, make(chan eventWithState, 8192))
	}
	store.spanStates.dispatcherMap = utils.NewBtreeMap[*heartbeatpb.TableSpan, *spanState](heartbeatpb.LessTableSpan)
	store.spanStates.subscriptionMap = make(map[logpuller.SubscriptionID]*spanState)

	// start background goroutines to handle events from puller
	for i := range store.dbs {
		// send batch signal periodically
		eventCh := store.eventChs[i]
		store.wgBatchSignal.Add(1)
		go func() {
			defer store.wgBatchSignal.Done()
			store.sendBatchSignalPeriodically(ctx, eventCh)
		}()

		db := store.dbs[i]
		store.wg.Add(1)
		go func(index int) {
			defer store.wg.Done()
			store.handleEvents(ctx, db, eventCh)
		}(i)
	}

	consume := func(ctx context.Context, raw *common.RawKVEntry, subID logpuller.SubscriptionID, extraData interface{}) error {
		store.writeEvent(subID, raw, extraData)
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

	// TODO: manage gcManager exit
	eg.Go(func() error {
		return e.gcManager.run(ctx, e.deleteEvents)
	})

	eg.Go(func() error {
		return e.updateMetrics(ctx)
	})

	return eg.Wait()
}

func (e *eventStore) Close(ctx context.Context) error {
	// notify and wait the background routine for sending batch signal to exit
	e.closed.Store(true)
	e.wgBatchSignal.Wait()

	if err := e.puller.Close(ctx); err != nil {
		log.Error("failed to close log puller", zap.Error(err))
	}

	// now we can be sure that e.eventChs won't be used

	// notify and wait background goroutines for writing events to exit before cloase pebble db
	for _, ch := range e.eventChs {
		close(ch)
	}
	// TODO: wait gc manager, because it may also write data to pebble db
	e.wg.Wait()

	for _, db := range e.dbs {
		if err := db.Close(); err != nil {
			log.Error("failed to close pebble db", zap.Error(err))
		}
	}
	return nil
}

type subscriptionTag struct {
	chIndex int
	tableID int64
}

func (e *eventStore) RegisterDispatcher(
	dispatcherID common.DispatcherID,
	tableSpan *heartbeatpb.TableSpan,
	startTs uint64,
	observer EventObserver,
	notifier WatermarkNotifier,
) error {
	span := *tableSpan
	log.Info("register dispatcher",
		zap.Any("dispatcherID", dispatcherID),
		zap.String("span", tableSpan.String()),
		zap.Uint64("startTS", startTs))

	e.spanStates.Lock()
	if state, ok := e.spanStates.dispatcherMap.Get(&span); ok {
		state.dispatchers[dispatcherID] = &dispatcherStat{
			dispatcherID: dispatcherID,
			startTS:      startTs,
			watermark:    startTs,
		}
		e.spanStates.Unlock()
		return nil
	}

	e.spanStates.Unlock()
	log.Info("add a span in eventstore", zap.String("span", span.String()))
	chIndex := common.HashTableSpan(span, len(e.eventChs))
	subID := e.puller.Subscribe(span, startTs, subscriptionTag{
		chIndex: chIndex,
		tableID: tableSpan.TableID,
	})
	state := &spanState{
		span:        span,
		observer:    observer,
		dispatchers: make(map[common.DispatcherID]*dispatcherStat),
		notifier:    notifier,
		subID:       subID,
		// TODO: support split table to multiple subSpans.
		// maybe share data for different subSpan is meaningless.
		chIndex: chIndex,
	}
	// TODO: how to support different startTs for different dispatchers?
	state.resolvedTs.Store(uint64(startTs))
	state.dispatchers[dispatcherID] = &dispatcherStat{
		dispatcherID: dispatcherID,
		startTS:      startTs,
		watermark:    startTs,
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

func (e *eventStore) GetDispatcherDMLEventState(dispatcherID common.DispatcherID, span *heartbeatpb.TableSpan) DMLEventState {
	// FIXME
	e.spanStates.Lock()
	defer e.spanStates.Unlock()
	state, ok := e.spanStates.dispatcherMap.Get(span)
	if !ok {
		log.Panic("deregister an unregistered span", zap.String("span", span.String()))
	}
	return DMLEventState{
		ResolvedTs: state.resolvedTs.Load(),
		// MaxEventCommitTs: state.maxEventCommitTs.Load(),
	}
}

func (e *eventStore) GetIterator(dispatcherID common.DispatcherID, dataRange common.DataRange) (EventIterator, error) {
	// do some check
	e.spanStates.RLock()
	state, ok := e.spanStates.dispatcherMap.Get(dataRange.Span)
	if !ok {
		log.Panic("should not happen: cannot find span", zap.String("span", dataRange.Span.String()))
	}
	dispatcher, okw := state.dispatchers[dispatcherID]
	if !okw || dispatcher.watermark > dataRange.StartTs {
		log.Panic("should not happen",
			zap.Any("dispatcherID", dispatcherID),
			zap.Uint64("watermark", dispatcher.watermark),
			zap.Uint64("startTs", dataRange.StartTs))
	}
	span := state.span
	db := e.dbs[state.chIndex]
	e.spanStates.RUnlock()

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
		iterMounter:  event.NewMounter(time.Local), // FIXME
		startTs:      dataRange.StartTs,
		endTs:        dataRange.EndTs,
		rowCount:     0,
		decoder:      e.decoder,
	}, nil
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

	maxEventCommitTsMap map[logpuller.SubscriptionID]uint64

	resolvedTsMap map[logpuller.SubscriptionID]uint64
}

const (
	batchCommitSize     int = 16 * 1024 * 1024
	batchCommitInterval     = 20 * time.Millisecond
)

// handleEvents fetch events from puller and write them to pebble db
func (e *eventStore) handleEvents(ctx context.Context, db *pebble.DB, inputCh <-chan eventWithState) {
	batchCh := make(chan *DBBatchEvent, 10240)

	// consume batch events
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for batchEvent := range batchCh {
			batch := batchEvent.batch
			if batch != nil && !batch.Empty() {
				size := batch.Len()
				if err := batch.Commit(pebble.NoSync); err != nil {
					log.Panic("failed to commit pebble batch", zap.Error(err))
				}
				metrics.EventStoreWriteBytes.Add(float64(size))
			}

			for subID, maxEventCommitTs := range batchEvent.maxEventCommitTsMap {
				e.spanStates.RLock()
				state, ok := e.spanStates.subscriptionMap[subID]
				if !ok {
					// the dispatcher is removed?
					log.Warn("unknown subscriptionID", zap.Uint64("subID", uint64(subID)))
					e.spanStates.RUnlock()
					continue
				}
				e.spanStates.RUnlock()
				state.observer(maxEventCommitTs)
				// state.maxEventCommitTs.Store(maxEventCommitTs)
			}

			// update resolved ts after commit successfully
			for subID, resolvedTs := range batchEvent.resolvedTsMap {
				e.spanStates.RLock()
				state, ok := e.spanStates.subscriptionMap[subID]
				if !ok {
					// the dispatcher is removed?
					log.Warn("unknown subscriptionID", zap.Uint64("subID", uint64(subID)))
					e.spanStates.RUnlock()
					continue
				}
				e.spanStates.RUnlock()
				state.resolvedTs.Store(resolvedTs)
				state.notifier(resolvedTs)
			}
		}
	}()

	addEvent2Batch := func(batch *pebble.Batch, item eventWithState) {
		key := EncodeKey(uint64(item.tableID), item.raw)
		value := item.raw.Encode()
		compressedValue := e.encoder.EncodeAll(value, nil)
		ratio := float64(len(value)) / float64(len(compressedValue))
		metrics.EventStoreCompressRatio.Set(ratio)
		if err := batch.Set(key, compressedValue, pebble.NoSync); err != nil {
			log.Panic("failed to update pebble batch", zap.Error(err))
		}
	}

	// fetch events from channel and return a batch when the batch is full or timeout
	doBatching := func() *DBBatchEvent {
		var batch *pebble.Batch
		resolvedTsMap := make(map[logpuller.SubscriptionID]uint64)
		maxEventCommitTsMap := make(map[logpuller.SubscriptionID]uint64)
		startToBatch := time.Now()
		// Note: don't use select here for performance
		for item := range inputCh {
			if item.eventType == eventTypeBatchSignal {
				if time.Since(startToBatch) >= batchCommitInterval {
					if batch != nil || len(resolvedTsMap) > 0 {
						return &DBBatchEvent{batch, maxEventCommitTsMap, resolvedTsMap}
					}
				}
				continue
			}

			if item.raw.IsResolved() {
				resolvedTsMap[item.subID] = item.raw.CRTs
				continue
			} else {
				if batch == nil {
					batch = db.NewBatch()
				}
				if item.raw.CRTs > maxEventCommitTsMap[item.subID] {
					maxEventCommitTsMap[item.subID] = item.raw.CRTs
				}
				addEvent2Batch(batch, item)
				if batch.Len() >= batchCommitSize {
					return &DBBatchEvent{batch, maxEventCommitTsMap, resolvedTsMap}
				}
			}
		}
		return nil
	}

	for {
		batchEvent := doBatching()
		if batchEvent == nil {
			// notify batch goroutine to exit
			close(batchCh)
			return
		}
		select {
		case <-ctx.Done():
			// notify batch goroutine to exit
			close(batchCh)
			return
		case batchCh <- batchEvent:
		}
	}
}

// TODO: maybe we can remove it and just rely on resolved ts? Do it after we know how to share
func (e *eventStore) sendBatchSignalPeriodically(ctx context.Context, inputCh chan eventWithState) {
	ticker := time.NewTicker(batchCommitInterval / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if e.closed.Load() {
				return
			}
			inputCh <- eventWithState{eventType: eventTypeBatchSignal}
		}
	}
}

func (e *eventStore) writeEvent(subID logpuller.SubscriptionID, raw *common.RawKVEntry, tag interface{}) {
	subTag := tag.(subscriptionTag)
	e.eventChs[subTag.chIndex] <- eventWithState{
		eventType: eventTypeNormal,
		raw:       raw,
		subID:     subID,
		tableID:   subTag.tableID,
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
	iterMounter  event.Mounter

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
