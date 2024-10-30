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

type ResolvedTsNotifier func(watermark uint64)

type EventStore interface {
	Name() string

	Run(ctx context.Context) error

	Close(ctx context.Context) error

	RegisterDispatcher(
		dispatcherID common.DispatcherID,
		span *heartbeatpb.TableSpan,
		startTS uint64,
		notifier ResolvedTsNotifier,
	) error

	UnregisterDispatcher(dispatcherID common.DispatcherID) error

	UpdateDispatcherSendTs(dispatcherID common.DispatcherID, gcTS uint64) error

	GetDispatcherDMLEventState(dispatcherID common.DispatcherID) DMLEventState

	// return an iterator which scan the data in ts range (dataRange.StartTs, dataRange.EndTs]
	GetIterator(dispatcherID common.DispatcherID, dataRange common.DataRange) (EventIterator, error)
}

type DMLEventState struct {
	// ResolvedTs       uint64
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
	raw      *common.RawKVEntry
	subID    logpuller.SubscriptionID
	tableID  int64
	uniqueID uint64
}

var uniqueIDGen uint64 = 0

func genUniqueID() uint64 {
	return atomic.AddUint64(&uniqueIDGen, 1)
}

type dispatcherStat struct {
	dispatcherID common.DispatcherID
	// called when new resolved ts event come
	notifier ResolvedTsNotifier

	subID logpuller.SubscriptionID

	tableID int64
	// an id encode in the event key of this dispatcher
	// used to seperate data between dispatchers with overlapping spans
	uniqueKeyID uint64
	// the max ts of events which is not needed by this dispatcher
	checkpointTs atomic.Uint64
	// the max commit ts of dml event in the store
	maxEventCommitTs atomic.Uint64
	// the resolveTs persisted in the store
	// just used in metrics now. maybe can be removed in the future.
	resolvedTs atomic.Uint64
	chIndex    int
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

	dispatcherStates struct {
		sync.RWMutex
		m map[common.DispatcherID]*dispatcherStat
		n map[logpuller.SubscriptionID]common.DispatcherID
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
	store.dispatcherStates.m = make(map[common.DispatcherID]*dispatcherStat)
	store.dispatcherStates.n = make(map[logpuller.SubscriptionID]common.DispatcherID)

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
			store.batchAndWriteEvents(ctx, db, eventCh)
		}(i)
	}

	consume := func(ctx context.Context, raw *common.RawKVEntry, subID logpuller.SubscriptionID, extraData interface{}) error {
		store.consumeEvent(subID, raw, extraData)
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
	for i := range e.eventChs {
		close(e.eventChs[i])
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
	chIndex     int
	tableID     int64
	uniqueKeyID uint64
}

func (e *eventStore) RegisterDispatcher(
	dispatcherID common.DispatcherID,
	tableSpan *heartbeatpb.TableSpan,
	startTs uint64,
	notifier ResolvedTsNotifier,
) error {
	log.Info("register dispatcher",
		zap.Any("dispatcherID", dispatcherID),
		zap.String("span", tableSpan.String()),
		zap.Uint64("startTs", startTs))

	chIndex := common.HashTableSpan(tableSpan, len(e.eventChs))
	uniqueKeyID := genUniqueID()
	// Note: don'w hold any lock when call Subscribe
	subID := e.puller.Subscribe(*tableSpan, startTs, subscriptionTag{
		chIndex:     chIndex,
		tableID:     tableSpan.TableID,
		uniqueKeyID: uniqueKeyID,
	})

	// initialize dispatcherStat
	// TODO: if puller event come before we initialize dispatcherStat,
	// maxEventCommitTs may not be updated correctly and cause data loss.(lost resolved ts is harmless)
	// To fix it, we need to alloc subID and initialize dispatcherStat before puller may send events.
	// That is allocate subID in a separate method.
	e.dispatcherStates.Lock()
	defer e.dispatcherStates.Unlock()
	stat := &dispatcherStat{
		dispatcherID: dispatcherID,
		notifier:     notifier,
		tableID:      tableSpan.TableID,
		uniqueKeyID:  uniqueKeyID,
		chIndex:      chIndex,
	}
	stat.checkpointTs.Store(startTs)
	stat.maxEventCommitTs.Store(startTs)
	e.dispatcherStates.m[dispatcherID] = stat
	e.dispatcherStates.n[subID] = dispatcherID
	return nil
}

func (e *eventStore) UnregisterDispatcher(dispatcherID common.DispatcherID) error {
	log.Info("unregister dispatcher", zap.Stringer("dispatcherID", dispatcherID))
	e.dispatcherStates.Lock()
	defer e.dispatcherStates.Unlock()
	stat, ok := e.dispatcherStates.m[dispatcherID]
	if !ok {
		return nil
	}
	subID := stat.subID
	delete(e.dispatcherStates.m, dispatcherID)
	delete(e.dispatcherStates.n, subID)

	// TODO: do we need unlock before puller.Unsubscribe?
	e.puller.Unsubscribe(subID)
	return nil
}

func (e *eventStore) UpdateDispatcherSendTs(
	dispatcherID common.DispatcherID,
	sendTs uint64,
) error {
	e.dispatcherStates.RLock()
	defer e.dispatcherStates.RUnlock()
	if stat, ok := e.dispatcherStates.m[dispatcherID]; ok {
		oldSendTs := stat.checkpointTs.Load()
		if sendTs > oldSendTs {
			stat.checkpointTs.Store(sendTs)
			e.gcManager.addGCItem(stat.chIndex, stat.uniqueKeyID, stat.tableID, oldSendTs, sendTs)
		}
	}
	return nil
}

func (e *eventStore) GetDispatcherDMLEventState(dispatcherID common.DispatcherID) DMLEventState {
	e.dispatcherStates.RLock()
	defer e.dispatcherStates.RUnlock()
	stat, ok := e.dispatcherStates.m[dispatcherID]
	if !ok {
		log.Panic("fail to find dispatcher", zap.Any("dispatcherID", dispatcherID))
	}

	return DMLEventState{
		// ResolvedTs:       stat.resolvedTs.Load(),
		MaxEventCommitTs: stat.maxEventCommitTs.Load(),
	}
}

func (e *eventStore) GetIterator(dispatcherID common.DispatcherID, dataRange common.DataRange) (EventIterator, error) {
	e.dispatcherStates.RLock()
	stat, ok := e.dispatcherStates.m[dispatcherID]
	if !ok {
		log.Panic("fail to find dispatcher", zap.Any("dispatcherID", dispatcherID))
	}
	if dataRange.StartTs < stat.checkpointTs.Load() {
		log.Panic("should not happen",
			zap.Any("dispatcherID", dispatcherID),
			zap.Uint64("checkpointTs", stat.checkpointTs.Load()),
			zap.Uint64("startTs", dataRange.StartTs))
	}
	db := e.dbs[stat.chIndex]
	e.dispatcherStates.RUnlock()

	// convert range before pass it to pebble: (startTs, endTs] is equal to [startTs + 1, endTs + 1)
	start := EncodeKeyPrefix(stat.uniqueKeyID, stat.tableID, dataRange.StartTs+1)
	end := EncodeKeyPrefix(stat.uniqueKeyID, stat.tableID, dataRange.EndTs+1)
	// TODO: optimize read performance
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
		tableID:      stat.tableID,
		innerIter:    iter,
		prevStartTs:  0,
		prevCommitTs: 0,
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
			e.dispatcherStates.RLock()
			for _, stat := range e.dispatcherStates.m {
				// resolved ts lag
				resolvedTs := stat.resolvedTs.Load()
				resolvedPhyTs := oracle.ExtractPhysical(resolvedTs)
				resolvedLag := float64(currentPhyTs-resolvedPhyTs) / 1e3
				metrics.EventStoreDispatcherResolvedTsLagHist.Observe(float64(resolvedLag))
				if minResolvedTs == 0 || resolvedTs < minResolvedTs {
					minResolvedTs = resolvedTs
				}
				// checkpoint ts lag
				checkpointTs := stat.checkpointTs.Load()
				watermarkPhyTs := oracle.ExtractPhysical(checkpointTs)
				watermarkLag := float64(currentPhyTs-watermarkPhyTs) / 1e3
				metrics.EventStoreDispatcherWatermarkLagHist.Observe(float64(watermarkLag))
			}
			e.dispatcherStates.RUnlock()

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
func (e *eventStore) batchAndWriteEvents(ctx context.Context, db *pebble.DB, inputCh <-chan eventWithState) {
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
				e.dispatcherStates.RLock()
				dispatcherID, ok := e.dispatcherStates.n[subID]
				if !ok {
					// the dispatcher is removed?
					log.Warn("unknown subscriptionID", zap.Uint64("subID", uint64(subID)))
					e.dispatcherStates.RUnlock()
					continue
				}
				stat := e.dispatcherStates.m[dispatcherID]
				e.dispatcherStates.RUnlock()
				stat.maxEventCommitTs.Store(maxEventCommitTs)
			}

			// update resolved ts after commit successfully
			for subID, resolvedTs := range batchEvent.resolvedTsMap {
				e.dispatcherStates.RLock()
				dispatcherID, ok := e.dispatcherStates.n[subID]
				if !ok {
					// the dispatcher is removed?
					log.Warn("unknown subscriptionID", zap.Uint64("subID", uint64(subID)))
					e.dispatcherStates.RUnlock()
					continue
				}
				stat := e.dispatcherStates.m[dispatcherID]
				e.dispatcherStates.RUnlock()
				stat.resolvedTs.Store(resolvedTs)
				stat.notifier(resolvedTs)
			}
		}
	}()

	addEvent2Batch := func(batch *pebble.Batch, item eventWithState) {
		key := EncodeKey(item.uniqueID, item.tableID, item.raw)
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
func (e *eventStore) sendBatchSignalPeriodically(ctx context.Context, inputCh chan<- eventWithState) {
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

func (e *eventStore) consumeEvent(subID logpuller.SubscriptionID, raw *common.RawKVEntry, tag interface{}) {
	subTag := tag.(subscriptionTag)
	e.eventChs[subTag.chIndex] <- eventWithState{
		eventType: eventTypeNormal,
		raw:       raw,
		subID:     subID,
		tableID:   subTag.tableID,
		uniqueID:  subTag.uniqueKeyID,
	}
}

func (e *eventStore) deleteEvents(dbIndex int, uniqueKeyID uint64, tableID int64, startTs uint64, endTs uint64) error {
	db := e.dbs[dbIndex]
	start := EncodeKeyPrefix(uniqueKeyID, tableID, startTs)
	end := EncodeKeyPrefix(uniqueKeyID, tableID, endTs)

	return db.DeleteRange(start, end, pebble.NoSync)
}

type eventStoreIter struct {
	tableID      common.TableID
	innerIter    *pebble.Iterator
	prevStartTs  uint64
	prevCommitTs uint64
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

	value := iter.innerIter.Value()
	decompressedValue, err := iter.decoder.DecodeAll(value, nil)
	if err != nil {
		log.Panic("failed to decompress value", zap.Error(err))
	}
	metrics.EventStoreScanBytes.Add(float64(len(decompressedValue)))
	rawKV := &common.RawKVEntry{}
	rawKV.Decode(decompressedValue)
	isNewTxn := false
	if iter.prevCommitTs == 0 || (rawKV.StartTs != iter.prevStartTs || rawKV.CRTs != iter.prevCommitTs) {
		isNewTxn = true
	}
	iter.prevCommitTs = rawKV.CRTs
	iter.prevStartTs = rawKV.StartTs
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
