// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package eventstore

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/tikv/client-go/v2/oracle"

	"github.com/pingcap/ticdc/utils/chann"

	"github.com/cockroachdb/pebble"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	CounterKv       = metrics.EventStoreReceivedEventCount.WithLabelValues("kv")
	CounterResolved = metrics.EventStoreReceivedEventCount.WithLabelValues("resolved")
)

type ResolvedTsNotifier func(watermark uint64, latestCommitTs uint64)

type EventStore interface {
	Name() string

	Run(ctx context.Context) error

	Close(ctx context.Context) error

	RegisterDispatcher(
		dispatcherID common.DispatcherID,
		span *heartbeatpb.TableSpan,
		startTS uint64,
		notifier ResolvedTsNotifier,
		onlyReuse bool,
	) (bool, error)

	UnregisterDispatcher(dispatcherID common.DispatcherID) error

	UpdateDispatcherCheckpointTs(dispatcherID common.DispatcherID, checkpointTs uint64) error

	GetDispatcherDMLEventState(dispatcherID common.DispatcherID) (bool, DMLEventState)

	// return an iterator which scan the data in ts range (dataRange.StartTs, dataRange.EndTs]
	GetIterator(dispatcherID common.DispatcherID, dataRange common.DataRange) (EventIterator, error)
}

type DMLEventState struct {
	// ResolvedTs       uint64
	// The max commit ts of dml event in the store.
	MaxEventCommitTs uint64
}

type EventIterator interface {
	Next() (*common.RawKVEntry, bool, error)

	// Close closes the iterator.
	Close() (eventCnt int64, err error)
}

type dispatcherStat struct {
	dispatcherID common.DispatcherID

	tableSpan *heartbeatpb.TableSpan
	// the max ts of events which is not needed by this dispatcher
	checkpointTs uint64

	subID logpuller.SubscriptionID
}

type subscriptionStat struct {
	subID logpuller.SubscriptionID

	tableID int64

	// dispatchers depend on this subscription
	dispatchers struct {
		sync.RWMutex
		notifiers map[common.DispatcherID]ResolvedTsNotifier
	}

	dbIndex int

	eventCh *chann.UnlimitedChannel[kvEventsAndCallback, uint64]
	// data <= checkpointTs can be deleted
	checkpointTs atomic.Uint64
	// the resolveTs persisted in the store
	resolvedTs atomic.Uint64
	// the max commit ts of dml event in the store
	maxEventCommitTs atomic.Uint64
}

type kvEventsAndCallback struct {
	subID    logpuller.SubscriptionID
	tableID  int64
	kvs      []common.RawKVEntry
	callback func()
}

func kvEventsAndCallbackSizer(_ kvEventsAndCallback) int { return 0 }

type eventStore struct {
	pdClock   pdutil.Clock
	subClient *logpuller.SubscriptionClient

	dbs            []*pebble.DB
	chs            []*chann.UnlimitedChannel[kvEventsAndCallback, uint64]
	writeTaskPools []*writeTaskPool

	gcManager *gcManager

	messageCenter messaging.MessageCenter

	coordinatorInfo struct {
		sync.RWMutex
		id node.ID
	}

	// To manage background goroutines.
	wg sync.WaitGroup

	dispatcherMeta struct {
		sync.RWMutex
		dispatcherStats   map[common.DispatcherID]*dispatcherStat
		subscriptionStats map[logpuller.SubscriptionID]*subscriptionStat
		// table id -> dispatcher ids
		// use table id as the key is to share data between spans not completely the same in the future.
		tableToDispatchers map[int64]map[common.DispatcherID]bool
	}

	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

const (
	dataDir             = "event_store"
	dbCount             = 32
	writeWorkerNumPerDB = 2
	streamCount         = 8
)

type pathHasher struct {
}

func (h pathHasher) HashPath(subID logpuller.SubscriptionID) uint64 {
	return uint64(subID)
}

func New(
	ctx context.Context,
	root string,
	subClient *logpuller.SubscriptionClient,
	pdClock pdutil.Clock,
) EventStore {
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
		pdClock:   pdClock,
		subClient: subClient,

		dbs:            make([]*pebble.DB, 0, dbCount),
		chs:            make([]*chann.UnlimitedChannel[kvEventsAndCallback, uint64], 0, dbCount),
		writeTaskPools: make([]*writeTaskPool, 0, dbCount),

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
		store.chs = append(store.chs, chann.NewUnlimitedChannel[kvEventsAndCallback, uint64](nil, kvEventsAndCallbackSizer))
		store.writeTaskPools = append(store.writeTaskPools, newWriteTaskPool(store, store.dbs[i], store.chs[i], writeWorkerNumPerDB))
	}
	store.dispatcherMeta.dispatcherStats = make(map[common.DispatcherID]*dispatcherStat)
	store.dispatcherMeta.subscriptionStats = make(map[logpuller.SubscriptionID]*subscriptionStat)
	store.dispatcherMeta.tableToDispatchers = make(map[int64]map[common.DispatcherID]bool)

	// recv and handle messages
	messageCenter := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	store.messageCenter = messageCenter
	messageCenter.RegisterHandler(messaging.EventStoreTopic, store.handleMessage)

	return store
}

type writeTaskPool struct {
	store     *eventStore
	db        *pebble.DB
	dataCh    *chann.UnlimitedChannel[kvEventsAndCallback, uint64]
	workerNum int
}

func newWriteTaskPool(store *eventStore, db *pebble.DB, ch *chann.UnlimitedChannel[kvEventsAndCallback, uint64], workerNum int) *writeTaskPool {
	return &writeTaskPool{
		store:     store,
		db:        db,
		dataCh:    ch,
		workerNum: workerNum,
	}
}

func (p *writeTaskPool) run(_ context.Context) {
	p.store.wg.Add(p.workerNum)
	for i := 0; i < p.workerNum; i++ {
		go func() {
			defer p.store.wg.Done()
			buffer := make([]kvEventsAndCallback, 0, 1024)
			for {
				events, ok := p.dataCh.GetMultipleNoGroup(buffer)
				if !ok {
					return
				}
				p.store.writeEvents(p.db, events)
				for i := range events {
					events[i].callback()
				}
				buffer = buffer[:0]
			}
		}()
	}
}

func (e *eventStore) Name() string {
	return appcontext.EventStore
}

func (e *eventStore) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	for _, p := range e.writeTaskPools {
		p := p
		eg.Go(func() error {
			p.run(ctx)
			return nil
		})
	}

	// TODO: manage gcManager exit
	eg.Go(func() error {
		return e.gcManager.run(ctx, e.deleteEvents)
	})

	eg.Go(func() error {
		return e.updateMetrics(ctx)
	})

	eg.Go(func() error {
		return e.uploadStatePeriodically(ctx)
	})

	return eg.Wait()
}

func (e *eventStore) Close(ctx context.Context) error {
	e.wg.Wait()

	for _, db := range e.dbs {
		if err := db.Close(); err != nil {
			log.Error("failed to close pebble db", zap.Error(err))
		}
	}
	return nil
}

func (e *eventStore) RegisterDispatcher(
	dispatcherID common.DispatcherID,
	tableSpan *heartbeatpb.TableSpan,
	startTs uint64,
	notifier ResolvedTsNotifier,
	onlyReuse bool,
) (bool, error) {
	log.Info("register dispatcher",
		zap.Any("dispatcherID", dispatcherID),
		zap.String("span", tableSpan.String()),
		zap.Uint64("startTs", startTs))

	start := time.Now()
	defer func() {
		log.Info("register dispatcher done",
			zap.Any("dispatcherID", dispatcherID),
			zap.String("span", tableSpan.String()),
			zap.Uint64("startTs", startTs),
			zap.Duration("duration", time.Since(start)))
	}()

	stat := &dispatcherStat{
		dispatcherID: dispatcherID,
		tableSpan:    tableSpan,
		checkpointTs: startTs,
	}

	e.dispatcherMeta.Lock()
	if candidateIDs, ok := e.dispatcherMeta.tableToDispatchers[tableSpan.TableID]; ok {
		for candidateID := range candidateIDs {
			candidateDispatcher, ok := e.dispatcherMeta.dispatcherStats[candidateID]
			if !ok {
				log.Panic("should not happen")
			}
			if candidateDispatcher.tableSpan.Equal(tableSpan) {
				subscriptionStat, ok := e.dispatcherMeta.subscriptionStats[candidateDispatcher.subID]
				if !ok {
					log.Panic("should not happen")
				}
				// check whether startTs is in the range [checkpointTs, resolvedTs]
				// for `[checkpointTs`: because we want data > startTs, so data <= checkpointTs == startTs deleted is ok.
				// for `resolvedTs]`: startTs == resolvedTs is a special case that no resolved ts has been recieved, so it is ok.
				if subscriptionStat.checkpointTs.Load() <= startTs && startTs <= subscriptionStat.resolvedTs.Load() {
					stat.subID = candidateDispatcher.subID
					e.dispatcherMeta.dispatcherStats[dispatcherID] = stat
					// add dispatcher to existing subscription and return
					subscriptionStat.dispatchers.Lock()
					subscriptionStat.dispatchers.notifiers[dispatcherID] = notifier
					subscriptionStat.dispatchers.Unlock()
					candidateIDs[dispatcherID] = true
					e.dispatcherMeta.Unlock()
					log.Info("reuse existing subscription",
						zap.Any("dispatcherID", dispatcherID),
						zap.Uint64("subID", uint64(stat.subID)),
						zap.Uint64("checkpointTs", subscriptionStat.checkpointTs.Load()),
						zap.Uint64("startTs", startTs))
					return true, nil
				}
			}
		}
	}
	e.dispatcherMeta.Unlock()

	if onlyReuse {
		return false, nil
	}

	// cannot share data from existing subscription, create a new subscription

	// TODO: hash span is only needed when we need to reuse data after restart
	// (if we finally decide not to reuse data after restart, use round robin instead)
	// But if we need to share data for sub span, we need hash table id instead.
	chIndex := common.HashTableSpan(tableSpan, len(e.chs))
	stat.subID = e.subClient.AllocSubscriptionID()
	subStat := &subscriptionStat{
		subID:   stat.subID,
		tableID: tableSpan.TableID,
		dbIndex: chIndex,
		eventCh: e.chs[chIndex],
	}

	e.dispatcherMeta.Lock()
	e.dispatcherMeta.dispatcherStats[dispatcherID] = stat
	subStat.dispatchers.notifiers = make(map[common.DispatcherID]ResolvedTsNotifier)
	subStat.dispatchers.notifiers[dispatcherID] = notifier
	subStat.checkpointTs.Store(startTs)
	subStat.resolvedTs.Store(startTs)
	subStat.maxEventCommitTs.Store(startTs)
	e.dispatcherMeta.subscriptionStats[stat.subID] = subStat

	dispatchersForSameTable, ok := e.dispatcherMeta.tableToDispatchers[tableSpan.TableID]
	if !ok {
		e.dispatcherMeta.tableToDispatchers[tableSpan.TableID] = map[common.DispatcherID]bool{dispatcherID: true}
	} else {
		dispatchersForSameTable[dispatcherID] = true
	}
	e.dispatcherMeta.Unlock()

	consumeKVEvents := func(kvs []common.RawKVEntry, finishCallback func()) bool {
		subStat.maxEventCommitTs.Store(kvs[len(kvs)-1].CRTs)
		subStat.eventCh.Push(kvEventsAndCallback{
			subID:    subStat.subID,
			tableID:  subStat.tableID,
			kvs:      kvs,
			callback: finishCallback,
		})
		return true
	}
	advanceResolvedTs := func(ts uint64) {
		subStat.resolvedTs.Store(ts)
		subStat.dispatchers.RLock()
		defer subStat.dispatchers.RUnlock()
		for _, notifier := range subStat.dispatchers.notifiers {
			notifier(ts, subStat.maxEventCommitTs.Load())
		}
		CounterResolved.Inc()
	}
	// Note: don't hold any lock when call Subscribe
	e.subClient.Subscribe(stat.subID, *tableSpan, startTs, consumeKVEvents, advanceResolvedTs, 600)
	metrics.EventStoreSubscriptionGauge.Inc()
	return true, nil
}

func (e *eventStore) UnregisterDispatcher(dispatcherID common.DispatcherID) error {
	log.Info("unregister dispatcher", zap.Stringer("dispatcherID", dispatcherID))
	defer func() {
		log.Info("unregister dispatcher done", zap.Any("dispatcherID", dispatcherID))
	}()
	e.dispatcherMeta.Lock()
	defer e.dispatcherMeta.Unlock()
	stat, ok := e.dispatcherMeta.dispatcherStats[dispatcherID]
	if !ok {
		return nil
	}
	subID := stat.subID
	tableID := stat.tableSpan.TableID
	delete(e.dispatcherMeta.dispatcherStats, dispatcherID)

	// delete the dispatcher from subscription
	subscriptionStat, ok := e.dispatcherMeta.subscriptionStats[subID]
	if !ok {
		log.Panic("should not happen")
	}
	delete(subscriptionStat.dispatchers.notifiers, dispatcherID)
	if len(subscriptionStat.dispatchers.notifiers) == 0 {
		delete(e.dispatcherMeta.subscriptionStats, subID)
		// TODO: do we need unlock before puller.Unsubscribe?
		e.subClient.Unsubscribe(subID)
		metrics.EventStoreSubscriptionGauge.Dec()
	}

	// delete the dispatcher from table subscriptions
	dispatchersForSameTable, ok := e.dispatcherMeta.tableToDispatchers[tableID]
	if !ok {
		log.Panic("should not happen")
	}
	delete(dispatchersForSameTable, dispatcherID)
	if len(dispatchersForSameTable) == 0 {
		delete(e.dispatcherMeta.tableToDispatchers, tableID)
	}

	return nil
}

func (e *eventStore) UpdateDispatcherCheckpointTs(
	dispatcherID common.DispatcherID,
	checkpointTs uint64,
) error {
	// e.dispatcherMeta.RLock()
	// defer e.dispatcherMeta.RUnlock()
	// if stat, ok := e.dispatcherMeta.dispatcherStats[dispatcherID]; ok {
	// 	stat.checkpointTs = checkpointTs
	// 	subscriptionStat := e.dispatcherMeta.subscriptionStats[stat.subID]
	// 	// calculate the new checkpoint ts of the subscription
	// 	newCheckpointTs := uint64(0)
	// 	for dispatcherID := range subscriptionStat.ids {
	// 		dispatcherStat := e.dispatcherMeta.dispatcherStats[dispatcherID]
	// 		if newCheckpointTs == 0 || dispatcherStat.checkpointTs < newCheckpointTs {
	// 			newCheckpointTs = dispatcherStat.checkpointTs
	// 		}
	// 	}
	// 	if newCheckpointTs == 0 {
	// 		return nil
	// 	}
	// 	if newCheckpointTs < subscriptionStat.checkpointTs {
	// 		log.Panic("should not happen",
	// 			zap.Uint64("newCheckpointTs", newCheckpointTs),
	// 			zap.Uint64("oldCheckpointTs", subscriptionStat.checkpointTs))
	// 	}
	// 	if subscriptionStat.checkpointTs < newCheckpointTs {
	// 		e.gcManager.addGCItem(
	// 			subscriptionStat.chIndex,
	// 			subscriptionStat.uniqueKeyID,
	// 			stat.tableSpan.TableID,
	// 			subscriptionStat.checkpointTs,
	// 			newCheckpointTs,
	// 		)
	// 		if log.GetLevel() <= zap.DebugLevel {
	// 			log.Debug("update checkpoint ts",
	// 				zap.Any("dispatcherID", dispatcherID),
	// 				zap.Uint64("subID", uint64(stat.subID)),
	// 				zap.Uint64("newCheckpointTs", newCheckpointTs),
	// 				zap.Uint64("oldCheckpointTs", subscriptionStat.checkpointTs))
	// 			subscriptionStat.checkpointTs = newCheckpointTs
	// 		}
	// 	}
	// }
	return nil
}

func (e *eventStore) GetDispatcherDMLEventState(dispatcherID common.DispatcherID) (bool, DMLEventState) {
	e.dispatcherMeta.RLock()
	defer e.dispatcherMeta.RUnlock()
	stat, ok := e.dispatcherMeta.dispatcherStats[dispatcherID]
	if !ok {
		log.Warn("fail to find dispatcher", zap.Any("dispatcherID", dispatcherID))
		return false, DMLEventState{
			// ResolvedTs:       subscriptionStat.resolvedTs,
			MaxEventCommitTs: math.MaxUint64,
		}
	}
	subscriptionStat := e.dispatcherMeta.subscriptionStats[stat.subID]
	return true, DMLEventState{
		// ResolvedTs:       subscriptionStat.resolvedTs,
		MaxEventCommitTs: subscriptionStat.maxEventCommitTs.Load(),
	}
}

func (e *eventStore) GetIterator(dispatcherID common.DispatcherID, dataRange common.DataRange) (EventIterator, error) {
	e.dispatcherMeta.RLock()
	stat, ok := e.dispatcherMeta.dispatcherStats[dispatcherID]
	if !ok {
		log.Warn("fail to find dispatcher", zap.Any("dispatcherID", dispatcherID))
		e.dispatcherMeta.RUnlock()
		return nil, nil
	}
	subscriptionStat := e.dispatcherMeta.subscriptionStats[stat.subID]
	if dataRange.StartTs < subscriptionStat.checkpointTs.Load() {
		log.Panic("should not happen",
			zap.Any("dispatcherID", dispatcherID),
			zap.Uint64("checkpointTs", subscriptionStat.checkpointTs.Load()),
			zap.Uint64("startTs", dataRange.StartTs))
	}
	db := e.dbs[subscriptionStat.dbIndex]
	e.dispatcherMeta.RUnlock()

	// convert range before pass it to pebble: (startTs, endTs] is equal to [startTs + 1, endTs + 1)
	start := EncodeKeyPrefix(uint64(subscriptionStat.subID), stat.tableSpan.TableID, dataRange.StartTs+1)
	end := EncodeKeyPrefix(uint64(subscriptionStat.subID), stat.tableSpan.TableID, dataRange.EndTs+1)
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
		tableID:      stat.tableSpan.TableID,
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
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			e.updateMetricsOnce()
		}
	}
}

func (e *eventStore) updateMetricsOnce() {
	log.Info("try update metrics")
	currentTime := e.pdClock.CurrentTime()
	currentPhyTs := oracle.GetPhysical(currentTime)
	minResolvedTs := uint64(0)
	e.dispatcherMeta.RLock()
	for _, subscriptionStat := range e.dispatcherMeta.subscriptionStats {
		// resolved ts lag
		resolvedTs := subscriptionStat.resolvedTs.Load()
		resolvedPhyTs := oracle.ExtractPhysical(resolvedTs)
		resolvedLag := float64(currentPhyTs-resolvedPhyTs) / 1e3
		metrics.EventStoreDispatcherResolvedTsLagHist.Observe(float64(resolvedLag))
		if minResolvedTs == 0 || resolvedTs < minResolvedTs {
			minResolvedTs = resolvedTs
		}
		// checkpoint ts lag
		checkpointTs := subscriptionStat.checkpointTs.Load()
		watermarkPhyTs := oracle.ExtractPhysical(checkpointTs)
		watermarkLag := float64(currentPhyTs-watermarkPhyTs) / 1e3
		metrics.EventStoreDispatcherWatermarkLagHist.Observe(float64(watermarkLag))
	}
	e.dispatcherMeta.RUnlock()
	if minResolvedTs == 0 {
		return
	}
	minResolvedPhyTs := oracle.ExtractPhysical(minResolvedTs)
	eventStoreResolvedTsLag := float64(currentPhyTs-minResolvedPhyTs) / 1e3
	metrics.EventStoreResolvedTsLagGauge.Set(eventStoreResolvedTsLag)
}

func (e *eventStore) writeEvents(db *pebble.DB, events []kvEventsAndCallback) error {
	metrics.EventStoreWriteRequestsCount.Inc()
	batch := db.NewBatch()
	kvCount := 0
	for _, event := range events {
		for _, kv := range event.kvs {
			kvCount += 1
			key := EncodeKey(uint64(event.subID), event.tableID, &kv)
			value := kv.Encode()
			compressedValue := e.encoder.EncodeAll(value, nil)
			ratio := float64(len(value)) / float64(len(compressedValue))
			metrics.EventStoreCompressRatio.Set(ratio)
			if err := batch.Set(key, compressedValue, pebble.NoSync); err != nil {
				log.Panic("failed to update pebble batch", zap.Error(err))
			}
		}
	}
	CounterKv.Add(float64(kvCount))
	metrics.EventStoreWriteBatchEventsCountHist.Observe(float64(kvCount))
	metrics.EventStoreWriteBatchSizeHist.Observe(float64(batch.Len()))
	metrics.EventStoreWriteBytes.Add(float64(batch.Len()))
	start := time.Now()
	err := batch.Commit(pebble.NoSync)
	metrics.EventStoreWriteDurationHistogram.Observe(float64(time.Since(start).Milliseconds()) / 1000)
	return err
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

func (e *eventStore) handleMessage(_ context.Context, targetMessage *messaging.TargetMessage) error {
	for _, msg := range targetMessage.Message {
		switch msg.(type) {
		case *common.LogCoordinatorBroadcastRequest:
			e.coordinatorInfo.Lock()
			e.coordinatorInfo.id = targetMessage.From
			e.coordinatorInfo.Unlock()
		default:
			log.Panic("invalid message type", zap.Any("msg", msg))
		}
	}
	return nil
}

func (e *eventStore) uploadStatePeriodically(ctx context.Context) error {
	tick := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			e.dispatcherMeta.RLock()
			state := &logservicepb.EventStoreState{
				Subscriptions: make(map[int64]*logservicepb.SubscriptionStates),
			}
			for tableID, dispatcherIDs := range e.dispatcherMeta.tableToDispatchers {
				subStates := make([]*logservicepb.SubscriptionState, 0, len(dispatcherIDs))
				subIDs := make(map[logpuller.SubscriptionID]bool)
				for dispatcherID := range dispatcherIDs {
					dispatcherStat := e.dispatcherMeta.dispatcherStats[dispatcherID]
					subID := dispatcherStat.subID
					subStat := e.dispatcherMeta.subscriptionStats[subID]
					if _, ok := subIDs[subID]; ok {
						continue
					}
					subStates = append(subStates, &logservicepb.SubscriptionState{
						SubID:        uint64(subID),
						Span:         dispatcherStat.tableSpan,
						CheckpointTs: subStat.checkpointTs.Load(),
						ResolvedTs:   subStat.resolvedTs.Load(),
					})
					subIDs[subID] = true
				}
				sort.Slice(subStates, func(i, j int) bool {
					return subStates[i].SubID < subStates[j].SubID
				})
				state.Subscriptions[tableID] = &logservicepb.SubscriptionStates{
					Subscriptions: subStates,
				}
			}

			message := messaging.NewSingleTargetMessage(e.coordinatorInfo.id, messaging.LogCoordinatorTopic, state)
			e.dispatcherMeta.RUnlock()
			// just ignore messagees fail to send
			if err := e.messageCenter.SendEvent(message); err != nil {
				log.Debug("send broadcast message to node failed", zap.Error(err))
			}
		}
	}
}
