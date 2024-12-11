// Copyright 2023 PingCAP, Inc.
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

package logpuller

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/logservice/txnutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/utils/dynstream"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	kvclientv2 "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// Maximum total sleep time(in ms), 20 seconds.
	tikvRequestMaxBackoff = 20000

	// TiCDC always interacts with region leader, every time something goes wrong,
	// failed region will be reloaded via `BatchLoadRegionsWithKeyRange` API. So we
	// don't need to force reload region anymore.
	regionScheduleReload = false

	loadRegionRetryInterval time.Duration = 100 * time.Millisecond
	resolveLockMinInterval  time.Duration = 10 * time.Second
)

var (
	metricFeedNotLeaderCounter        = metrics.EventFeedErrorCounter.WithLabelValues("NotLeader")
	metricFeedEpochNotMatchCounter    = metrics.EventFeedErrorCounter.WithLabelValues("EpochNotMatch")
	metricFeedRegionNotFoundCounter   = metrics.EventFeedErrorCounter.WithLabelValues("RegionNotFound")
	metricFeedDuplicateRequestCounter = metrics.EventFeedErrorCounter.WithLabelValues("DuplicateRequest")
	metricFeedUnknownErrorCounter     = metrics.EventFeedErrorCounter.WithLabelValues("Unknown")
	metricFeedRPCCtxUnavailable       = metrics.EventFeedErrorCounter.WithLabelValues("RPCCtxUnavailable")
	metricStoreSendRequestErr         = metrics.EventFeedErrorCounter.WithLabelValues("SendRequestToStore")
	metricKvIsBusyCounter             = metrics.EventFeedErrorCounter.WithLabelValues("KvIsBusy")

	metricSubscriptionClientDSChannelSize     = metrics.DynamicStreamEventChanSize.WithLabelValues("event-store")
	metricSubscriptionClientDSPendingQueueLen = metrics.DynamicStreamPendingQueueLen.WithLabelValues("event-store")
	metricEventStoreDSAddPathNum              = metrics.DynamicStreamAddPathNum.WithLabelValues("event-store")
	metricEventStoreDSRemovePathNum           = metrics.DynamicStreamRemovePathNum.WithLabelValues("event-store")
	// metricEventStoreDSArrageStreamNum         = metrics.DynamicStreamArrangeStreamNum.WithLabelValues("event-store")
)

// To generate an ID for a new subscription.
var subscriptionIDGen atomic.Uint64

// subscriptionID is a unique identifier for a subscription.
// It is used as `RequestId` in region requests to remote store.
type SubscriptionID uint64

const InvalidSubscriptionID SubscriptionID = 0

type resolveLockTask struct {
	regionID uint64
	targetTs uint64
	state    *regionlock.LockedRangeState
	create   time.Time
}

// rangeTask represents a task to subscribe a range span of a table.
// It can be a part of a table or a whole table, it also can be a part of a region.
type rangeTask struct {
	span           heartbeatpb.TableSpan
	subscribedSpan *subscribedSpan
}

// subscribedSpan represents a span to subscribe.
// It contains a sub span of a table(or the total span of a table),
// the startTs of the table, and the output event channel.
type subscribedSpan struct {
	subID   SubscriptionID
	startTs uint64

	// The target span
	span heartbeatpb.TableSpan
	// The range lock of the span,
	// it is used to prevent duplicate requests to the same region range,
	// and it also used to calculate this table's resolvedTs.
	rangeLock *regionlock.RangeLock

	consumeKVEvents func(events []common.RawKVEntry, wakeCallback func()) bool

	advanceResolvedTs func(ts uint64)

	advanceInterval int64

	kvEventsCache []common.RawKVEntry

	// To handle span removing.
	stopped atomic.Bool

	// To handle stale lock resolvings.
	tryResolveLock     func(regionID uint64, state *regionlock.LockedRangeState)
	staleLocksTargetTs atomic.Uint64

	lastAdvanceTime atomic.Int64
	// This is used to calculate the resolvedTs lag for metrics.
	resolvedTs atomic.Uint64
}

type SubscriptionClientConfig struct {
	// The number of region request workers to send region task for every tikv store
	RegionRequestWorkerPerStore uint
}

type sharedClientMetrics struct {
	// regionLockDuration    prometheus.Observer
	// regionLocateDuration  prometheus.Observer
	// regionConnectDuration prometheus.Observer
	batchResolvedSize prometheus.Observer
	kvCounter         prometheus.Counter
	resolvedTsCounter prometheus.Counter
	// lockResolveWaitDuration prometheus.Observer
	// lockResolveRunDuration  prometheus.Observer
	// slowInitializeRegion prometheus.Gauge
}

// SubscriptionClient is used to subscribe events of table ranges from TiKV.
// All exported Methods are thread-safe.
type SubscriptionClient struct {
	config     *SubscriptionClientConfig
	metrics    sharedClientMetrics
	clusterID  uint64
	filterLoop bool

	pd           pd.Client
	regionCache  *tikv.RegionCache
	pdClock      pdutil.Clock
	lockResolver txnutil.LockResolver

	ds dynstream.DynamicStream[int, SubscriptionID, regionEvent, *subscribedSpan, *regionEventHandler]

	// the credential to connect tikv
	credential *security.Credential

	totalSpans struct {
		sync.RWMutex
		spanMap map[SubscriptionID]*subscribedSpan
	}

	// rangeTaskCh is used to receive range tasks.
	// The tasks will be handled in `handleRangeTask` goroutine.
	rangeTaskCh chan rangeTask
	// regionCh is used to receive region tasks have been locked in rangeLock.
	// The region will be handled in `handleRegions` goroutine.
	regionCh chan regionInfo
	// resolveLockTaskCh is used to receive resolve lock tasks.
	// The tasks will be handled in `handleResolveLockTasks` goroutine.
	resolveLockTaskCh chan resolveLockTask
	// errCh is used to receive region errors.
	// The errors will be handled in `handleErrors` goroutine.
	errCache *errCache
}

// NewSubscriptionClient creates a client.
func NewSubscriptionClient(
	config *SubscriptionClientConfig,
	pd pd.Client,
	regionCache *tikv.RegionCache,
	pdClock pdutil.Clock,
	lockResolver txnutil.LockResolver,
	credential *security.Credential,
) *SubscriptionClient {
	subClient := &SubscriptionClient{
		config:     config,
		filterLoop: false, // FIXME

		pd:           pd,
		regionCache:  regionCache,
		pdClock:      pdClock,
		lockResolver: lockResolver,

		credential: credential,

		rangeTaskCh:       make(chan rangeTask, 1024),
		regionCh:          make(chan regionInfo, 1024),
		resolveLockTaskCh: make(chan resolveLockTask, 1024),
		errCache:          newErrCache(),
	}
	subClient.totalSpans.spanMap = make(map[SubscriptionID]*subscribedSpan)

	option := dynstream.NewOption()
	option.BatchCount = 1024
	option.UseBuffer = true
	ds := dynstream.NewParallelDynamicStream(
		func(subID SubscriptionID) uint64 { return uint64(subID) },
		&regionEventHandler{subClient: subClient},
		option,
	)
	ds.Start()
	subClient.ds = ds

	subClient.initMetrics()
	return subClient
}

func (s *SubscriptionClient) Name() string {
	return appcontext.SubscriptionClient
}

// AllocsubscriptionID gets an ID can be used in `Subscribe`.
func (s *SubscriptionClient) AllocSubscriptionID() SubscriptionID {
	return SubscriptionID(subscriptionIDGen.Add(1))
}

func (s *SubscriptionClient) initMetrics() {
	// TODO: fix metrics
	s.metrics.batchResolvedSize = metrics.BatchResolvedEventSize.WithLabelValues("event-store")
}

func (s *SubscriptionClient) updateMetrics(ctx context.Context) error {
	ticker1 := time.NewTicker(10 * time.Second)
	ticker2 := time.NewTicker(5 * time.Millisecond)
	defer ticker1.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker1.C:
			resolvedTsLag := s.GetResolvedTsLag()
			if resolvedTsLag > 0 {
				metrics.LogPullerResolvedTsLag.Set(resolvedTsLag)
			}
		case <-ticker2.C:
			dsMetrics := s.ds.GetMetrics()
			metricSubscriptionClientDSChannelSize.Set(float64(dsMetrics.EventChanSize))
			metricSubscriptionClientDSPendingQueueLen.Set(float64(dsMetrics.PendingQueueLen))
			metricEventStoreDSAddPathNum.Set(float64(dsMetrics.AddPath))
			metricEventStoreDSRemovePathNum.Set(float64(dsMetrics.RemovePath))
		}
	}
}

// Subscribe the given table span.
// NOTE: `span.TableID` must be set correctly.
// It new a subscribedSpan and store it in `s.totalSpans`,
// and send a rangeTask to `s.rangeTaskCh`.
// The rangeTask will be handled in `handleRangeTasks` goroutine.
func (s *SubscriptionClient) Subscribe(
	subID SubscriptionID,
	span heartbeatpb.TableSpan,
	startTs uint64,
	consumeKVEvents func(raw []common.RawKVEntry, wakeCallback func()) bool,
	advanceResolvedTs func(ts uint64),
	advanceInterval int64,
) {
	if span.TableID == 0 {
		log.Panic("subscription client subscribe with zero TableID")
		return
	}
	log.Info("subscribes span",
		zap.Uint64("subscriptionID", uint64(subID)),
		zap.String("span", span.String()))
	defer func() {
		log.Info("subscribes span done",
			zap.Uint64("subscriptionID", uint64(subID)),
			zap.String("span", span.String()))
	}()

	rt := s.newSubscribedSpan(subID, span, startTs, consumeKVEvents, advanceResolvedTs, advanceInterval)
	s.totalSpans.Lock()
	s.totalSpans.spanMap[subID] = rt
	s.totalSpans.Unlock()

	s.ds.AddPath(rt.subID, rt, dynstream.AreaSettings{})

	s.rangeTaskCh <- rangeTask{span: span, subscribedSpan: rt}
}

// Unsubscribe the given table span. All covered regions will be deregistered asynchronously.
// NOTE: `span.TableID` must be set correctly.
func (s *SubscriptionClient) Unsubscribe(subID SubscriptionID) {
	// NOTE: `subID` is cleared from `s.totalSpans` in `onTableDrained`.
	s.totalSpans.Lock()
	rt := s.totalSpans.spanMap[subID]
	s.totalSpans.Unlock()
	if rt == nil {
		log.Warn("unknown subscription", zap.Uint64("subscriptionID", uint64(subID)))
		return
	}
	s.ds.RemovePath(rt.subID)
	s.setTableStopped(rt)

	log.Info("unsubscribe span success",
		zap.Uint64("subscriptionID", uint64(rt.subID)),
		zap.Bool("exists", rt != nil))
}

func (s *SubscriptionClient) wakeSubscription(subID SubscriptionID) {
	s.ds.Wake(subID)
}

// ResolveLock is a function. If outsider subscribers find a span resolved timestamp is
// advanced slowly or stopped, they can try to resolve locks in the given span.
func (s *SubscriptionClient) ResolveLock(subID SubscriptionID, targetTs uint64) {
	s.totalSpans.Lock()
	rt := s.totalSpans.spanMap[subID]
	s.totalSpans.Unlock()
	if rt != nil {
		rt.resolveStaleLocks(targetTs)
	}
}

// RegionCount returns subscribed region count for the span.
func (s *SubscriptionClient) RegionCount(subID SubscriptionID) uint64 {
	s.totalSpans.RLock()
	defer s.totalSpans.RUnlock()
	if rt := s.totalSpans.spanMap[subID]; rt != nil {
		return uint64(rt.rangeLock.Len())
	}
	return 0
}

func (s *SubscriptionClient) Run(ctx context.Context) error {
	// s.consume = consume
	if s.pd == nil {
		log.Warn("subsription client should be in test mode, skip run")
		return nil
	}
	s.clusterID = s.pd.GetClusterID(ctx)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error { return s.updateMetrics(ctx) })
	g.Go(func() error { return s.handleRangeTasks(ctx) })
	g.Go(func() error { return s.handleRegions(ctx, g) })
	g.Go(func() error { return s.handleErrors(ctx) })
	g.Go(func() error { return s.handleResolveLockTasks(ctx) })
	g.Go(func() error { return s.logSlowRegions(ctx) })
	g.Go(func() error { return s.errCache.dispatch(ctx) })

	log.Info("subscription client starts")
	defer log.Info("subscription client exits")
	return g.Wait()
}

// Close closes the client. Must be called after `Run` returns.
func (s *SubscriptionClient) Close(ctx context.Context) error {
	// FIXME: close and drain all channels
	s.ds.Close()
	return nil
}

func (s *SubscriptionClient) setTableStopped(rt *subscribedSpan) {
	log.Info("subscription client starts to stop table",
		zap.Uint64("subscriptionID", uint64(rt.subID)))

	// Set stopped to true so we can stop handling region events from the table.
	// Then send a special singleRegionInfo to regionRouter to deregister the table
	// from all TiKV instances.
	if rt.stopped.CompareAndSwap(false, true) {
		s.regionCh <- regionInfo{subscribedSpan: rt}
		if rt.rangeLock.Stop() {
			s.onTableDrained(rt)
		}
	}
}

func (s *SubscriptionClient) onTableDrained(rt *subscribedSpan) {
	log.Info("subscription client stop span is finished",
		zap.Uint64("subscriptionID", uint64(rt.subID)))

	s.totalSpans.Lock()
	defer s.totalSpans.Unlock()
	delete(s.totalSpans.spanMap, rt.subID)
}

// Note: don't block the caller, otherwise there may be deadlock
func (s *SubscriptionClient) onRegionFail(errInfo regionErrorInfo) {
	s.errCache.add(errInfo)
}

// requestedStore represents a store that has been connected.
type requestedStore struct {
	storeID   uint64
	storeAddr string
	// Use to select a worker to send request.
	nextWorker     atomic.Uint32
	requestWorkers []*regionRequestWorker
}

func (rs *requestedStore) getRequestWorker() *regionRequestWorker {
	index := rs.nextWorker.Add(1) % uint32(len(rs.requestWorkers))
	return rs.requestWorkers[index]
}

// handleRegions receives regionInfo from regionCh and attch rpcCtx to them,
// then send them to corresponding requestedStore.
func (s *SubscriptionClient) handleRegions(ctx context.Context, eg *errgroup.Group) error {
	stores := make(map[uint64]*requestedStore) // storeId -> requestedStore
	getStore := func(storeID uint64, storeAddr string) *requestedStore {
		var rs *requestedStore
		if rs = stores[storeID]; rs != nil {
			return rs
		}
		rs = &requestedStore{storeID: storeID, storeAddr: storeAddr}
		stores[storeID] = rs
		for i := uint(0); i < s.config.RegionRequestWorkerPerStore; i++ {
			requestWorker := newRegionRequestWorker(ctx, s, s.credential, eg, rs)
			rs.requestWorkers = append(rs.requestWorkers, requestWorker)
		}

		return rs
	}

	defer func() {
		for _, rs := range stores {
			for _, w := range rs.requestWorkers {
				close(w.requestsCh)
				for range w.requestsCh {
					// TODO: do we need handle it?
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case region := <-s.regionCh:
			if region.isStopped() {
				for _, rs := range stores {
					for _, worker := range rs.requestWorkers {
						worker.requestsCh <- region
					}
				}
				continue
			}

			log.Info("subscription client try request a region",
				zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
				zap.Uint64("regionID", region.verID.GetID()))

			region, ok := s.attachRPCContextForRegion(ctx, region)
			// If attachRPCContextForRegion fails, the region will be re-scheduled.
			if !ok {
				continue
			}

			store := getStore(region.rpcCtx.Peer.StoreId, region.rpcCtx.Addr)
			worker := store.getRequestWorker()
			worker.requestsCh <- region

			log.Info("subscription client will request a region",
				zap.Uint64("workID", worker.workerID),
				zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
				zap.Uint64("regionID", region.verID.GetID()),
				zap.Uint64("storeID", store.storeID),
				zap.String("addr", store.storeAddr))
		}
	}
}

func (s *SubscriptionClient) attachRPCContextForRegion(ctx context.Context, region regionInfo) (regionInfo, bool) {
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := s.regionCache.GetTiKVRPCContext(bo, region.verID, kvclientv2.ReplicaReadLeader, 0)
	if rpcCtx != nil {
		region.rpcCtx = rpcCtx
		return region, true
	}
	if err != nil {
		log.Debug("subscription client get rpc context fail",
			zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.Error(err))
	}
	s.onRegionFail(newRegionErrorInfo(region, &rpcCtxUnavailableErr{verID: region.verID}))
	return region, false
}

func (s *SubscriptionClient) handleRangeTasks(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	// Limit the concurrent number of goroutines to convert range tasks to region tasks.
	g.SetLimit(1024)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-s.rangeTaskCh:
			g.Go(func() error { return s.divideSpanAndScheduleRegionRequests(ctx, task.span, task.subscribedSpan) })
		}
	}
}

// divideSpanAndScheduleRegionRequests processes the specified span by dividing it into
// manageable regions and schedules requests to subscribe to these regions.
// 1. Load regions from PD.
// 2. Find the intersection of each region.span and the subscribedSpan.span.
// 3. Schedule a region request to subscribe the region.
func (s *SubscriptionClient) divideSpanAndScheduleRegionRequests(
	ctx context.Context,
	span heartbeatpb.TableSpan,
	subscribedSpan *subscribedSpan,
) error {
	// Limit the number of regions loaded at a time to make the load more stable.
	limit := 1024
	nextSpan := span
	backoffBeforeLoad := false
	for {
		if backoffBeforeLoad {
			if err := util.Hang(ctx, loadRegionRetryInterval); err != nil {
				return err
			}
			backoffBeforeLoad = false
		}
		log.Debug("subscription client is going to load regions",
			zap.Uint64("subscriptionID", uint64(subscribedSpan.subID)),
			zap.Any("span", nextSpan))

		backoff := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		regions, err := s.regionCache.BatchLoadRegionsWithKeyRange(backoff, nextSpan.StartKey, nextSpan.EndKey, limit)
		if err != nil {
			log.Warn("subscription client load regions failed",
				zap.Uint64("subscriptionID", uint64(subscribedSpan.subID)),
				zap.Any("span", nextSpan),
				zap.Error(err))
			backoffBeforeLoad = true
			continue
		}

		regionMetas := make([]*metapb.Region, 0, len(regions))
		for _, region := range regions {
			if meta := region.GetMeta(); meta != nil {
				regionMetas = append(regionMetas, meta)
			}
		}
		regionMetas = regionlock.CutRegionsLeftCoverSpan(regionMetas, nextSpan)
		if len(regionMetas) == 0 {
			log.Warn("subscription client load regions with holes",
				zap.Uint64("subscriptionID", uint64(subscribedSpan.subID)),
				zap.Any("span", nextSpan))
			backoffBeforeLoad = true
			continue
		}

		for _, regionMeta := range regionMetas {
			regionSpan := heartbeatpb.TableSpan{
				StartKey: regionMeta.StartKey,
				EndKey:   regionMeta.EndKey,
			}
			// NOTE: the End key return by the PD API will be nil to represent the biggest key.
			// So we need to fix it by calling spanz.HackSpan.
			regionSpan = common.HackTableSpan(regionSpan)

			// Find the intersection of the regionSpan returned by PD and the subscribedSpan.span.
			// The intersection is the span that needs to be subscribed.
			intersectSpan := common.GetIntersectSpan(subscribedSpan.span, regionSpan)
			if common.IsEmptySpan(intersectSpan) {
				log.Panic("subscription client check spans intersect shouldn't fail",
					zap.Uint64("subscriptionID", uint64(subscribedSpan.subID)))
			}

			verID := tikv.NewRegionVerID(regionMeta.Id, regionMeta.RegionEpoch.ConfVer, regionMeta.RegionEpoch.Version)
			regionInfo := newRegionInfo(verID, intersectSpan, nil, subscribedSpan)

			// Schedule a region request to subscribe the region.
			s.scheduleRegionRequest(ctx, regionInfo)

			nextSpan.StartKey = regionMeta.EndKey
			// If the nextSpan.StartKey is larger than the subscribedSpan.span.EndKey,
			// it means all span of the subscribedSpan have been requested. So we return.
			if spanz.EndCompare(nextSpan.StartKey, span.EndKey) >= 0 {
				return nil
			}
		}
	}
}

// scheduleRegionRequest locks the region's range and send the region to regionCh,
// which will be handled by handleRegions.
func (s *SubscriptionClient) scheduleRegionRequest(ctx context.Context, region regionInfo) {
	lockRangeResult := region.subscribedSpan.rangeLock.LockRange(
		ctx, region.span.StartKey, region.span.EndKey, region.verID.GetID(), region.verID.GetVer())

	if lockRangeResult.Status == regionlock.LockRangeStatusWait {
		lockRangeResult = lockRangeResult.WaitFn()
	}

	switch lockRangeResult.Status {
	case regionlock.LockRangeStatusSuccess:
		region.lockedRangeState = lockRangeResult.LockedRangeState
		select {
		case s.regionCh <- region:
		case <-ctx.Done():
		}
	case regionlock.LockRangeStatusStale:
		for _, r := range lockRangeResult.RetryRanges {
			s.scheduleRangeRequest(ctx, r, region.subscribedSpan)
		}
	default:
		return
	}
}

func (s *SubscriptionClient) scheduleRangeRequest(
	ctx context.Context, span heartbeatpb.TableSpan,
	subscribedSpan *subscribedSpan,
) {
	select {
	case <-ctx.Done():
	case s.rangeTaskCh <- rangeTask{span: span, subscribedSpan: subscribedSpan}:
	}
}

func (s *SubscriptionClient) handleErrors(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case errInfo := <-s.errCache.errCh:
			if err := s.doHandleError(ctx, errInfo); err != nil {
				return err
			}
		}
	}
}

func (s *SubscriptionClient) doHandleError(ctx context.Context, errInfo regionErrorInfo) error {
	if errInfo.subscribedSpan.rangeLock.UnlockRange(
		errInfo.span.StartKey, errInfo.span.EndKey,
		errInfo.verID.GetID(), errInfo.verID.GetVer(), errInfo.resolvedTs()) {
		s.onTableDrained(errInfo.subscribedSpan)
		return nil
	}

	err := errors.Cause(errInfo.err)
	switch eerr := err.(type) {
	case *eventError:
		innerErr := eerr.err
		log.Debug("cdc region error",
			zap.Uint64("subscriptionID", uint64(errInfo.subscribedSpan.subID)),
			zap.Stringer("error", innerErr))

		if notLeader := innerErr.GetNotLeader(); notLeader != nil {
			metricFeedNotLeaderCounter.Inc()
			s.regionCache.UpdateLeader(errInfo.verID, notLeader.GetLeader(), errInfo.rpcCtx.AccessIdx)
			s.scheduleRegionRequest(ctx, errInfo.regionInfo)
			return nil
		}
		if innerErr.GetEpochNotMatch() != nil {
			metricFeedEpochNotMatchCounter.Inc()
			s.scheduleRangeRequest(ctx, errInfo.span, errInfo.subscribedSpan)
			return nil
		}
		if innerErr.GetRegionNotFound() != nil {
			metricFeedRegionNotFoundCounter.Inc()
			s.scheduleRangeRequest(ctx, errInfo.span, errInfo.subscribedSpan)
			return nil
		}
		if innerErr.GetServerIsBusy() != nil {
			metricKvIsBusyCounter.Inc()
			s.scheduleRegionRequest(ctx, errInfo.regionInfo)
			return nil
		}
		if duplicated := innerErr.GetDuplicateRequest(); duplicated != nil {
			// TODO(qupeng): It's better to add a new machanism to deregister one region.
			metricFeedDuplicateRequestCounter.Inc()
			return errors.New("duplicate request")
		}
		if compatibility := innerErr.GetCompatibility(); compatibility != nil {
			return cerror.ErrVersionIncompatible.GenWithStackByArgs(compatibility)
		}
		if mismatch := innerErr.GetClusterIdMismatch(); mismatch != nil {
			return cerror.ErrClusterIDMismatch.GenWithStackByArgs(mismatch.Current, mismatch.Request)
		}

		log.Warn("empty or unknown cdc error",
			zap.Uint64("subscriptionID", uint64(errInfo.subscribedSpan.subID)),
			zap.Stringer("error", innerErr))
		metricFeedUnknownErrorCounter.Inc()
		s.scheduleRegionRequest(ctx, errInfo.regionInfo)
		return nil
	case *rpcCtxUnavailableErr:
		metricFeedRPCCtxUnavailable.Inc()
		s.scheduleRangeRequest(ctx, errInfo.span, errInfo.subscribedSpan)
		return nil
	case *sendRequestToStoreErr:
		metricStoreSendRequestErr.Inc()
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		s.regionCache.OnSendFail(bo, errInfo.rpcCtx, regionScheduleReload, err)
		s.scheduleRegionRequest(ctx, errInfo.regionInfo)
		return nil
	default:
		// TODO(qupeng): for some errors it's better to just deregister the region from TiKVs.
		log.Warn("subscription client meets an internal error, fail the changefeed",
			zap.Uint64("subscriptionID", uint64(errInfo.subscribedSpan.subID)),
			zap.Error(err))
		return err
	}
}

func (s *SubscriptionClient) handleResolveLockTasks(ctx context.Context) error {
	resolveLastRun := make(map[uint64]time.Time)

	gcResolveLastRun := func() {
		if len(resolveLastRun) > 1024 {
			copied := make(map[uint64]time.Time)
			now := time.Now()
			for regionID, lastRun := range resolveLastRun {
				if now.Sub(lastRun) < resolveLockMinInterval {
					resolveLastRun[regionID] = lastRun
				}
			}
			resolveLastRun = copied
		}
	}

	doResolve := func(regionID uint64, state *regionlock.LockedRangeState, targetTs uint64) {
		if state.ResolvedTs.Load() > targetTs || !state.Initialized.Load() {
			return
		}
		if lastRun, ok := resolveLastRun[regionID]; ok {
			if time.Since(lastRun) < resolveLockMinInterval {
				return
			}
		}

		if err := s.lockResolver.Resolve(ctx, regionID, targetTs); err != nil {
			log.Warn("subscription client resolve lock fail",
				zap.Uint64("regionID", regionID),
				zap.Error(err))
		}
		resolveLastRun[regionID] = time.Now()
	}

	gcTicker := time.NewTicker(resolveLockMinInterval * 3 / 2)
	defer gcTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gcTicker.C:
			gcResolveLastRun()
		case task := <-s.resolveLockTaskCh:
			doResolve(task.regionID, task.state, task.targetTs)
		}
	}
}

func (s *SubscriptionClient) logSlowRegions(ctx context.Context) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		currTime := s.pdClock.CurrentTime()
		s.totalSpans.RLock()
		slowInitializeRegion := 0
		for subscriptionID, rt := range s.totalSpans.spanMap {
			attr := rt.rangeLock.IterAll(nil)
			ckptTime := oracle.GetTimeFromTS(attr.SlowestRegion.ResolvedTs)
			if attr.SlowestRegion.Initialized {
				if currTime.Sub(ckptTime) > 2*resolveLockMinInterval {
					log.Info("subscription client finds a initialized slow region",
						zap.Uint64("subscriptionID", uint64(subscriptionID)),
						zap.Any("slowRegion", attr.SlowestRegion))
				}
			} else if currTime.Sub(attr.SlowestRegion.Created) > 10*time.Minute {
				slowInitializeRegion += 1
				log.Info("subscription client initializes a region too slow",
					zap.Uint64("subscriptionID", uint64(subscriptionID)),
					zap.Any("slowRegion", attr.SlowestRegion))
			} else if currTime.Sub(ckptTime) > 10*time.Minute {
				log.Info("subscription client finds a uninitialized slow region",
					zap.Uint64("subscriptionID", uint64(subscriptionID)),
					zap.Any("slowRegion", attr.SlowestRegion))
			}
			if len(attr.UnLockedRanges) > 0 {
				log.Info("subscription client holes exist",
					zap.Uint64("subscriptionID", uint64(subscriptionID)),
					zap.Any("holes", attr.UnLockedRanges))
			}
		}
		s.totalSpans.RUnlock()
	}
}

func (s *SubscriptionClient) newSubscribedSpan(
	subID SubscriptionID,
	span heartbeatpb.TableSpan,
	startTs uint64,
	consumeKVEvents func(raw []common.RawKVEntry, wakeCallback func()) bool,
	advanceResolvedTs func(ts uint64),
	advanceInterval int64,
) *subscribedSpan {
	rangeLock := regionlock.NewRangeLock(uint64(subID), span.StartKey, span.EndKey, startTs)

	rt := &subscribedSpan{
		subID:     subID,
		span:      span,
		startTs:   startTs,
		rangeLock: rangeLock,

		consumeKVEvents:   consumeKVEvents,
		advanceResolvedTs: advanceResolvedTs,
		advanceInterval:   advanceInterval,
	}
	rt.resolvedTs.Store(startTs)

	rt.tryResolveLock = func(regionID uint64, state *regionlock.LockedRangeState) {
		targetTs := rt.staleLocksTargetTs.Load()
		if state.ResolvedTs.Load() < targetTs && state.Initialized.Load() {
			s.resolveLockTaskCh <- resolveLockTask{
				regionID: regionID,
				targetTs: targetTs,
				state:    state,
				create:   time.Now(),
			}
		}
	}
	return rt
}

func (s *SubscriptionClient) GetResolvedTsLag() float64 {
	pullerMinResolvedTs := uint64(0)
	s.totalSpans.RLock()
	for _, rt := range s.totalSpans.spanMap {
		resolvedTs := rt.resolvedTs.Load()
		if pullerMinResolvedTs == 0 || resolvedTs < pullerMinResolvedTs {
			pullerMinResolvedTs = resolvedTs
		}
	}
	s.totalSpans.RUnlock()
	if pullerMinResolvedTs == 0 {
		return 0
	}
	phyResolvedTs := oracle.ExtractPhysical(pullerMinResolvedTs)
	lag := float64(oracle.GetPhysical(time.Now())-phyResolvedTs) / 1e3
	return lag
}

func (r *subscribedSpan) resolveStaleLocks(targetTs uint64) {
	util.MustCompareAndMonotonicIncrease(&r.staleLocksTargetTs, targetTs)
	res := r.rangeLock.IterAll(r.tryResolveLock)
	log.Debug("subscription client finds slow locked ranges",
		zap.Uint64("subscriptionID", uint64(r.subID)),
		zap.Any("ranges", res))
}

type errCache struct {
	sync.Mutex
	cache  []regionErrorInfo
	errCh  chan regionErrorInfo
	notify chan struct{}
}

func newErrCache() *errCache {
	return &errCache{
		cache:  make([]regionErrorInfo, 0, 1024),
		errCh:  make(chan regionErrorInfo, 1024),
		notify: make(chan struct{}, 1024),
	}
}

func (e *errCache) add(errInfo regionErrorInfo) {
	e.Lock()
	defer e.Unlock()
	e.cache = append(e.cache, errInfo)
	select {
	case e.notify <- struct{}{}:
	default:
	}
}

func (e *errCache) dispatch(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	sendToErrCh := func() {
		e.Lock()
		if len(e.cache) == 0 {
			e.Unlock()
			return
		}
		errInfo := e.cache[0]
		e.cache = e.cache[1:]
		e.Unlock()
		e.errCh <- errInfo
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			sendToErrCh()
		case <-e.notify:
			sendToErrCh()
		}
	}
}
