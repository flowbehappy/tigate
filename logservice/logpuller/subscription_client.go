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

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/logservice/logpuller/regionlock"
	"github.com/flowbehappy/tigate/logservice/txnutil"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/util"
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

// To generate an ID for a new subscription.
var subscriptionIDGen atomic.Uint64

// subscriptionID is a unique identifier for a subscription.
// It is used as `RequestId` in region requests to remote store.
type subscriptionID uint64

// regionFeedEvent from the kv layer.
type regionFeedEvent struct {
	// TODO: every resolve ts event may allocate a common.RawKVEntry, is it memory consuming?
	Val *common.RawKVEntry

	// Additional debug info, not used
	RegionID uint64
}

// LogEvent wrap a region event with subscriptionID to indicate which subscription it belongs to.
type LogEvent struct {
	regionFeedEvent
	subscriptionID
}

func newLogEvent(e regionFeedEvent, span *subscribedSpan) LogEvent {
	return LogEvent{
		regionFeedEvent: e,
		subscriptionID:  span.subID,
	}
}

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
	subID   subscriptionID
	startTs tablepb.Ts

	// The target span
	span heartbeatpb.TableSpan
	// The range lock of the span,
	// it is used to prevent duplicate requests to the same region range,
	// and it also used to calculate this table's resolvedTs.
	rangeLock *regionlock.RangeLock

	// To handle span removing.
	stopped atomic.Bool

	// To handle stale lock resolvings.
	tryResolveLock     func(regionID uint64, state *regionlock.LockedRangeState)
	staleLocksTargetTs atomic.Uint64

	lastAdvanceTime atomic.Int64
}

type SubscriptionClientConfig struct {
	// The number of region request workers to send region task for every tikv store
	RegionRequestWorkerPerStore uint
	// The number of region change event processor to process region events
	// TODO: add a metric for busy ratio?
	ChangeEventProcessorNum uint
	// The time interval to advance resolvedTs for a region
	AdvanceResolvedTsIntervalInMs uint
	// The limit of concurrent incremental scan regions for every tikv store
	RegionIncrementalScanLimitPerStore uint
}

// SubscriptionClient is used to subscribe events of table ranges from TiKV.
// All exported Methods are thread-safe.
type SubscriptionClient struct {
	config     *SubscriptionClientConfig
	clusterID  uint64
	filterLoop bool

	pd           pd.Client
	regionCache  *tikv.RegionCache
	pdClock      pdutil.Clock
	lockResolver txnutil.LockResolver

	// the credential to connect tikv
	credential *security.Credential

	totalSpans struct {
		sync.RWMutex
		spanMap map[subscriptionID]*subscribedSpan
	}

	changeEventProcessors []*changeEventProcessor

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
	errCh chan regionErrorInfo

	consume func(ctx context.Context, e LogEvent) error
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
	s := &SubscriptionClient{
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
		errCh:             make(chan regionErrorInfo, 1024),
	}
	s.totalSpans.spanMap = make(map[subscriptionID]*subscribedSpan)

	return s
}

// AllocsubscriptionID gets an ID can be used in `Subscribe`.
func (s *SubscriptionClient) AllocSubscriptionID() subscriptionID {
	return subscriptionID(subscriptionIDGen.Add(1))
}

// Subscribe the given table span.
// NOTE: `span.TableID` must be set correctly.
// It new a subscribedSpan and store it in `s.totalSpans`,
// and send a rangeTask to `s.rangeTaskCh`.
// The rangeTask will be handled in `handleRangeTasks` goroutine.
func (s *SubscriptionClient) Subscribe(subID subscriptionID, span heartbeatpb.TableSpan, startTs uint64) {
	if span.TableID == 0 {
		log.Panic("subscription client subscribe with zero TableID")
	}

	rt := s.newSubscribedSpan(subID, span, startTs)
	s.totalSpans.Lock()
	s.totalSpans.spanMap[subID] = rt
	s.totalSpans.Unlock()

	s.rangeTaskCh <- rangeTask{span: span, subscribedSpan: rt}
	log.Info("subscribes span success",
		zap.Any("subscriptionID", rt.subID),
		zap.String("span", rt.span.String()))
}

// Unsubscribe the given table span. All covered regions will be deregistered asynchronously.
// NOTE: `span.TableID` must be set correctly.
func (s *SubscriptionClient) Unsubscribe(subID subscriptionID) {
	// NOTE: `subID` is cleared from `s.totalSpans` in `onTableDrained`.
	s.totalSpans.Lock()
	rt := s.totalSpans.spanMap[subID]
	s.totalSpans.Unlock()
	if rt != nil {
		s.setTableStopped(rt)
	}

	log.Info("unsubscribe span success",
		zap.Any("subscriptionID", rt.subID),
		zap.Bool("exists", rt != nil))
}

// ResolveLock is a function. If outsider subscribers find a span resolved timestamp is
// advanced slowly or stopped, they can try to resolve locks in the given span.
func (s *SubscriptionClient) ResolveLock(subID subscriptionID, targetTs uint64) {
	s.totalSpans.Lock()
	rt := s.totalSpans.spanMap[subID]
	s.totalSpans.Unlock()
	if rt != nil {
		rt.resolveStaleLocks(targetTs)
	}
}

// RegionCount returns subscribed region count for the span.
func (s *SubscriptionClient) RegionCount(subID subscriptionID) uint64 {
	s.totalSpans.RLock()
	defer s.totalSpans.RUnlock()
	if rt := s.totalSpans.spanMap[subID]; rt != nil {
		return uint64(rt.rangeLock.Len())
	}
	return 0
}

func (s *SubscriptionClient) Run(ctx context.Context, consume func(ctx context.Context, e LogEvent) error) error {
	s.consume = consume
	if s.pd == nil {
		log.Warn("subsription client should be in test mode, skip run")
		return nil
	}
	s.clusterID = s.pd.GetClusterID(ctx)

	g, ctx := errgroup.WithContext(ctx)
	s.changeEventProcessors = make([]*changeEventProcessor, 0, s.config.ChangeEventProcessorNum)
	for i := uint(0); i < s.config.ChangeEventProcessorNum; i++ {
		processor := newChangeEventProcessor(s)
		g.Go(func() error { return processor.run(ctx) })
		s.changeEventProcessors = append(s.changeEventProcessors, processor)
	}

	g.Go(func() error { return s.handleRangeTasks(ctx) })
	g.Go(func() error { return s.handleRegions(ctx, g) })
	g.Go(func() error { return s.handleErrors(ctx) })
	g.Go(func() error { return s.handleResolveLockTasks(ctx) })
	g.Go(func() error { return s.logSlowRegions(ctx) })

	log.Info("subscription client starts")
	defer log.Info("subscription client exits")
	return g.Wait()
}

// Close closes the client. Must be called after `Run` returns.
func (s *SubscriptionClient) Close(ctx context.Context) error {
	// FIXME: close and drain all channels
	return nil
}

func (s *SubscriptionClient) setTableStopped(rt *subscribedSpan) {
	log.Info("subscription client starts to stop table",
		zap.Any("subscriptionID", rt.subID))

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
		zap.Any("subscriptionID", rt.subID))

	s.totalSpans.Lock()
	defer s.totalSpans.Unlock()
	delete(s.totalSpans.spanMap, rt.subID)
}

// Note: don't block the caller, otherwise there may be deadlock
func (s *SubscriptionClient) onRegionFail(ctx context.Context, errInfo regionErrorInfo) {
	select {
	case <-ctx.Done():
	case s.errCh <- errInfo:
	default:
		log.Warn("error channel is full, start a goroutine to send the error")
		go func() {
			select {
			case <-ctx.Done():
			case s.errCh <- errInfo:
			}
		}()
	}
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
			if region.isStoped() {
				for _, rs := range stores {
					for _, worker := range rs.requestWorkers {
						worker.requestsCh <- region
					}
				}
				continue
			}

			region, ok := s.attachRPCContextForRegion(ctx, region)
			// If attachRPCContextForRegion fails, the region will be re-scheduled.
			if !ok {
				continue
			}

			store := getStore(region.rpcCtx.Peer.StoreId, region.rpcCtx.Addr)
			worker := store.getRequestWorker()
			worker.requestsCh <- region

			log.Debug("subscription client will request a region",
				zap.Uint64("workID", worker.workerID),
				zap.Any("subscriptionID", region.subscribedSpan.subID),
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
			zap.Any("subscriptionID", region.subscribedSpan.subID),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.Error(err))
	}
	s.onRegionFail(ctx, newRegionErrorInfo(region, &rpcCtxUnavailableErr{verID: region.verID}))
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
			zap.Any("subscriptionID", subscribedSpan.subID),
			zap.Any("span", nextSpan))

		backoff := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		regions, err := s.regionCache.BatchLoadRegionsWithKeyRange(backoff, nextSpan.StartKey, nextSpan.EndKey, limit)
		if err != nil {
			log.Warn("subscription client load regions failed",
				zap.Any("subscriptionID", subscribedSpan.subID),
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
				zap.Any("subscriptionID", subscribedSpan.subID),
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
					zap.Any("subscriptionID", subscribedSpan.subID))
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
	case s.rangeTaskCh <- rangeTask{span: span, subscribedSpan: subscribedSpan}:
	case <-ctx.Done():
	}
}

func (s *SubscriptionClient) handleErrors(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case errInfo := <-s.errCh:
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
			zap.Any("subscriptionID", errInfo.subscribedSpan.subID),
			zap.Stringer("error", innerErr))

		if notLeader := innerErr.GetNotLeader(); notLeader != nil {
			s.regionCache.UpdateLeader(errInfo.verID, notLeader.GetLeader(), errInfo.rpcCtx.AccessIdx)
			s.scheduleRegionRequest(ctx, errInfo.regionInfo)
			return nil
		}
		if innerErr.GetEpochNotMatch() != nil {
			s.scheduleRangeRequest(ctx, errInfo.span, errInfo.subscribedSpan)
			return nil
		}
		if innerErr.GetRegionNotFound() != nil {
			s.scheduleRangeRequest(ctx, errInfo.span, errInfo.subscribedSpan)
			return nil
		}
		if innerErr.GetServerIsBusy() != nil {
			s.scheduleRegionRequest(ctx, errInfo.regionInfo)
			return nil
		}
		if duplicated := innerErr.GetDuplicateRequest(); duplicated != nil {
			// TODO(qupeng): It's better to add a new machanism to deregister one region.
			return errors.New("duplicate request")
		}
		if compatibility := innerErr.GetCompatibility(); compatibility != nil {
			return cerror.ErrVersionIncompatible.GenWithStackByArgs(compatibility)
		}
		if mismatch := innerErr.GetClusterIdMismatch(); mismatch != nil {
			return cerror.ErrClusterIDMismatch.GenWithStackByArgs(mismatch.Current, mismatch.Request)
		}

		log.Warn("empty or unknown cdc error",
			zap.Any("subscriptionID", errInfo.subscribedSpan.subID),
			zap.Stringer("error", innerErr))
		s.scheduleRegionRequest(ctx, errInfo.regionInfo)
		return nil
	case *rpcCtxUnavailableErr:
		s.scheduleRangeRequest(ctx, errInfo.span, errInfo.subscribedSpan)
		return nil
	case *sendRequestToStoreErr:
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		s.regionCache.OnSendFail(bo, errInfo.rpcCtx, regionScheduleReload, err)
		s.scheduleRegionRequest(ctx, errInfo.regionInfo)
		return nil
	default:
		// TODO(qupeng): for some errors it's better to just deregister the region from TiKVs.
		log.Warn("subscription client meets an internal error, fail the changefeed",
			zap.Any("subscriptionID", errInfo.subscribedSpan.subID),
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
						zap.Any("subscriptionID", subscriptionID),
						zap.Any("slowRegion", attr.SlowestRegion))
				}
			} else if currTime.Sub(attr.SlowestRegion.Created) > 10*time.Minute {
				slowInitializeRegion += 1
				log.Info("subscription client initializes a region too slow",
					zap.Any("subscriptionID", subscriptionID),
					zap.Any("slowRegion", attr.SlowestRegion))
			} else if currTime.Sub(ckptTime) > 10*time.Minute {
				log.Info("subscription client finds a uninitialized slow region",
					zap.Any("subscriptionID", subscriptionID),
					zap.Any("slowRegion", attr.SlowestRegion))
			}
			if len(attr.UnLockedRanges) > 0 {
				log.Info("subscription client holes exist",
					zap.Any("subscriptionID", subscriptionID),
					zap.Any("holes", attr.UnLockedRanges))
			}
		}
		s.totalSpans.RUnlock()
	}
}

func (s *SubscriptionClient) newSubscribedSpan(
	subID subscriptionID,
	span heartbeatpb.TableSpan,
	startTs uint64,
) *subscribedSpan {
	rangeLock := regionlock.NewRangeLock(uint64(subID), span.StartKey, span.EndKey, startTs)

	rt := &subscribedSpan{
		subID:     subID,
		span:      span,
		startTs:   startTs,
		rangeLock: rangeLock,
	}

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

func (r *subscribedSpan) resolveStaleLocks(targetTs uint64) {
	util.MustCompareAndMonotonicIncrease(&r.staleLocksTargetTs, targetTs)
	res := r.rangeLock.IterAll(r.tryResolveLock)
	log.Debug("subscription client finds slow locked ranges",
		zap.Any("subscriptionID", r.subID),
		zap.Any("ranges", res))
}
