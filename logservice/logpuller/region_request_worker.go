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
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/util/seahash"
	"github.com/pingcap/tiflow/pkg/version"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	grpcstatus "google.golang.org/grpc/status"
)

// To generate a workerID in `newRegionRequestWorker`.
var workerIDGen atomic.Uint64

type regionFeedStates map[uint64]*regionFeedState

// regionRequestWorker is responsible for sending region requests to a specific TiKV store.
type regionRequestWorker struct {
	workerID uint64

	client *SubscriptionClient

	store *requestedStore

	// we must always get a region to request before create a grpc stream.
	// only in this way we can avoid to try to connect to an offline store infinitely.
	preFetchForConnecting *regionInfo

	// used to receive region requests from outside.
	requestsCh chan regionInfo

	// all regions maintained by this worker.
	requestedRegions struct {
		sync.RWMutex

		subscriptions map[SubscriptionID]regionFeedStates
	}

	tsBatches struct {
		sync.Mutex
		events []statefulEvent
	}

	// used to indicate whether there are new pending ts events for every processores in dispatchResolvedTs
	boolCache []bool
}

func newRegionRequestWorker(
	ctx context.Context,
	client *SubscriptionClient,
	credential *security.Credential,
	g *errgroup.Group,
	store *requestedStore,
) *regionRequestWorker {
	worker := &regionRequestWorker{
		workerID:   workerIDGen.Add(1),
		client:     client,
		store:      store,
		requestsCh: make(chan regionInfo, 256), // 256 is an arbitrary number.

		boolCache: make([]bool, len(client.changeEventProcessors)),
	}
	worker.requestedRegions.subscriptions = make(map[SubscriptionID]regionFeedStates)
	worker.tsBatches.events = make([]statefulEvent, len(client.changeEventProcessors))
	for i := range worker.tsBatches.events {
		worker.tsBatches.events[i].worker = worker
	}

	waitForPreFetching := func() error {
		if worker.preFetchForConnecting != nil {
			log.Panic("preFetchForConnecting should be nil",
				zap.Int("subscriptionClientID", int(worker.client.id)),
				zap.Uint64("workerID", worker.workerID),
				zap.Uint64("storeID", store.storeID),
				zap.String("addr", store.storeAddr))
		}
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case region := <-worker.requestsCh:
				if !region.isStoped() {
					worker.preFetchForConnecting = new(regionInfo)
					*worker.preFetchForConnecting = region
					return nil
				}
			}
		}
	}

	g.Go(func() error {
		for {
			if err := waitForPreFetching(); err != nil {
				return err
			}
			if canceled := worker.run(ctx, credential); canceled {
				return nil
			}
			for _, m := range worker.clearRegionStates() {
				for _, state := range m {
					state.markStopped(&sendRequestToStoreErr{})
					sfEvent := newEventItem(nil, state, worker)
					slot := hashRegionID(state.region.verID.GetID(), len(client.changeEventProcessors))
					_ = client.changeEventProcessors[slot].sendEvent(ctx, sfEvent)
				}
			}
			// The store may fail forever, so we need try to re-schedule al pending regions.
			for _, region := range worker.clearPendingRegions() {
				if region.isStoped() {
					// It means it's a special task for stopping the table.
					continue
				}
				client.onRegionFail(newRegionErrorInfo(region, &sendRequestToStoreErr{}))
			}
			if err := util.Hang(ctx, time.Second); err != nil {
				return err
			}
		}
	})

	return worker
}

func (s *regionRequestWorker) run(ctx context.Context, credential *security.Credential) (canceled bool) {
	isCanceled := func() bool {
		select {
		case <-ctx.Done():
			return true
		default:
			return false
		}
	}

	// FIXME: check tikv store version

	log.Info("region request worker going to create grpc stream",
		zap.Int("subscriptionClientID", int(s.client.id)),
		zap.Uint64("workerID", s.workerID),
		zap.Uint64("storeID", s.store.storeID),
		zap.String("addr", s.store.storeAddr))

	defer func() {
		log.Info("region request worker exits",
			zap.Int("subscriptionClientID", int(s.client.id)),
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("storeID", s.store.storeID),
			zap.String("addr", s.store.storeAddr),
			zap.Bool("canceled", canceled))
	}()

	g, gctx := errgroup.WithContext(ctx)
	cc, err := Connect(gctx, credential, s.store.storeAddr)
	if err != nil {
		log.Warn("region request worker create grpc stream failed",
			zap.Int("subscriptionClientID", int(s.client.id)),
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("storeID", s.store.storeID),
			zap.String("addr", s.store.storeAddr),
			zap.Error(err))
		return isCanceled()
	}
	defer func() {
		_ = cc.Conn.Close()
	}()

	g.Go(func() error {
		return s.receiveAndDispatchChangeEventsToProcessor(gctx, cc)
	})
	g.Go(func() error { return s.processRegionSendTask(gctx, cc) })
	g.Go(func() error { return s.sendBatchedResolvedTs(gctx) })
	_ = g.Wait()
	return isCanceled()
}

// receiveAndDispatchChangeEventsToProcessor receives events from the grpc stream and dispatches them to the processor.
func (s *regionRequestWorker) receiveAndDispatchChangeEventsToProcessor(
	ctx context.Context,
	conn *ConnAndClient,
) error {
	for {
		changeEvent, err := conn.Client.Recv()
		if err != nil {
			log.Debug("region request worker receive from grpc stream failed",
				zap.Int("subscriptionClientID", int(s.client.id)),
				zap.Uint64("workerID", s.workerID),
				zap.Uint64("storeID", s.store.storeID),
				zap.String("addr", s.store.storeAddr),
				zap.String("code", grpcstatus.Code(err).String()),
				zap.Error(err))
			if StatusIsEOF(grpcstatus.Convert(err)) {
				return nil
			}
			return errors.Trace(err)
		}
		if len(changeEvent.Events) > 0 {
			if err := s.dispatchRegionChangeEvents(ctx, changeEvent.Events); err != nil {
				return err
			}
		}
		if changeEvent.ResolvedTs != nil {
			if err := s.dispatchResolvedTs(changeEvent.ResolvedTs); err != nil {
				return err
			}
		}
	}
}

// processRegionSendTask receives region requests from the channel and sends them to the remote store.
func (s *regionRequestWorker) processRegionSendTask(
	ctx context.Context,
	conn *ConnAndClient,
) error {
	doSend := func(req *cdcpb.ChangeDataRequest, subscriptionID SubscriptionID) error {
		if err := conn.Client.Send(req); err != nil {
			log.Warn("region request worker send request to grpc stream failed",
				zap.Int("subscriptionClientID", int(s.client.id)),
				zap.Uint64("workerID", s.workerID),
				zap.Uint64("subscriptionID", uint64(subscriptionID)),
				zap.Uint64("regionID", req.RegionId),
				zap.Uint64("storeID", s.store.storeID),
				zap.String("addr", s.store.storeAddr),
				zap.Error(err))
			return errors.Trace(err)
		}
		// TODO: add a metric?
		return nil
	}

	fetchMoreReq := func() (regionInfo, error) {
		for {
			var region regionInfo
			select {
			case <-ctx.Done():
				return region, ctx.Err()
			case region = <-s.requestsCh:
				return region, nil
			}
		}
	}

	region := *s.preFetchForConnecting
	s.preFetchForConnecting = nil
	for {
		// TODO: can region be nil?
		subID := region.subscribedSpan.subID
		log.Debug("region request worker gets a singleRegionInfo",
			zap.Int("subscriptionClientID", int(s.client.id)),
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("subscriptionID", uint64(subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.Uint64("storeID", s.store.storeID),
			zap.String("addr", s.store.storeAddr))

		// It means it's a special task for stopping the table.
		if region.isStoped() {
			req := &cdcpb.ChangeDataRequest{
				RequestId: uint64(subID),
				Request:   &cdcpb.ChangeDataRequest_Deregister_{},
			}
			if err := doSend(req, subID); err != nil {
				return err
			}
			for _, state := range s.takeRegionStates(subID) {
				state.markStopped(&sendRequestToStoreErr{})
				sfEvent := newEventItem(nil, state, s)
				slot := hashRegionID(state.region.verID.GetID(), len(s.client.changeEventProcessors))
				if err := s.client.changeEventProcessors[slot].sendEvent(ctx, sfEvent); err != nil {
					return errors.Trace(err)
				}
			}
		} else if region.subscribedSpan.stopped.Load() {
			// It can be skipped directly because there must be no pending states from
			// the stopped subscribedTable, or the special singleRegionInfo for stopping
			// the table will be handled later.
			s.client.onRegionFail(newRegionErrorInfo(region, &sendRequestToStoreErr{}))
		} else {
			state := newRegionFeedState(region, uint64(subID))
			state.start()
			s.addRegionState(subID, region.verID.GetID(), state)

			if err := doSend(s.createRegionRequest(region), subID); err != nil {
				return err
			}
		}

		var err error
		if region, err = fetchMoreReq(); err != nil {
			return err
		}
	}
}

func (s *regionRequestWorker) createRegionRequest(region regionInfo) *cdcpb.ChangeDataRequest {
	return &cdcpb.ChangeDataRequest{
		Header:       &cdcpb.Header{ClusterId: s.client.clusterID, TicdcVersion: version.ReleaseSemver()},
		RegionId:     region.verID.GetID(),
		RequestId:    uint64(region.subscribedSpan.subID),
		RegionEpoch:  region.rpcCtx.Meta.RegionEpoch,
		CheckpointTs: region.resolvedTs(),
		StartKey:     region.span.StartKey,
		EndKey:       region.span.EndKey,
		ExtraOp:      kvrpcpb.ExtraOp_ReadOldValue,
		FilterLoop:   s.client.filterLoop,
	}
}

func (s *regionRequestWorker) addRegionState(subscriptionID SubscriptionID, regionID uint64, state *regionFeedState) {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	states := s.requestedRegions.subscriptions[subscriptionID]
	if states == nil {
		states = make(regionFeedStates)
		s.requestedRegions.subscriptions[subscriptionID] = states
	}
	states[regionID] = state
}

func (s *regionRequestWorker) getRegionState(subscriptionID SubscriptionID, regionID uint64) *regionFeedState {
	s.requestedRegions.RLock()
	defer s.requestedRegions.RUnlock()
	if states, ok := s.requestedRegions.subscriptions[subscriptionID]; ok {
		return states[regionID]
	}
	return nil
}

func (s *regionRequestWorker) takeRegionState(subscriptionID SubscriptionID, regionID uint64) *regionFeedState {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	if states, ok := s.requestedRegions.subscriptions[subscriptionID]; ok {
		state := states[regionID]
		delete(states, regionID)
		if len(states) == 0 {
			delete(s.requestedRegions.subscriptions, subscriptionID)
		}
		return state
	}
	return nil
}

func (s *regionRequestWorker) takeRegionStates(subscriptionID SubscriptionID) regionFeedStates {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	states := s.requestedRegions.subscriptions[subscriptionID]
	delete(s.requestedRegions.subscriptions, subscriptionID)
	return states
}

func (s *regionRequestWorker) clearRegionStates() map[SubscriptionID]regionFeedStates {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	subscriptions := s.requestedRegions.subscriptions
	s.requestedRegions.subscriptions = make(map[SubscriptionID]regionFeedStates)
	return subscriptions
}

func (s *regionRequestWorker) clearPendingRegions() []regionInfo {
	regions := make([]regionInfo, 0, len(s.requestsCh))
	if s.preFetchForConnecting != nil {
		region := *s.preFetchForConnecting
		s.preFetchForConnecting = nil
		regions = append(regions, region)
	}
	// TODO: do we need to start with i := 0(i := len(regions)) if s.preFetchForConnecting is nil?
	for i := 1; i < cap(regions); i++ {
		regions = append(regions, <-s.requestsCh)
	}
	return regions
}

func (s *regionRequestWorker) dispatchRegionChangeEvents(ctx context.Context, events []*cdcpb.Event) error {
	for _, event := range events {
		regionID := event.RegionId
		subscriptionID := SubscriptionID(event.RequestId)

		state := s.getRegionState(subscriptionID, regionID)
		switch x := event.Event.(type) {
		case *cdcpb.Event_Error:
			log.Debug("region request worker receives a region error",
				zap.Int("subscriptionClientID", int(s.client.id)),
				zap.Uint64("workerID", s.workerID),
				zap.Uint64("subscriptionID", uint64(subscriptionID)),
				zap.Uint64("regionID", event.RegionId),
				zap.Bool("stateIsNil", state == nil),
				zap.Any("error", x.Error))
		}
		if state != nil {
			sfEvent := newEventItem(event, state, s)
			slot := hashRegionID(regionID, len(s.client.changeEventProcessors))
			if err := s.client.changeEventProcessors[slot].sendEvent(ctx, sfEvent); err != nil {
				return errors.Trace(err)
			}
		} else {
			log.Warn("region request worker receives a region event for an untracked region",
				zap.Int("subscriptionClientID", int(s.client.id)),
				zap.Uint64("workerID", s.workerID),
				zap.Uint64("subscriptionID", uint64(subscriptionID)),
				zap.Uint64("regionID", event.RegionId))
		}
	}
	return nil
}

func (s *regionRequestWorker) dispatchResolvedTs(resolvedTs *cdcpb.ResolvedTs) error {
	s.tsBatches.Lock()
	defer s.tsBatches.Unlock()
	subscriptionID := SubscriptionID(resolvedTs.RequestId)
	for i := range s.boolCache {
		s.boolCache[i] = false
	}
	s.client.metrics.batchResolvedSize.Observe(float64(len(resolvedTs.Regions)))
	for _, regionID := range resolvedTs.Regions {
		slot := hashRegionID(regionID, len(s.client.changeEventProcessors))
		if !s.boolCache[slot] {
			s.tsBatches.events[slot].resolvedTsBatches = append(s.tsBatches.events[slot].resolvedTsBatches, resolvedTsBatch{
				ts: resolvedTs.Ts,
			})
			s.boolCache[slot] = true
		}
		batch := &s.tsBatches.events[slot].resolvedTsBatches[len(s.tsBatches.events[slot].resolvedTsBatches)-1]
		if state := s.getRegionState(subscriptionID, regionID); state != nil {
			batch.regions = append(batch.regions, state)
		}
	}

	return nil
}

func (s *regionRequestWorker) sendBatchedResolvedTs(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.tsBatches.Lock()
			for i := range s.tsBatches.events {
				event := &s.tsBatches.events[i]
				if len(event.resolvedTsBatches) > 0 {
					if err := s.client.changeEventProcessors[i].sendEvent(ctx, *event); err != nil {
						// TODO: how to handle event.resolvedTsBatches when meet error?
						s.tsBatches.Unlock()
						return err
					}
					event.resolvedTsBatches = nil
				}
			}
			s.tsBatches.Unlock()
		}
	}
}

func hashRegionID(regionID uint64, slots int) int {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, regionID)
	return int(seahash.Sum64(b) % uint64(slots))
}
