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
	"encoding/hex"
	"strconv"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// NOTE:
//  1. all contents come from one same TiKV store stream;
//  2. eventItem and resolvedTs shouldn't appear simultaneously;
type statefulEvent struct {
	eventItem         eventItem
	resolvedTsBatches []resolvedTsBatch
	worker            *regionRequestWorker
}

type eventItem struct {
	// All items come from one same region.
	item  *cdcpb.Event
	state *regionFeedState
}

// NOTE: all regions must come from the same subscribedSpan, and regions will never be empty.
type resolvedTsBatch struct {
	ts      uint64
	regions []*regionFeedState
}

func newEventItem(item *cdcpb.Event, state *regionFeedState, worker *regionRequestWorker) statefulEvent {
	return statefulEvent{
		eventItem: eventItem{item, state},
		worker:    worker,
	}
}

type changeEventProcessor struct {
	client  *SubscriptionClient
	inputCh chan statefulEvent

	metricProcessDuration prometheus.Observer
	metricTotalDuration   prometheus.Observer
}

func newChangeEventProcessor(id uint, client *SubscriptionClient) *changeEventProcessor {
	clientID := client.id.String()
	workerID := strconv.Itoa(int(id))

	return &changeEventProcessor{
		client:                client,
		inputCh:               make(chan statefulEvent, 64), // 64 is an arbitrary number.
		metricProcessDuration: metrics.RegionWorkerProcessDuration.WithLabelValues(clientID, workerID),
		metricTotalDuration:   metrics.RegionWorkerTotalDuration.WithLabelValues(clientID, workerID),
	}
}

func (w *changeEventProcessor) sendEvent(ctx context.Context, event statefulEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.inputCh <- event:
		return nil
	}
}

func (w *changeEventProcessor) run(ctx context.Context) error {
	start := time.Now()
	for {
		var event statefulEvent
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event = <-w.inputCh:
		}
		w.processEvent(ctx, event)
		w.metricTotalDuration.Observe(time.Since(start).Seconds())
		start = time.Now()
	}
}

func (w *changeEventProcessor) handleSingleRegionError(ctx context.Context, state *regionFeedState, worker *regionRequestWorker) {
	stepsToRemoved := state.markRemoved()
	err := state.takeError()
	if err != nil {
		log.Debug("region change event processor get a region error",
			zap.Int("subscriptionClientID", int(w.client.id)),
			zap.Uint64("workerID", worker.workerID),
			zap.Uint64("subscriptionID", uint64(state.region.subscribedSpan.subID)),
			zap.Uint64("regionID", state.region.verID.GetID()),
			zap.Bool("reschedule", stepsToRemoved),
			zap.Error(err))
	}
	if stepsToRemoved {
		worker.takeRegionState(SubscriptionID(state.requestID), state.getRegionID())
		w.client.onRegionFail(newRegionErrorInfo(state.getRegionInfo(), err))
	}
}

func (w *changeEventProcessor) processEvent(ctx context.Context, event statefulEvent) {
	start := time.Now()
	defer func() {
		w.metricProcessDuration.Observe(time.Since(start).Seconds())
	}()
	if event.eventItem.state != nil {
		state := event.eventItem.state
		if state.isStale() {
			w.handleSingleRegionError(ctx, state, event.worker)
			return
		}
		switch x := event.eventItem.item.Event.(type) {
		case *cdcpb.Event_Entries_:
			if err := w.handleEventEntry(ctx, x, state); err != nil {
				state.markStopped(err)
				w.handleSingleRegionError(ctx, state, event.worker)
				return
			}
		case *cdcpb.Event_ResolvedTs:
			w.handleResolvedTs(ctx, resolvedTsBatch{
				ts:      x.ResolvedTs,
				regions: []*regionFeedState{state},
			})
		case *cdcpb.Event_Error:
			state.markStopped(&eventError{err: x.Error})
			w.handleSingleRegionError(ctx, state, event.worker)
			return
		case *cdcpb.Event_Admin_:
		}
	} else if len(event.resolvedTsBatches) > 0 {
		for _, batch := range event.resolvedTsBatches {
			w.handleResolvedTs(ctx, batch)
		}
	}
}

// NOTE: context.Canceled won't be treated as an error.
func (w *changeEventProcessor) handleEventEntry(ctx context.Context, x *cdcpb.Event_Entries_, state *regionFeedState) error {
	startTs := state.region.subscribedSpan.startTs
	emit := func(val *common.RawKVEntry) error {
		// TODO: add a metric to indicate whether the event is sent successfully.
		e := newLogEvent(val, state.region.subscribedSpan)
		w.client.metrics.kvCounter.Add(1)
		return w.client.consume(ctx, e)
	}
	tableID := state.region.subscribedSpan.span.TableID
	log.Debug("region change event processor get an Event",
		zap.Int("subscriptionClientID", int(w.client.id)),
		zap.Uint64("subscriptionID", uint64(state.region.subscribedSpan.subID)),
		zap.Int64("tableID", tableID),
		zap.Int("rows", len(x.Entries.GetEntries())))
	return w.doHandle(x, startTs, state, emit, tableID)
}

func (w *changeEventProcessor) doHandle(
	x *cdcpb.Event_Entries_,
	startTs uint64,
	state *regionFeedState,
	emit func(val *common.RawKVEntry) error,
	tableID common.TableID,
) error {
	regionID, _, _ := state.getRegionMeta()
	for _, entry := range x.Entries.GetEntries() {
		switch entry.Type {
		case cdcpb.Event_INITIALIZED:
			state.setInitialized()
			log.Debug("region is initialized",
				zap.Any("tableID", tableID),
				zap.Uint64("regionID", regionID),
				zap.Uint64("requestID", state.requestID),
				zap.Stringer("span", &state.region.span))

			for _, cachedEvent := range state.matcher.matchCachedRow(true) {
				revent, err := w.assembleRowEvent(regionID, cachedEvent)
				if err != nil {
					return errors.Trace(err)
				}
				if err := emit(revent); err != nil {
					return err
				}
			}
			state.matcher.matchCachedRollbackRow(true)
		case cdcpb.Event_COMMITTED:
			resolvedTs := state.getLastResolvedTs()
			if entry.CommitTs <= resolvedTs {
				log.Panic("The CommitTs must be greater than the resolvedTs",
					zap.String("EventType", "COMMITTED"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", resolvedTs),
					zap.Uint64("regionID", regionID))
				return errUnreachable
			}
			revent, err := w.assembleRowEvent(regionID, entry)
			if err != nil {
				return errors.Trace(err)
			}
			if err := emit(revent); err != nil {
				return err
			}
		case cdcpb.Event_PREWRITE:
			state.matcher.putPrewriteRow(entry)
		case cdcpb.Event_COMMIT:
			// NOTE: matchRow should always be called even if the event is stale.
			if !state.matcher.matchRow(entry, state.isInitialized()) {
				if !state.isInitialized() {
					state.matcher.cacheCommitRow(entry)
					continue
				}
				return cerror.ErrPrewriteNotMatch.GenWithStackByArgs(
					hex.EncodeToString(entry.GetKey()),
					entry.GetStartTs(), entry.GetCommitTs(),
					entry.GetType(), entry.GetOpType())
			}

			// TiKV can send events with StartTs/CommitTs less than startTs.
			isStaleEvent := entry.CommitTs <= startTs
			if isStaleEvent {
				continue
			}

			// NOTE: state.getLastResolvedTs() will never less than startTs.
			resolvedTs := state.getLastResolvedTs()
			if entry.CommitTs <= resolvedTs {
				log.Panic("The CommitTs must be greater than the resolvedTs",
					zap.String("EventType", "COMMIT"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", resolvedTs),
					zap.Uint64("regionID", regionID))
				return errUnreachable
			}

			revent, err := w.assembleRowEvent(regionID, entry)
			if err != nil {
				return errors.Trace(err)
			}
			if err := emit(revent); err != nil {
				return err
			}
		case cdcpb.Event_ROLLBACK:
			if !state.isInitialized() {
				state.matcher.cacheRollbackRow(entry)
				continue
			}
			state.matcher.rollbackRow(entry)
		}
	}
	return nil
}

func (w *changeEventProcessor) assembleRowEvent(regionID uint64, entry *cdcpb.Event_Row) (*common.RawKVEntry, error) {
	var opType common.OpType
	switch entry.GetOpType() {
	case cdcpb.Event_Row_DELETE:
		opType = common.OpTypeDelete
	case cdcpb.Event_Row_PUT:
		opType = common.OpTypePut
	default:
		return &common.RawKVEntry{}, cerror.ErrUnknownKVEventType.GenWithStackByArgs(entry.GetOpType(), entry)
	}
	return &common.RawKVEntry{
		OpType:   opType,
		Key:      entry.Key,
		Value:    entry.GetValue(),
		StartTs:  entry.StartTs,
		CRTs:     entry.CommitTs,
		RegionID: regionID,
		OldValue: entry.GetOldValue(),
	}, nil
}

func (w *changeEventProcessor) handleResolvedTs(ctx context.Context, batch resolvedTsBatch) {
	w.advanceTableSpan(ctx, batch)
}

func (w *changeEventProcessor) advanceTableSpan(ctx context.Context, batch resolvedTsBatch) {
	if len(batch.regions) == 0 {
		return
	}

	for _, state := range batch.regions {
		if state.isStale() || !state.isInitialized() {
			continue
		}
		state.matcher.tryCleanUnmatchedValue()
		regionID := state.getRegionID()
		lastResolvedTs := state.getLastResolvedTs()
		if batch.ts < lastResolvedTs {
			log.Info("The resolvedTs is fallen back in kvclient",
				zap.Int("subscriptionClientID", int(w.client.id)),
				zap.Uint64("subscriptionID", uint64(state.region.subscribedSpan.subID)),
				zap.Uint64("regionID", regionID),
				zap.Uint64("resolvedTs", batch.ts),
				zap.Uint64("lastResolvedTs", lastResolvedTs))
			continue
		}

		state.updateResolvedTs(batch.ts)
	}

	span := batch.regions[0].region.subscribedSpan
	now := time.Now().UnixMilli()
	lastAdvance := span.lastAdvanceTime.Load()
	if now-lastAdvance > span.advanceInterval && span.lastAdvanceTime.CompareAndSwap(lastAdvance, now) {
		ts := span.rangeLock.ResolvedTs()
		if ts > span.startTs {
			e := newLogEvent(&common.RawKVEntry{
				OpType: common.OpTypeResolved,
				CRTs:   ts,
			}, span)
			if err := w.client.consume(ctx, e); err != nil {
				return
			}
		}
	}
}
