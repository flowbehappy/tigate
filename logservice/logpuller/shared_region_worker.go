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
	"time"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// The magic number here is keep the same with some magic numbers in some
// other components in TiCDC, including worker pool task chan size, mounter
// chan size etc.
// TODO: unified channel buffer mechanism
var regionWorkerInputChanSize = 32

// NOTE:
//  1. all contents come from one same TiKV store stream;
//  2. eventItem and resolvedTs shouldn't appear simultaneously;
type statefulEvent struct {
	eventItem       eventItem
	resolvedTsBatch resolvedTsBatch
	stream          *requestedStream
	start           time.Time
}

type eventItem struct {
	// All items come from one same region.
	item  *cdcpb.Event
	state *regionFeedState
}

// NOTE: all regions must come from the same subscribedTable, and regions will never be empty.
type resolvedTsBatch struct {
	ts      uint64
	regions []*regionFeedState
}

func newEventItem(item *cdcpb.Event, state *regionFeedState, stream *requestedStream) statefulEvent {
	return statefulEvent{
		eventItem: eventItem{item, state},
		stream:    stream,
		start:     time.Now(),
	}
}

func newResolvedTsBatch(ts uint64, stream *requestedStream) statefulEvent {
	return statefulEvent{
		resolvedTsBatch: resolvedTsBatch{ts: ts},
		stream:          stream,
		start:           time.Now(),
	}
}

type sharedRegionWorker struct {
	client  *SharedClient
	inputCh chan statefulEvent
}

func newSharedRegionWorker(c *SharedClient) *sharedRegionWorker {
	return &sharedRegionWorker{
		client:  c,
		inputCh: make(chan statefulEvent, regionWorkerInputChanSize),
	}
}

func (w *sharedRegionWorker) sendEvent(ctx context.Context, event statefulEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.inputCh <- event:
		return nil
	}
}

func (w *sharedRegionWorker) run(ctx context.Context) error {
	for {
		var event statefulEvent
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event = <-w.inputCh:
		}

		w.processEvent(ctx, event)
	}
}

func (w *sharedRegionWorker) handleSingleRegionError(state *regionFeedState, stream *requestedStream) {
	stepsToRemoved := state.markRemoved()
	err := state.takeError()
	if err != nil {
		w.client.logRegionDetails("region worker get a region error",
			zap.Uint64("streamID", stream.streamID),
			zap.Any("subscriptionID", state.getRegionID()),
			zap.Uint64("regionID", state.region.verID.GetID()),
			zap.Bool("reschedule", stepsToRemoved),
			zap.Error(err))
	}
	if stepsToRemoved {
		stream.takeState(SubscriptionID(state.requestID), state.getRegionID())
		w.client.onRegionFail(newRegionErrorInfo(state.getRegionInfo(), err))
	}
}

func (w *sharedRegionWorker) processEvent(ctx context.Context, event statefulEvent) {
	if event.eventItem.state != nil {
		state := event.eventItem.state
		if state.isStale() {
			w.handleSingleRegionError(state, event.stream)
			return
		}
		switch x := event.eventItem.item.Event.(type) {
		case *cdcpb.Event_Entries_:
			if err := w.handleEventEntry(ctx, x, state); err != nil {
				state.markStopped(err)
				w.handleSingleRegionError(state, event.stream)
				return
			}
		case *cdcpb.Event_ResolvedTs:
			log.Info("region worker get a resolvedTs",
				zap.Uint64("regionID", state.getRegionID()),
				zap.Uint64("resolvedTs", x.ResolvedTs))
			w.handleResolvedTs(ctx, resolvedTsBatch{
				ts:      x.ResolvedTs,
				regions: []*regionFeedState{state},
			})
		case *cdcpb.Event_Error:
			state.markStopped(&eventError{err: x.Error})
			w.handleSingleRegionError(state, event.stream)
			return
		case *cdcpb.Event_Admin_:
		}
	} else if len(event.resolvedTsBatch.regions) > 0 {
		w.handleResolvedTs(ctx, event.resolvedTsBatch)
	}
}

// NOTE: context.Canceled won't be treated as an error.
func (w *sharedRegionWorker) handleEventEntry(ctx context.Context, x *cdcpb.Event_Entries_, state *regionFeedState) error {
	startTs := state.region.subscribedTable.startTs
	emit := func(assembled common.RegionFeedEvent) bool {
		e := newMultiplexingEvent(assembled, state.region.subscribedTable)
		select {
		case state.region.subscribedTable.eventCh <- e:
			return true
		case <-ctx.Done():
			return false
		}
	}
	tableID := state.region.subscribedTable.span.TableID
	log.Debug("region worker get an Event",
		zap.Any("subscriptionID", state.region.subscribedTable.subscriptionID),
		zap.Uint64("tableID", tableID),
		zap.Int("rows", len(x.Entries.GetEntries())))
	return handleEventEntry(x, startTs, state, emit, common.TableID(tableID), w.client.logRegionDetails)
}

func handleEventEntry(
	x *cdcpb.Event_Entries_,
	startTs uint64,
	state *regionFeedState,
	emit func(assembled common.RegionFeedEvent) bool,
	tableID common.TableID,
	logRegionDetails func(msg string, fields ...zap.Field),
) error {
	regionID, _, _ := state.getRegionMeta()
	for _, entry := range x.Entries.GetEntries() {
		switch entry.Type {
		case cdcpb.Event_INITIALIZED:
			state.setInitialized()
			logRegionDetails("region is initialized",
				zap.Any("tableID", tableID),
				zap.Uint64("regionID", regionID),
				zap.Uint64("requestID", state.requestID),
				zap.Stringer("span", &state.region.span))

			for _, cachedEvent := range state.matcher.matchCachedRow(true) {
				revent, err := assembleRowEvent(regionID, cachedEvent)
				if err != nil {
					return errors.Trace(err)
				}
				if !emit(revent) {
					return nil
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

			revent, err := assembleRowEvent(regionID, entry)
			if err != nil {
				return errors.Trace(err)
			}
			if !emit(revent) {
				return nil
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

			revent, err := assembleRowEvent(regionID, entry)
			if err != nil {
				return errors.Trace(err)
			}
			if !emit(revent) {
				return nil
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

func assembleRowEvent(regionID uint64, entry *cdcpb.Event_Row) (common.RegionFeedEvent, error) {
	var opType common.OpType
	switch entry.GetOpType() {
	case cdcpb.Event_Row_DELETE:
		opType = common.OpTypeDelete
	case cdcpb.Event_Row_PUT:
		opType = common.OpTypePut
	default:
		return common.RegionFeedEvent{}, cerror.ErrUnknownKVEventType.GenWithStackByArgs(entry.GetOpType(), entry)
	}

	revent := common.RegionFeedEvent{
		RegionID: regionID,
		Val: &common.RawKVEntry{
			OpType:   opType,
			Key:      entry.Key,
			Value:    entry.GetValue(),
			StartTs:  entry.StartTs,
			CRTs:     entry.CommitTs,
			RegionID: regionID,
			OldValue: entry.GetOldValue(),
		},
	}

	return revent, nil
}

func (w *sharedRegionWorker) handleResolvedTs(ctx context.Context, batch resolvedTsBatch) {
	w.advanceTableSpan(ctx, batch)
}

func (w *sharedRegionWorker) advanceTableSpan(ctx context.Context, batch resolvedTsBatch) {
	for _, state := range batch.regions {
		if state.isStale() || !state.isInitialized() {
			continue
		}

		regionID := state.getRegionID()
		lastResolvedTs := state.getLastResolvedTs()
		if batch.ts < lastResolvedTs {
			log.Info("The resolvedTs is fallen back in kvclient",
				zap.Uint64("regionID", regionID),
				zap.Uint64("resolvedTs", batch.ts),
				zap.Uint64("lastResolvedTs", lastResolvedTs))
			continue
		}
		state.updateResolvedTs(batch.ts)
	}

	table := batch.regions[0].region.subscribedTable
	now := time.Now().UnixMilli()
	lastAdvance := table.lastAdvanceTime.Load()
	log.Info("try advanceTableSpan")
	if now-lastAdvance > int64(w.client.config.KVClientAdvanceIntervalInMs) && table.lastAdvanceTime.CompareAndSwap(lastAdvance, now) {
		ts := table.rangeLock.ResolvedTs()
		if ts > table.startTs {
			log.Info("do advanceTableSpan",
				zap.Uint64("ts", ts),
				zap.Any("subscriptionID", table.subscriptionID))
			revent := common.RegionFeedEvent{
				Val: &common.RawKVEntry{
					OpType: common.OpTypeResolved,
					CRTs:   ts,
				},
			}
			e := newMultiplexingEvent(revent, table)
			select {
			case table.eventCh <- e:
			case <-ctx.Done():
			}
		}
	}
}
