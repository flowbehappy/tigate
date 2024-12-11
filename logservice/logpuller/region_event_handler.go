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

package logpuller

import (
	"encoding/hex"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/utils/dynstream"
	"go.uber.org/zap"
)

const (
	DataGroupResolvedTs = 1
	DataGroupEntries    = 2
	DataGroupError      = 3
)

type regionEvent struct {
	state  *regionFeedState
	worker *regionRequestWorker // TODO: remove the field

	// only one of the following fields will be set
	entries    *cdcpb.Event_Entries_
	resolvedTs uint64
	err        *cdcpb.Event_Error
}

type pathHasher struct {
}

func (h pathHasher) HashPath(subID SubscriptionID) uint64 {
	return uint64(subID)
}

type regionEventHandler struct {
	subClient *SubscriptionClient
}

func (h *regionEventHandler) Path(event regionEvent) SubscriptionID {
	return SubscriptionID(event.state.requestID)
}

func (h *regionEventHandler) Handle(span *subscribedSpan, events ...regionEvent) bool {
	if len(span.kvEventsCache) != 0 {
		log.Panic("kvEventsCache is not empty",
			zap.Int("kvEventsCacheLen", len(span.kvEventsCache)),
			zap.Uint64("subID", uint64(span.subID)))
	}

	for _, event := range events {
		if event.state.isStale() {
			h.handleRegionError(event.state, event.worker)
			continue
		}
		if event.entries != nil {
			handleEventEntries(span, event.state, event.entries)
		} else if event.resolvedTs != 0 {
			handleResolvedTs(span, event.state, event.resolvedTs)
		} else if event.err != nil {
			event.state.markStopped(&eventError{err: event.err.Error})
			h.handleRegionError(event.state, event.worker)
		} else {
			log.Panic("should not reach", zap.Any("event", event), zap.Any("events", events))
		}
	}
	if len(span.kvEventsCache) > 0 {
		await := span.consumeKVEvents(span.kvEventsCache, func() {
			span.clearKVEventsCache()
			h.subClient.wakeSubscription(span.subID)
		})
		// if not await, the wake callback will not be called, we need clear the cache manually.
		if !await {
			span.clearKVEventsCache()
		}
		return await
	}
	return false
}

func (h *regionEventHandler) GetSize(event regionEvent) int { return 0 }
func (h *regionEventHandler) GetArea(path SubscriptionID, dest *subscribedSpan) int {
	return 0
}
func (h *regionEventHandler) GetTimestamp(event regionEvent) dynstream.Timestamp {
	if event.entries != nil {
		entries := event.entries.Entries.GetEntries()
		switch entries[0].Type {
		case cdcpb.Event_INITIALIZED:
			return dynstream.Timestamp(event.state.region.resolvedTs())
		case cdcpb.Event_COMMITTED,
			cdcpb.Event_PREWRITE,
			cdcpb.Event_COMMIT,
			cdcpb.Event_ROLLBACK:
			return dynstream.Timestamp(entries[0].CommitTs)
		}
	} else {
		return dynstream.Timestamp(event.resolvedTs)
	}
	log.Panic("unknown event type", zap.Any("event", event))
	return 0
}
func (h *regionEventHandler) IsPaused(event regionEvent) bool { return false }

func (h *regionEventHandler) GetType(event regionEvent) dynstream.EventType {
	if event.entries != nil {
		return dynstream.EventType{DataGroup: DataGroupEntries, Property: dynstream.BatchableData}
	} else if event.resolvedTs != 0 {
		return dynstream.EventType{DataGroup: DataGroupResolvedTs, Property: dynstream.PeriodicSignal}
	} else if event.err != nil || event.state.isStale() {
		return dynstream.EventType{DataGroup: DataGroupError, Property: dynstream.BatchableData}
	} else {
		log.Panic("should not reach", zap.Any("event", event))
	}
	return dynstream.DefaultEventType
}

func (h *regionEventHandler) OnDrop(event regionEvent) {}

func (h *regionEventHandler) handleRegionError(state *regionFeedState, worker *regionRequestWorker) {
	stepsToRemoved := state.markRemoved()
	err := state.takeError()
	if err != nil {
		log.Debug("region event handler get a region error",
			zap.Uint64("workerID", worker.workerID),
			zap.Uint64("subscriptionID", uint64(state.region.subscribedSpan.subID)),
			zap.Uint64("regionID", state.region.verID.GetID()),
			zap.Bool("reschedule", stepsToRemoved),
			zap.Error(err))
	}
	if stepsToRemoved {
		worker.takeRegionState(SubscriptionID(state.requestID), state.getRegionID())
		h.subClient.onRegionFail(newRegionErrorInfo(state.getRegionInfo(), err))
	}
}

// func handleEventEntries(span *subscribedSpan, state *regionFeedState, entries *cdcpb.Event_Entries_, kvEvents []common.RawKVEntry) []common.RawKVEntry {
func handleEventEntries(span *subscribedSpan, state *regionFeedState, entries *cdcpb.Event_Entries_) {
	regionID, _, _ := state.getRegionMeta()
	assembleRowEvent := func(regionID uint64, entry *cdcpb.Event_Row) common.RawKVEntry {
		var opType common.OpType
		switch entry.GetOpType() {
		case cdcpb.Event_Row_DELETE:
			opType = common.OpTypeDelete
		case cdcpb.Event_Row_PUT:
			opType = common.OpTypePut
		default:
			log.Panic("meet unknown op type", zap.Any("entry", entry))
		}
		return common.RawKVEntry{
			OpType:   opType,
			Key:      entry.Key,
			Value:    entry.GetValue(),
			StartTs:  entry.StartTs,
			CRTs:     entry.CommitTs,
			RegionID: regionID,
			OldValue: entry.GetOldValue(),
		}
	}

	for _, entry := range entries.Entries.GetEntries() {
		switch entry.Type {
		case cdcpb.Event_INITIALIZED:
			state.setInitialized()
			log.Debug("region is initialized",
				zap.Int64("tableID", span.span.TableID),
				zap.Uint64("regionID", regionID),
				zap.Uint64("requestID", state.requestID),
				zap.Stringer("span", &state.region.span))

			for _, cachedEvent := range state.matcher.matchCachedRow(true) {
				span.kvEventsCache = append(span.kvEventsCache, assembleRowEvent(regionID, cachedEvent))
			}
			state.matcher.matchCachedRollbackRow(true)
		case cdcpb.Event_COMMITTED:
			resolvedTs := state.getLastResolvedTs()
			if entry.CommitTs <= resolvedTs {
				log.Fatal("The CommitTs must be greater than the resolvedTs",
					zap.String("EventType", "COMMITTED"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", resolvedTs),
					zap.Uint64("regionID", regionID))
			}
			span.kvEventsCache = append(span.kvEventsCache, assembleRowEvent(regionID, entry))
		case cdcpb.Event_PREWRITE:
			state.matcher.putPrewriteRow(entry)
		case cdcpb.Event_COMMIT:
			// NOTE: matchRow should always be called even if the event is stale.
			if !state.matcher.matchRow(entry, state.isInitialized()) {
				if !state.isInitialized() {
					state.matcher.cacheCommitRow(entry)
					continue
				}
				log.Fatal("prewrite not match",
					zap.String("key", hex.EncodeToString(entry.GetKey())),
					zap.Uint64("startTs", entry.GetStartTs()),
					zap.Uint64("commitTs", entry.GetCommitTs()),
					zap.Any("type", entry.GetType()),
					zap.Uint64("regionID", state.getRegionID()),
					zap.Any("opType", entry.GetOpType()))
				return
			}

			// TiKV can send events with StartTs/CommitTs less than startTs.
			isStaleEvent := entry.CommitTs <= span.startTs
			if isStaleEvent {
				continue
			}

			// NOTE: state.getLastResolvedTs() will never less than startTs.
			resolvedTs := state.getLastResolvedTs()
			if entry.CommitTs <= resolvedTs {
				log.Fatal("The CommitTs must be greater than the resolvedTs",
					zap.String("EventType", "COMMIT"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", resolvedTs),
					zap.Uint64("regionID", regionID))
				return
			}
			// kvEvents = append(kvEvents, assembleRowEvent(regionID, entry))
			span.kvEventsCache = append(span.kvEventsCache, assembleRowEvent(regionID, entry))
		case cdcpb.Event_ROLLBACK:
			if !state.isInitialized() {
				state.matcher.cacheRollbackRow(entry)
				continue
			}
			state.matcher.rollbackRow(entry)
		}
	}
}

func handleResolvedTs(span *subscribedSpan, state *regionFeedState, resolvedTs uint64) {
	if state.isStale() || !state.isInitialized() {
		return
	}
	state.matcher.tryCleanUnmatchedValue()
	regionID := state.getRegionID()
	lastResolvedTs := state.getLastResolvedTs()
	if resolvedTs < lastResolvedTs {
		log.Info("The resolvedTs is fallen back in subscription client",
			zap.Uint64("subscriptionID", uint64(state.region.subscribedSpan.subID)),
			zap.Uint64("regionID", regionID),
			zap.Uint64("resolvedTs", resolvedTs),
			zap.Uint64("lastResolvedTs", lastResolvedTs))
		return
	}
	state.updateResolvedTs(resolvedTs)

	now := time.Now().UnixMilli()
	lastAdvance := span.lastAdvanceTime.Load()
	if now-lastAdvance > span.advanceInterval && span.lastAdvanceTime.CompareAndSwap(lastAdvance, now) {
		ts := span.rangeLock.ResolvedTs()
		if ts > span.startTs {
			span.advanceResolvedTs(ts)
		}
	}
}
