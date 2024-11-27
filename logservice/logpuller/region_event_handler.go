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
)

type regionEvent struct {
	state *regionFeedState
	// only one of the following two fields will be set
	entries    *cdcpb.Event_Entries_
	resolvedTs uint64
}

type regionEventPath struct {
	subID    uint64
	regionID uint64
}

type regionEventPathHasher struct {
}

func (h regionEventPathHasher) HashPath(path regionEventPath) uint64 {
	// Combine the two parts using XOR and a bit shift
	return path.regionID ^ (path.subID << 1)
}

type regionEventHandler struct {
}

func (h *regionEventHandler) Path(event regionEvent) regionEventPath {
	return regionEventPath{
		subID:    event.state.requestID,
		regionID: event.state.getRegionID(),
	}
}

func (h *regionEventHandler) Handle(worker *regionRequestWorker, events ...regionEvent) bool {
	for _, event := range events {
		if event.state.isStale() {
			worker.handleRegionError(event.state)
			continue
		}
		if event.entries != nil {
			// TODO: need block? and how to wake?
			handleEventEntries(event.state, worker, event.entries)
		} else if event.resolvedTs != 0 {
			handleResolvedTs(event.state, worker, event.resolvedTs)
		} else {
			log.Panic("should not reach", zap.Any("event", event), zap.Any("events", events))
		}
	}
	return false
}

func (h *regionEventHandler) GetSize(event regionEvent) int { return 0 }
func (h *regionEventHandler) GetArea(path regionEventPath, dest *regionRequestWorker) int {
	return 0
}
func (h *regionEventHandler) GetTimestamp(event regionEvent) dynstream.Timestamp {
	// TODO: it is hard to get timestamp of *cdcpb.Event_Entries_
	if event.entries != nil {
		entries := event.entries.Entries.GetEntries()
		switch entries[0].Type {
		case cdcpb.Event_INITIALIZED:
			// FIXME: is this ok?
			return 0
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
	} else {
		log.Panic("should not reach", zap.Any("event", event))
	}
	return dynstream.DefaultEventType
}

func (h *regionEventHandler) OnDrop(event regionEvent) {}

func handleEventEntries(state *regionFeedState, worker *regionRequestWorker, entries *cdcpb.Event_Entries_) {
	regionID, _, _ := state.getRegionMeta()
	span := state.region.subscribedSpan
	assembleRowEvent := func(regionID uint64, entry *cdcpb.Event_Row) *common.RawKVEntry {
		var opType common.OpType
		switch entry.GetOpType() {
		case cdcpb.Event_Row_DELETE:
			opType = common.OpTypeDelete
		case cdcpb.Event_Row_PUT:
			opType = common.OpTypePut
		default:
			log.Panic("meet unknown op type", zap.Any("entry", entry))
		}
		return &common.RawKVEntry{
			OpType:   opType,
			Key:      entry.Key,
			Value:    entry.GetValue(),
			StartTs:  entry.StartTs,
			CRTs:     entry.CommitTs,
			RegionID: regionID,
			OldValue: entry.GetOldValue(),
		}
	}
	emit := func(val *common.RawKVEntry) {
		e := newLogEvent(val, span)
		worker.client.consume(e)
	}

	for _, entry := range entries.Entries.GetEntries() {
		// log.Info("handleEventEntries",
		// 	zap.Uint64("startTs", entry.StartTs),
		// 	zap.Uint64("commitTs", entry.CommitTs),
		// 	zap.Any("type", entry.Type),
		// 	zap.Any("opType", entry.OpType))
		switch entry.Type {
		case cdcpb.Event_INITIALIZED:
			state.setInitialized()
			log.Debug("region is initialized",
				zap.Int64("tableID", span.span.TableID),
				zap.Uint64("regionID", regionID),
				zap.Uint64("requestID", state.requestID),
				zap.Stringer("span", &state.region.span))

			for _, cachedEvent := range state.matcher.matchCachedRow(true) {
				revent := assembleRowEvent(regionID, cachedEvent)
				emit(revent)
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
			}
			revent := assembleRowEvent(regionID, entry)
			emit(revent)
		case cdcpb.Event_PREWRITE:
			state.matcher.putPrewriteRow(entry)
		case cdcpb.Event_COMMIT:
			// NOTE: matchRow should always be called even if the event is stale.
			if !state.matcher.matchRow(entry, state.isInitialized()) {
				if !state.isInitialized() {
					state.matcher.cacheCommitRow(entry)
					continue
				}
				log.Panic("prewrite not match",
					zap.String("key", hex.EncodeToString(entry.GetKey())),
					zap.Uint64("startTs", entry.GetStartTs()),
					zap.Uint64("commitTs", entry.GetCommitTs()),
					zap.Any("type", entry.GetType()),
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
				log.Panic("The CommitTs must be greater than the resolvedTs",
					zap.String("EventType", "COMMIT"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", resolvedTs),
					zap.Uint64("regionID", regionID))
				return
			}
			revent := assembleRowEvent(regionID, entry)
			emit(revent)
		case cdcpb.Event_ROLLBACK:
			if !state.isInitialized() {
				state.matcher.cacheRollbackRow(entry)
				continue
			}
			state.matcher.rollbackRow(entry)
		}
	}
}

func handleResolvedTs(state *regionFeedState, worker *regionRequestWorker, resolvedTs uint64) {
	if state.isStale() || !state.isInitialized() {
		return
	}
	state.matcher.tryCleanUnmatchedValue()
	regionID := state.getRegionID()
	lastResolvedTs := state.getLastResolvedTs()
	if resolvedTs < lastResolvedTs {
		log.Info("The resolvedTs is fallen back in kvclient",
			zap.Int("subscriptionClientID", int(worker.client.id)),
			zap.Uint64("subscriptionID", uint64(state.region.subscribedSpan.subID)),
			zap.Uint64("regionID", regionID),
			zap.Uint64("resolvedTs", resolvedTs),
			zap.Uint64("lastResolvedTs", lastResolvedTs))
		return
	}
	state.updateResolvedTs(resolvedTs)

	span := state.region.subscribedSpan
	now := time.Now().UnixMilli()
	lastAdvance := span.lastAdvanceTime.Load()
	if now-lastAdvance > span.advanceInterval && span.lastAdvanceTime.CompareAndSwap(lastAdvance, now) {
		ts := span.rangeLock.ResolvedTs()
		if ts > span.startTs {
			e := newLogEvent(&common.RawKVEntry{
				OpType: common.OpTypeResolved,
				CRTs:   ts,
			}, span)
			worker.client.consume(e)
		}
	}
}
