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
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/utils/dynstream"
)

const (
	DataGroupResolvedTs = 1
	DataGroupDML        = 2
)

type eventsHandler struct {
	// Used to generate a unique batchSeq for events
	batchSeq atomic.Uint64
}

func (h *eventsHandler) Path(event kvEvent) logpuller.SubscriptionID {
	return event.subID
}

func (h *eventsHandler) Handle(subStat *subscriptionStat, events ...kvEvent) bool {
	if events[0].raw.IsResolved() {
		if len(events) != 1 {
			log.Panic("should not happen")
		}
		subStat.resolvedTs.Store(events[0].raw.CRTs)
		subStat.dispatchers.RLock()
		defer subStat.dispatchers.RUnlock()
		for _, notifier := range subStat.dispatchers.notifiers {
			notifier(events[0].raw.CRTs)
		}
		return false
	}
	subStat.maxEventCommitTs.Store(events[len(events)-1].raw.CRTs)

	batchSeq := h.batchSeq.Add(1)
	for i := range events {
		events[i].tableID = subStat.tableID
		events[i].batchSeq = batchSeq
	}

	subStat.eventCh.Push(events...)
	return true
}

func (h *eventsHandler) GetSize(event kvEvent) int                                         { return 0 }
func (h *eventsHandler) GetArea(path logpuller.SubscriptionID, dest *subscriptionStat) int { return 0 }
func (h *eventsHandler) GetTimestamp(event kvEvent) dynstream.Timestamp {
	return dynstream.Timestamp(event.raw.CRTs)
}
func (h *eventsHandler) IsPaused(event kvEvent) bool { return false }

func (h *eventsHandler) GetType(event kvEvent) dynstream.EventType {
	if event.raw.IsResolved() {
		return dynstream.EventType{DataGroup: DataGroupResolvedTs, Property: dynstream.PeriodicSignal}
	}
	return dynstream.EventType{DataGroup: DataGroupDML, Property: dynstream.BatchableData}
}

func (h *eventsHandler) OnDrop(event kvEvent) {}
