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
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/utils/dynstream"
)

const (
	DataGroupResolvedTs = 1
	DataGroupDML        = 2
)

type eventsHandler struct {
}

func (h *eventsHandler) Path(event eventWithSubID) logpuller.SubscriptionID {
	return event.subID
}

func (h *eventsHandler) Handle(subStat *subscriptionStat, events ...eventWithSubID) bool {
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
	items := make([]*common.RawKVEntry, 0, len(events))
	for _, e := range events {
		items = append(items, e.raw)
	}
	subStat.eventCh <- kvEvents{
		kvs:     items,
		subID:   subStat.subID,
		tableID: subStat.tableID,
	}
	return true
}

func (h *eventsHandler) GetSize(event eventWithSubID) int                                  { return 0 }
func (h *eventsHandler) GetArea(path logpuller.SubscriptionID, dest *subscriptionStat) int { return 0 }
func (h *eventsHandler) GetTimestamp(event eventWithSubID) dynstream.Timestamp             { return 0 }
func (h *eventsHandler) IsPaused(event eventWithSubID) bool                                { return false }

func (h *eventsHandler) GetType(event eventWithSubID) dynstream.EventType {
	if event.raw.IsResolved() {
		return dynstream.EventType{DataGroup: DataGroupResolvedTs, Property: dynstream.PeriodicSignal}
	}
	return dynstream.EventType{DataGroup: DataGroupDML, Property: dynstream.BatchableData}
}

func (h *eventsHandler) OnDrop(event eventWithSubID) {}
