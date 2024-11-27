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
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/utils/dynstream"
)

const (
	DataGroupResolvedTs = 1
	DataGroupEntries    = 2
	DataGroupError      = 3
)

type regionEvent struct {
	state  *regionFeedState
	worker *regionRequestWorker
	// only one of the following three fields will be set
	resolvedTs uint64
	entries    *cdcpb.Event_Entries_
	error      *cdcpb.Event_Error
}

type regionEventHandler struct {
}

func (h *regionEventHandler) Path(event regionEvent) uint64 {
	return event.state.getRegionID()
}

func (h *regionEventHandler) Handle(subStat *subscriptionStat, events ...kvEvent) bool {
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
	for i := range events {
		events[i].tableID = subStat.tableID
	}
	subStat.eventCh.Push(events...)
	return true
}

func (h *regionEventHandler) GetSize(event kvEvent) int { return 0 }
func (h *regionEventHandler) GetArea(path logpuller.SubscriptionID, dest *subscriptionStat) int {
	return 0
}
func (h *regionEventHandler) GetTimestamp(event kvEvent) dynstream.Timestamp {
	return dynstream.Timestamp(event.raw.CRTs)
}
func (h *regionEventHandler) IsPaused(event kvEvent) bool { return false }

func (h *regionEventHandler) GetType(event kvEvent) dynstream.EventType {
	if event.raw.IsResolved() {
		return dynstream.EventType{DataGroup: DataGroupResolvedTs, Property: dynstream.PeriodicSignal}
	}
	return dynstream.EventType{DataGroup: DataGroupDML, Property: dynstream.BatchableData}
}

func (h *regionEventHandler) OnDrop(event kvEvent) {}
