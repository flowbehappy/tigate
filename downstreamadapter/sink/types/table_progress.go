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

package types

import (
	"container/list"
	"sync"
	"time"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"go.uber.org/zap"
)

// TableProgress maintains event timestamp information in the sink.
// It provides the ability to:
// - Query the current table checkpoint timestamp
// - Check if there are any events waiting to be flushed
// - Query event size flushed per second
//
// TableProgress assumes the event timestamps are monotonically increasing.
//
// This struct is thread-safe.
type TableProgress struct {
	rwMutex     sync.RWMutex
	list        *list.List
	elemMap     map[Ts]*list.Element
	maxCommitTs uint64

	// cumulate dml event size for a period of time,
	// it will be cleared after once query
	cumulateEventSize int64
	// it used to calculate the sum-dml-event-size/s for each dispatcher
	lastQueryTime time.Time
}

// Ts represents a timestamp pair, used for sorting primarily by commitTs and secondarily by startTs.
type Ts struct {
	commitTs uint64
	startTs  uint64
}

// NewTableProgress creates and initializes a new TableProgress instance.
func NewTableProgress() *TableProgress {
	return &TableProgress{
		list:              list.New(),
		elemMap:           make(map[Ts]*list.Element),
		maxCommitTs:       0,
		cumulateEventSize: 0,
		lastQueryTime:     time.Now(),
	}
}

// Add inserts a new event into the TableProgress.
func (p *TableProgress) Add(event commonEvent.FlushableEvent) {
	ts := Ts{startTs: event.GetStartTs(), commitTs: event.GetCommitTs()}
	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()
	elem := p.list.PushBack(ts)
	p.elemMap[ts] = elem
	p.maxCommitTs = event.GetCommitTs()
	log.Info("add event to table progress", zap.Any("event", event), zap.Any("cumulateEventSize", p.cumulateEventSize), zap.Any("eventSize", event.GetSize()))
	event.PushFrontFlushFunc(func() {
		p.Remove(event)
	})
}

// Remove deletes an event from the TableProgress.
// Note: Consider implementing batch removal in the future if needed.
func (p *TableProgress) Remove(event commonEvent.Event) {
	ts := Ts{startTs: event.GetStartTs(), commitTs: event.GetCommitTs()}
	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()

	if elem, ok := p.elemMap[ts]; ok {
		p.list.Remove(elem)
		delete(p.elemMap, ts)
	}
	p.cumulateEventSize += event.GetSize()
	log.Info("remove event from table progress", zap.Any("event", event), zap.Any("cumulateEventSize", p.cumulateEventSize), zap.Any("eventSize", event.GetSize()))
}

// Empty checks if the TableProgress is empty.
func (p *TableProgress) Empty() bool {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()
	return p.list.Len() == 0
}

// Pass updates the maxCommitTs with the given event's commit timestamp.
func (p *TableProgress) Pass(event commonEvent.BlockEvent) {
	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()
	p.maxCommitTs = event.GetCommitTs()
}

// GetCheckpointTs returns the current checkpoint timestamp for the table span.
// It returns:
// 1. The commitTs of the earliest unflushed event minus 1, if there are unflushed events.
// 2. The highest commitTs seen minus 1, if there are no unflushed events.
// 3. 0, if no events have been processed yet.
//
// It also returns a boolean indicating whether the TableProgress is empty.
// If empty and resolvedTs > checkpointTs, use resolvedTs as the actual checkpointTs.
func (p *TableProgress) GetCheckpointTs() (uint64, bool) {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	if p.list.Len() == 0 {
		if p.maxCommitTs == 0 {
			return 0, true
		}
		return p.maxCommitTs - 1, true
	}
	return p.list.Front().Value.(Ts).commitTs - 1, false
}

// GetEventSizePerSecond returns the sum-dml-event-size/s between the last query time and now.
// Besides, it clears the cumulateEventSize and update lastQueryTime to prepare for the next query.
func (p *TableProgress) GetEventSizePerSecond() float32 {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	log.Info("get event size per second", zap.Any("cumulateEventSize", p.cumulateEventSize), zap.Any("lastQueryTime", p.lastQueryTime))
	eventSizePerSecond := float32(p.cumulateEventSize) / float32(time.Since(p.lastQueryTime).Seconds())
	p.cumulateEventSize = 0
	p.lastQueryTime = time.Now()

	return eventSizePerSecond
}
