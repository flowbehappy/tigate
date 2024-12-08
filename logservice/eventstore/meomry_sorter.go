package eventstore

// Copyright 2022 PingCAP, Inc.
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

import (
	"container/heap"
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

const (
	defaultCapacity = 1024 * 1024 * 1024 * 4 // 4GB
	gcThreshold     = defaultCapacity / 4
	gcInterval      = 20 * time.Second
)

// MemorySorter accepts out-of-order raw kv entries and output sorted entries.
type MemorySorter struct {
	// subscriptionID -> tableSorter
	subscriptions *sync.Map
	// memory usage in bytes
	memoryUsage *atomic.Int64
	// The maximum memory usage of the sorter in bytes.
	capacity int64
	// Whether the sorter is available to accept new events.
	isAvailable *atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc
}

// New creates a new tableSorter.
func NewMemorySorter(ctx context.Context) *MemorySorter {
	ctx, cancel := context.WithCancel(ctx)
	s := &MemorySorter{
		subscriptions: &sync.Map{},
		memoryUsage:   &atomic.Int64{},
		capacity:      defaultCapacity,
		ctx:           ctx,
		cancel:        cancel,
		isAvailable:   &atomic.Bool{},
	}
	s.isAvailable.Store(true)
	log.Info("new memory sorter", zap.Int64("capacity", s.capacity))

	go s.backgroundGC()
	return s
}

// backgroundGC periodically checks memory usage and triggers GC if necessary
// TODO: Implement a more efficient GC strategy.
func (s *MemorySorter) backgroundGC() {
	ticker := time.NewTicker(gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if s.memoryUsage.Load() > int64(gcThreshold) {
				s.triggerGC()
			}
		}
	}
}

func (s *MemorySorter) triggerGC() {
	s.subscriptions.Range(func(key, value interface{}) bool {
		sub := key.(logpuller.SubscriptionID)
		ts := value.(*tableSorter)

		ts.mu.RLock()
		resolvedTs := ts.resolvedTs
		ts.mu.RUnlock()

		s.Clean(sub, resolvedTs)
		return true
	})
}

// AddTable implements sorter.SortEngine.
func (s *MemorySorter) AddSubscription(sub logpuller.SubscriptionID, startTs common.Ts) {
	if _, exists := s.subscriptions.LoadOrStore(sub, &tableSorter{resolvedTs: startTs}); exists {
		log.Panic("add an existing subscription", zap.Any("subscriptionID", sub))
	}
}

// RemoveSubscription implements sorter.SortEngine.
func (s *MemorySorter) RemoveSubscription(sub logpuller.SubscriptionID) {
	if _, exists := s.subscriptions.LoadAndDelete(sub); !exists {
		log.Panic("remove a non-existing subscription", zap.Any("subscriptionID", sub))
	}
}

// Add implements sorter.SortEngine.
func (s *MemorySorter) Add(tableSorter *tableSorter, events ...kvEvent) {
	// Must add resolvedTs event
	if len(events) == 1 && events[0].raw.IsResolved() {
		tableSorter.add(events[0])
		return
	}
	if !s.isAvailable.Load() {
		return
	}
	totalSize := tableSorter.add(events...)
	s.memoryUsage.Add(totalSize)
	if s.memoryUsage.Load() > int64(s.capacity) {
		s.isAvailable.Store(false)
	}
}

// Fetch returns an iterator of events for the given subscription.
func (s *MemorySorter) Fetch(sub logpuller.SubscriptionID, lowerBound, upperBound common.Ts) *eventIter {
	value, exists := s.subscriptions.Load(sub)
	if !exists {
		log.Panic("fetch events from a non-existing subscription", zap.Any("subscriptionID", sub))
	}
	return value.(*tableSorter).fetch(sub, lowerBound, upperBound)
}

// Clean removes events for the given subscription that are older than the given ts.
func (s *MemorySorter) Clean(sub logpuller.SubscriptionID, upperBound common.Ts) {
	value, exists := s.subscriptions.Load(sub)
	if !exists {
		log.Panic("clean a non-existing subscription", zap.Any("subscriptionID", sub))
	}
	totalSize := value.(*tableSorter).clean(sub, upperBound)
	s.memoryUsage.Add(-totalSize)
	if s.memoryUsage.Load() <= int64(s.capacity) {
		s.isAvailable.Store(true)
	}
}

// Close implements sorter.SortEngine.
func (s *MemorySorter) Close() error {
	s.cancel()
	s.subscriptions = &sync.Map{}
	return nil
}

type eventIter struct {
	resolved []*common.RawKVEntry
	position int

	prevStartTs  uint64
	prevCommitTs uint64
}

func newEventIter() *eventIter {
	return &eventIter{}
}

func (s *eventIter) IsEmpty() bool {
	return len(s.resolved) == 0
}

func (s *eventIter) Next() (event *common.RawKVEntry, isNewTxn bool, err error) {
	// Return early if no events available
	if len(s.resolved) == 0 || s.position >= len(s.resolved) {
		return nil, false, nil
	}

	event = s.resolved[s.position]

	// Check if this event belongs to a new transaction
	isNewTxn = s.prevCommitTs == 0 ||
		s.prevCommitTs != event.CRTs ||
		s.prevStartTs != event.StartTs

	// Update iterator state
	s.prevCommitTs = event.CRTs
	s.prevStartTs = event.StartTs
	s.position++

	// Clear the slice when all elements are traversed
	if s.position == len(s.resolved) {
		s.resolved = nil
	}

	return event, isNewTxn, nil
}

func (s *eventIter) Valid() bool {
	return s.position < len(s.resolved)
}

// Close implements sorter.EventIterator.
func (s *eventIter) Close() error {
	s.resolved = nil
	return nil
}

type tableSorter struct {
	mu         sync.RWMutex
	resolvedTs common.Ts
	unresolved eventHeap
	resolved   []*common.RawKVEntry
}

func (s *tableSorter) add(events ...kvEvent) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var totalSize int64
	for _, event := range events {
		totalSize += event.raw.Size()
		heap.Push(&s.unresolved, event.raw)
		if event.raw.IsResolved() {
			if s.resolvedTs == 0 {
				s.resolvedTs = event.raw.CRTs
			} else if s.resolvedTs < event.raw.CRTs {
				s.resolvedTs = event.raw.CRTs
			}
			for s.unresolved.Len() > 0 {
				item := heap.Pop(&s.unresolved).(*common.RawKVEntry)
				if item == event.raw {
					break
				}
				s.resolved = append(s.resolved, item)
			}
		}
	}
	return totalSize
}

func (s *tableSorter) fetch(
	sub logpuller.SubscriptionID, lowerBound, upperBound common.Ts,
) *eventIter {
	s.mu.RLock()
	defer s.mu.RUnlock()

	iter := newEventIter()

	if upperBound > s.resolvedTs {
		log.Panic("fetch unresolved events", zap.Any("subscriptionID", sub))
	}

	if len(s.resolved) == 0 {
		return iter
	}

	startIdx := sort.Search(len(s.resolved), func(idx int) bool {
		return s.resolved[idx].CRTs > lowerBound
	})

	endIdx := sort.Search(len(s.resolved), func(idx int) bool {
		return s.resolved[idx].CRTs > upperBound
	})

	iter.resolved = s.resolved[startIdx:endIdx]
	return iter
}

func (s *tableSorter) clean(sub logpuller.SubscriptionID, upperBound common.Ts) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if upperBound > s.resolvedTs {
		log.Panic("clean unresolved events", zap.Any("subscriptionID", sub))
	}

	if len(s.resolved) == 0 {
		return 0
	}

	startIdx := sort.Search(len(s.resolved), func(idx int) bool {
		return s.resolved[idx].CRTs > upperBound
	})

	totalSize := int64(0)
	for _, event := range s.resolved[startIdx:] {
		totalSize += event.Size()
	}

	s.resolved = s.resolved[startIdx:]
	return totalSize
}

func eventLess(i, j *common.RawKVEntry) bool {
	if i.CRTs != j.CRTs {
		return i.CRTs < j.CRTs
	}
	if i.StartTs != j.StartTs {
		return i.StartTs < j.StartTs
	}
	return getDMLOrder(i) < getDMLOrder(j)
}

type eventHeap []*common.RawKVEntry

func (h eventHeap) Len() int           { return len(h) }
func (h eventHeap) Less(i, j int) bool { return eventLess(h[i], h[j]) }
func (h eventHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *eventHeap) Push(x any) {
	*h = append(*h, x.(*common.RawKVEntry))
}

func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
