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

package replica

import (
	"sync"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
)

const (
	HotSpanWriteThreshold = 1024 * 1024 // 1MB per second
	HotSpanScoreThreshold = 10
)

type HotSpans struct {
	lock         sync.Mutex
	hotSpanCache map[common.DispatcherID]*hotSpan
}

type hotSpan struct {
	*SpanReplication
	// A span that continuously writes more than hotSpanWriteThreshold for
	// hotSpanScoreThreshold times will be considered a hot span.
	// TODO: use more flexible and efficient strategy to calculate the score.
	score int
}

func NewHotSpans() *HotSpans {
	return &HotSpans{
		hotSpanCache: make(map[common.DispatcherID]*hotSpan),
	}
}

func (s *HotSpans) GetBatch(cache []*SpanReplication) []*SpanReplication {
	batchSize := cap(cache)
	if batchSize == 0 {
		batchSize = 1024
		cache = make([]*SpanReplication, batchSize)
	}
	cache = cache[:0]

	s.lock.Lock()
	defer s.lock.Unlock()
	for _, span := range s.hotSpanCache {
		if span.score < HotSpanScoreThreshold {
			continue
		}
		cache = append(cache, span.SpanReplication)
		if len(cache) >= batchSize {
			break
		}
	}
	return cache
}

func (s *HotSpans) UpdateHotSpan(span *SpanReplication, status *heartbeatpb.TableSpanStatus) {
	if status.ComponentStatus != heartbeatpb.ComponentState_Working {
		return
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	if status.EventSizePerSecond < HotSpanWriteThreshold {
		span, ok := s.hotSpanCache[span.ID]
		if !ok {
			return
		}
		span.score--
		if span.score == 0 {
			delete(s.hotSpanCache, span.ID)
		}
	}

	if _, ok := s.hotSpanCache[span.ID]; !ok {
		s.hotSpanCache[span.ID] = &hotSpan{
			SpanReplication: span,
			score:           1,
		}
	} else {
		s.hotSpanCache[span.ID].score++
	}
}

func (s *HotSpans) ClearHotSpans(span ...*SpanReplication) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, span := range span {
		delete(s.hotSpanCache, span.ID)
	}
}
