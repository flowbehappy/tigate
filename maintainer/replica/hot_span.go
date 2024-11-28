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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

const (
	HotSpanWriteThreshold = 1024 * 1024 // 1MB per second
	HotSpanScoreThreshold = 10
	HotSpanMaxLevel       = 1
	clearTimeout          = 300 // seconds

	// TODO: use the imbalance threshold to calculate the score.
	// use multiple thresholds to control different split groups. For example:
	// For score 1-5, dispatcher it to the same split group.
	// For score 6-10, use single split group to track the hot span.
)

type HotSpans struct {
	lock          sync.Mutex
	hotSpanGroups map[GroupID]map[common.DispatcherID]*hotSpan
}

type hotSpan struct {
	*SpanReplication
	// A span that continuously writes more than hotSpanWriteThreshold for
	// hotSpanScoreThreshold times will be considered a hot span.
	// TODO: use more flexible and efficient strategy to calculate the score.
	score          int
	lastUpdateTime time.Time
}

func NewHotSpans() *HotSpans {
	s := &HotSpans{
		hotSpanGroups: make(map[GroupID]map[common.DispatcherID]*hotSpan),
	}
	return s
}

func (s *HotSpans) getOrCreateGroup(groupID GroupID) map[common.DispatcherID]*hotSpan {
	group, ok := s.hotSpanGroups[groupID]
	if !ok {
		group = make(map[common.DispatcherID]*hotSpan)
		s.hotSpanGroups[groupID] = group
	}
	return group
}

func (s *HotSpans) getBatch(cache []*SpanReplication) []*SpanReplication {
	batchSize := cap(cache)
	if batchSize == 0 {
		batchSize = 1024
		cache = make([]*SpanReplication, batchSize)
	}
	cache = cache[:0]

	s.lock.Lock()
	defer s.lock.Unlock()
	hotSpanCache := s.getOrCreateGroup(defaultGroupID)
	outdatedSpans := make([]*SpanReplication, 0)
	for _, span := range hotSpanCache {
		if time.Since(span.lastUpdateTime) > clearTimeout*time.Second {
			outdatedSpans = append(outdatedSpans, span.SpanReplication)
		} else if span.score >= HotSpanScoreThreshold {
			cache = append(cache, span.SpanReplication)
			if len(cache) >= batchSize {
				break
			}
		}
	}
	s.doClear(defaultGroupID, outdatedSpans...)
	return cache
}

func (s *HotSpans) updateHotSpan(span *SpanReplication, status *heartbeatpb.TableSpanStatus) {
	if status.ComponentStatus != heartbeatpb.ComponentState_Working {
		return
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	hotSpanCache := s.getOrCreateGroup(span.groupID)
	if status.EventSizePerSecond < HotSpanWriteThreshold {
		span, ok := hotSpanCache[span.ID]
		if !ok {
			return
		}
		span.score--
		if span.score == 0 {
			delete(hotSpanCache, span.ID)
		}
	}
	cache, ok := hotSpanCache[span.ID]
	if !ok {
		cache = &hotSpan{
			SpanReplication: span,
			score:           0,
		}
		hotSpanCache[span.ID] = cache
	}
	cache.score++
	cache.lastUpdateTime = time.Now()
}

func (s *HotSpans) clearHotSpans(groupID GroupID, spans ...*SpanReplication) {
	s.lock.Lock()
	defer s.lock.Unlock()
	log.Info("clear outdated hot spans", zap.Int("count", len(spans)))
	s.doClear(groupID, spans...)
}

func (s *HotSpans) doClear(groupID GroupID, spans ...*SpanReplication) {
	hotSpanCache := s.getOrCreateGroup(groupID)
	for _, span := range spans {
		delete(hotSpanCache, span.ID)
	}
}

func (s *HotSpans) stat() string {
	s.lock.Lock()
	defer s.lock.Unlock()

	var res strings.Builder
	total := 0
	for groupID, hotSpanCache := range s.hotSpanGroups {
		if total > 0 {
			res.WriteString(" ")
		}
		res.WriteString(printGroupID(groupID))
		res.WriteString(": [")
		cnt := [HotSpanScoreThreshold + 1]int{}
		for _, span := range hotSpanCache {
			score := min(HotSpanScoreThreshold, span.score)
			cnt[score]++
			total++
		}
		for i := 1; i <= 10; i++ {
			// if cnt[i] == 0 {
			// 	continue
			// }
			res.WriteString("score ")
			res.WriteString(strconv.Itoa(i))
			res.WriteString("->")
			res.WriteString(strconv.Itoa(cnt[i]))
			res.WriteString("; ")
		}
		res.WriteString("]")
	}
	if total == 0 {
		return "No hot spans"
	}
	return res.String()
}

func (db *ReplicationDB) UpdateHotSpan(span *SpanReplication, status *heartbeatpb.TableSpanStatus) {
	db.hotSpans.updateHotSpan(span, status)
}

func (db *ReplicationDB) ClearHotSpans(spans ...*SpanReplication) {
	db.hotSpans.clearHotSpans(defaultGroupID, spans...)
}

func (db *ReplicationDB) GetHotSpans(cache []*SpanReplication) []*SpanReplication {
	return db.hotSpans.getBatch(cache)
}

func (db *ReplicationDB) GetHotSpanStat() string {
	return db.hotSpans.stat()
}
