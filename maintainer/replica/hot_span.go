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
	HotSpanScoreThreshold = 3           // TODO: bump to 10 befroe release
	HotSpanMaxLevel       = 1

	EnableDynamicThreshold = false
	ImbalanceThreshold     = 3 // trigger merge after it is supported

	clearTimeout = 300 // seconds
)

// TODO: extract group interface
func getImbalanceThreshold(id GroupID) int {
	if id == defaultGroupID {
		return 1
	}
	return ImbalanceThreshold
}

type hotSpans struct {
	lock          sync.Mutex
	hotSpanGroups map[GroupID]map[common.DispatcherID]*HotSpan
}

type HotSpan struct {
	*SpanReplication
	HintMaxSpanNum uint64

	eventSizePerSecond   float32
	writeThreshold       float32 // maybe a dynamic value
	imbalanceCoefficient int     // fixed value for each group
	// score add 1 when the eventSizePerSecond is larger than writeThreshold*imbalanceCoefficient
	score          int
	lastUpdateTime time.Time
}

func NewHotSpans() *hotSpans {
	s := &hotSpans{
		hotSpanGroups: make(map[GroupID]map[common.DispatcherID]*HotSpan),
	}
	return s
}

func (s *hotSpans) getOrCreateGroup(groupID GroupID) map[common.DispatcherID]*HotSpan {
	group, ok := s.hotSpanGroups[groupID]
	if !ok {
		group = make(map[common.DispatcherID]*HotSpan)
		s.hotSpanGroups[groupID] = group
	}
	return group
}

func (s *hotSpans) getBatchByGroup(groupID GroupID, cache []*HotSpan) []*HotSpan {
	batchSize := cap(cache)
	if batchSize == 0 {
		batchSize = 1024
		cache = make([]*HotSpan, batchSize)
	}
	cache = cache[:0]

	outdatedSpans := make([]*HotSpan, 0)
	s.lock.Lock()
	defer s.lock.Unlock()
	hotSpanCache := s.getOrCreateGroup(groupID)
	if EnableDynamicThreshold && groupID != defaultGroupID {
		totalEventSizePerSecond := float32(0)
		for _, span := range hotSpanCache {
			totalEventSizePerSecond += span.eventSizePerSecond
		}
		if totalEventSizePerSecond > 0 {
			avg := float32(totalEventSizePerSecond / float32(len(hotSpanCache)))
			for _, span := range hotSpanCache {
				span.writeThreshold = avg
				span.HintMaxSpanNum = uint64(span.eventSizePerSecond / avg)
			}
		}
	}

	for _, span := range hotSpanCache {
		if time.Since(span.lastUpdateTime) > clearTimeout*time.Second {
			outdatedSpans = append(outdatedSpans, span)
		} else if span.score >= HotSpanScoreThreshold {
			cache = append(cache, span)
			if len(cache) >= batchSize {
				break
			}
		}
	}
	s.doClear(defaultGroupID, outdatedSpans...)
	return cache
}

func (s *hotSpans) updateHotSpan(span *SpanReplication, status *heartbeatpb.TableSpanStatus) {
	if status.ComponentStatus != heartbeatpb.ComponentState_Working {
		return
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	hotSpanCache := s.getOrCreateGroup(span.groupID)
	if status.EventSizePerSecond < HotSpanWriteThreshold {
		if span, ok := hotSpanCache[span.ID]; ok && status.EventSizePerSecond < span.writeThreshold {
			span.score--
			if span.groupID == defaultGroupID && span.score == 0 {
				delete(hotSpanCache, span.ID)
			}
		}
		return
	}

	cache, ok := hotSpanCache[span.ID]
	if !ok {
		cache = &HotSpan{
			SpanReplication:      span,
			writeThreshold:       HotSpanWriteThreshold,
			imbalanceCoefficient: getImbalanceThreshold(span.groupID),
			score:                0,
		}
		hotSpanCache[span.ID] = cache
	}
	if status.EventSizePerSecond >= cache.writeThreshold*float32(cache.imbalanceCoefficient) {
		cache.score++
		cache.lastUpdateTime = time.Now()
	}
}

func (s *hotSpans) clearHotSpansByGroup(groupID GroupID, spans ...*HotSpan) {
	// extract needClear method to simply the clear behavior
	if groupID != defaultGroupID {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	log.Info("clear outdated hot spans", zap.Int("count", len(spans)))
	s.doClear(groupID, spans...)
}

func (s *hotSpans) doClear(groupID GroupID, spans ...*HotSpan) {
	hotSpanCache := s.getOrCreateGroup(groupID)
	for _, span := range spans {
		delete(hotSpanCache, span.ID)
	}
}

func (s *hotSpans) stat() string {
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
		for i, cnt := range cnt {
			if cnt == 0 {
				continue
			}
			res.WriteString("score ")
			res.WriteString(strconv.Itoa(i + 1))
			res.WriteString("->")
			res.WriteString(strconv.Itoa(cnt))
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

func (db *ReplicationDB) ClearHotSpansByGroup(groupID GroupID, spans ...*HotSpan) {
	db.hotSpans.clearHotSpansByGroup(groupID, spans...)
}

func (db *ReplicationDB) GetHotSpansByGroup(groupID GroupID, cache []*HotSpan) []*HotSpan {
	return db.hotSpans.getBatchByGroup(groupID, cache)
}

func (db *ReplicationDB) GetHotSpanStat() string {
	return db.hotSpans.stat()
}
