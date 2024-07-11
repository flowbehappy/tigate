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

package dispatcher

import (
	"sync"
	"sync/atomic"
	"time"
)

type EventFeedSpeedRatio struct {
	ratio         float32
	updatedTime   time.Time
	lastUsedBytes int
	lastCheckTime time.Time
}

const MemoryQuota = 10 * 1024 * 1024 // 10MB

/*
MemoryUsage is a struct to record the mainly memory usage of the dispatcher(including sink and worker)
When the event is pushed into dispatcher.Ch, the memory usage will be recorded.
When the event is flushed to downstream successfully, the memory usage will be released.(这边做的是近似)
MemoryUsage is record the memory for each dispatcher.
*/
type MemoryUsage struct {
	mutex                 sync.Mutex
	UsedBytes             int
	commitTsList          []uint64       // record the commitTs in order.
	commitTsMemoryCostMap map[uint64]int // commitTs -> memory cost(total for the commitTs)

	speedRatio *EventFeedSpeedRatio
}

func NewMemoryUsage() *MemoryUsage {
	return &MemoryUsage{
		UsedBytes:             0,
		commitTsList:          make([]uint64, 0),
		commitTsMemoryCostMap: make(map[uint64]int),
		speedRatio: &EventFeedSpeedRatio{
			ratio:         float32(1),
			updatedTime:   time.Now(),
			lastUsedBytes: 0,
			lastCheckTime: time.Now(),
		},
	}
}

func (m *MemoryUsage) GetUsedBytes() int {
	return m.UsedBytes
}

func (m *MemoryUsage) Add(commitTs uint64, size int) {
	atomic.AddInt64(&GetGlobalMemoryUsage().UsedBytes, int64(size))

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.commitTsList) == 0 || m.commitTsList[len(m.commitTsList)-1] < commitTs {
		m.commitTsList = append(m.commitTsList, commitTs)
		m.commitTsMemoryCostMap[commitTs] = size
	} else {
		m.commitTsMemoryCostMap[commitTs] += size
	}

	m.UsedBytes += size
}

// the event with commitTs <= checkpointTs is flushed to downstream successfully,
// we can release the memory cost of these events.
func (m *MemoryUsage) Release(checkpointTs uint64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	index := 0
	for i, ts := range m.commitTsList {
		if ts <= checkpointTs {
			m.UsedBytes -= m.commitTsMemoryCostMap[ts]
			atomic.AddInt64(&GetGlobalMemoryUsage().UsedBytes, int64(m.commitTsMemoryCostMap[ts])*-1)
			delete(m.commitTsMemoryCostMap, ts)
		} else {
			index = i
			break
		}
	}
	if index > 0 {
		m.commitTsList = m.commitTsList[index:]
	}
}

// UpdatedSpeedRatio is used to decide whether we need to adjust the speed ratio for the dispatcher.
// We input the ratio of the current speed, and return whether we need to adjust the speed ratio,
// and the new speed ratio.
// We only deal with this function each 30 seconds.(也就是太过频繁的时候我不 check 了，也不更新数值)
// The adjustment rules are:
//  1. We decrease the speed ratio when it satisifies the following condition:
//     1.1. The used memory is larger than 50% of the memory quota.
//     1.2. Memory usage is increasing.
//     1.3. It has been more than 120 seconds since the new ratio value took effect.
//     The new ratio will be the old_ratio * (0.5 + (1-used_memory/quota) * 0.5)
//  2. We increase the speed ratio when it satisifies the following condition:
//     2.1. The used memory is smaller than 25% of the memory quota.
//     2.2. Memory usage is decreasing.
//     2.3. It has been more than 120 seconds since the new ratio value took effect.
//     The new ratio will be the old_ratio * (1 - used_memory/quota) * 2
func (m *MemoryUsage) UpdatedSpeedRatio(ratio float32) (bool, float32) {
	if time.Since(m.speedRatio.lastCheckTime) < 30*time.Second {
		return false, ratio
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.speedRatio.lastCheckTime = time.Now()

	if m.speedRatio.ratio != ratio {
		// new ratio
		m.speedRatio.ratio = ratio
		m.speedRatio.updatedTime = time.Now()
		m.speedRatio.lastUsedBytes = m.UsedBytes
		return false, ratio
	}

	if m.UsedBytes > MemoryQuota/2 && m.UsedBytes > m.speedRatio.lastUsedBytes && time.Since(m.speedRatio.updatedTime) > 120*time.Second {
		// decrease the speed ratio
		newRatio := ratio * (0.5 + (1-float32(m.UsedBytes)/MemoryQuota)*0.5)
		m.speedRatio.lastUsedBytes = m.UsedBytes
		return true, newRatio
	}

	if m.UsedBytes < MemoryQuota/4 && m.UsedBytes < m.speedRatio.lastUsedBytes && time.Since(m.speedRatio.updatedTime) > 120*time.Second {
		// increase the speed ratio
		newRatio := ratio * (1 - float32(m.UsedBytes)/MemoryQuota) * 2
		m.speedRatio.lastUsedBytes = m.UsedBytes
		return true, newRatio
	}

	m.speedRatio.lastUsedBytes = m.UsedBytes
	return false, ratio
}

/*
GlobalMemoryUsage is a struct to record the memory usage of events for all dispatchers in the instances.
GlobalMemoryUsage is a singleton.
*/
type GlobalMemoryUsage struct {
	UsedBytes int64
}

var globalMemoryUsage *GlobalMemoryUsage
var once sync.Once

func GetGlobalMemoryUsage() *GlobalMemoryUsage {
	if globalMemoryUsage == nil {
		once.Do(func() {
			globalMemoryUsage = &GlobalMemoryUsage{UsedBytes: 0}
		})
	}
	return globalMemoryUsage
}
