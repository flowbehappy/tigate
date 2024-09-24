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

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/prometheus/client_golang/prometheus"
)

// TableProgress 里面维护了目前 sink 中的 event ts 信息
// TableProgress 对外提供查询当前 table checkpointTs 的能力
// TableProgress 对外提供当前 table 是否有 event 在等待被 flush 的能力--用于判断 ddl 是否达到下推条件
//
// 本质是要频繁的删除随机数据，插入递增数据，查询最小值，后面自己可以实现一个红黑树吧，或者其他结构，先用 list 苟一苟
// 需要加个测试保证插入数据不会出现 commitTs 倒退的问题
// thread safe
type TableProgress struct {
	mutex       sync.Mutex
	list        *list.List
	elemMap     map[Ts]*list.Element
	maxCommitTs uint64

	metricSinkOutputChunkSize prometheus.Observer
}

// 按 commitTs 为主，startTs 为辅排序
type Ts struct {
	commitTs uint64
	startTs  uint64
}

func NewTableProgress(changefeedID model.ChangeFeedID) *TableProgress {
	tableProgress := &TableProgress{
		list:                      list.New(),
		elemMap:                   make(map[Ts]*list.Element),
		maxCommitTs:               0,
		metricSinkOutputChunkSize: metrics.SinkOutputChunkSize.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
	}
	return tableProgress
}

func (p *TableProgress) Add(event common.FlushableEvent) {
	ts := Ts{startTs: event.GetStartTs(), commitTs: event.GetCommitTs()}
	p.mutex.Lock()
	defer p.mutex.Unlock()
	elem := p.list.PushBack(ts)
	p.elemMap[ts] = elem
	p.maxCommitTs = event.GetCommitTs()
	event.AddPostFlushFunc(func() { p.Remove(event) })
	event.AddPostFlushFunc(func() {
		p.metricSinkOutputChunkSize.Observe(float64(event.GetChunkSize()))
	})
}

// 而且删除可以认为是批量的？但要不要做成批量可以后面再看
func (p *TableProgress) Remove(event common.Event) {
	ts := Ts{startTs: event.GetStartTs(), commitTs: event.GetCommitTs()}
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if elem, ok := p.elemMap[ts]; ok {
		p.list.Remove(elem)
		delete(p.elemMap, ts)
	}
}

func (p *TableProgress) Empty() bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.list.Len() == 0
}

func (p *TableProgress) Pass(event *common.DDLEvent) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.maxCommitTs = event.FinishedTs
}

// 返回当前 tableSpan 中最大的 checkpointTs，也就是最大的 ts，并且 <= ts 之前的数据都已经成功写下去了
// 1. 假设目前 sink 还有没 flush 下去的 event，就拿最小的这个 event的 commitTs。
// 2. 反之，则选择收到过 event 中 commitTs 最大的那个。
// 并且返回目前 是不是为空的状态，如果是空的话，resolvedTs 大于 checkpointTs，则用 resolvedTs 作为真的 checkpointTs
func (p *TableProgress) GetCheckpointTs() (uint64, bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.list.Len() == 0 {
		if p.maxCommitTs == 0 {
			return 0, true
		}
		return p.maxCommitTs - 1, true
	}
	return p.list.Front().Value.(Ts).commitTs - 1, false
}
