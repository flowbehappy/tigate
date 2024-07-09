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

package sink

import (
	"container/list"
	"sync"

	"github.com/flowbehappy/tigate/common"
)

// TableProgress 里面维护了目前正在 sink 中的 event ts 信息
// TableProgress 对外提供查询当前 table checkpointTs 的能力
// TableProgress 对外提供当前 table 是否有 event 在 sink 中等待被 flush 的能力--用于判断 ddl 是否达到下推条件
//
// 本质是要频繁的删除随机数据，插入递增数据，查询最小值，后面自己可以实现一个红黑树吧，或者其他结构，先用 list 苟一苟
// thread safe
type TableProgress struct {
	mutex   sync.Mutex
	list    *list.List
	elemMap map[*Ts]*list.Element
}

// 按 commitTs 为主，startTs 为辅排序
type Ts struct {
	commitTs uint64
	startTs  uint64
}

func NewTableProgress() *TableProgress {
	tableProgress := &TableProgress{
		list:    list.New(),
		elemMap: make(map[*Ts]*list.Element),
	}
	return tableProgress
}

func (p *TableProgress) Add(event *common.TxnEvent) {
	ts := Ts{startTs: event.StartTs, commitTs: event.CommitTs}
	p.mutex.Lock()
	defer p.mutex.Unlock()
	elem := p.list.PushBack(ts)
	p.elemMap[&ts] = elem
}

// 而且删除可以认为是批量的？但要不要做成批量可以后面再看
func (p *TableProgress) Remove(event *common.TxnEvent) {
	ts := Ts{startTs: event.StartTs, commitTs: event.CommitTs}
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if elem, ok := p.elemMap[&ts]; ok {
		p.list.Remove(elem)
		delete(p.elemMap, &ts)
	}
}

func (p *TableProgress) Empty() bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.list.Len() == 0
}

// 返回目前 sink 还没 flush 下去的 event 中 commitTs 最小的。
// 用于结合当前 dispatcher 收到的 resolvedTs，和 event list 中的 event 值计算 table checkpointTs
func (p *TableProgress) SmallestCommitTs() uint64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.list.Len() == 0 {
		return 0
	}
	return p.list.Front().Value.(Ts).commitTs
}
