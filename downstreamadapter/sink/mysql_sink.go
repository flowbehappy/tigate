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
	"new_arch/downstreamadapter/conflictdetector"
	"new_arch/downstreamadapter/worker"
	"new_arch/utils/threadpool"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tiflow/pkg/causality"
)

const (
	// DefaultConflictDetectorSlots indicates the default slot count of conflict detector. TODO:check this
	DefaultConflictDetectorSlots uint64 = 16 * 1024
)

// mysql sink 负责 mysql 类型下游的 sink 模块
// sink 接收已经达到下推资格（该资格指的是不需要等其他 ddl 或者 sync point 语句下推，
// 可以进入 conflict detector 开始计算冲突，没有冲突就可以下推了
// 一个 event dispatcher manager 对应一个 mysqlSink
type MysqlSink struct {
	conflictDetector *conflictdetector.ConflictDetector[*Event]
	// TableProgress 里面维护了目前正在 sink 中的 event ts 信息
	// TableProgress 对外提供查询当前 table checkpointTs 的能力
	// TableProgress 对外提供当前 table 是否有 event 在 sink 中等待被 flush 的能力--用于判断 ddl 是否达到下推条件
	tableProgressMap map[*Span]TableProgress
	// 主要是要保持一样的生命周期？不然 channel 会对应不上
	workers  []*worker.MysqlWorker
	eventChs map[*Span]chan *Event
	tasks    map[*Span]*MysqlSinkTask

	mutex sync.Mutex // 用于新插入 dispatcher 或者 remove dispatcher 时保护 tableProgressMap，eventChs，tasks 对象
}

// event dispatcher manager 初始化的时候创建 mysqlSink 对象
func newMysqlSink(workerCount int) *MysqlSink {
	mysqlSink := MysqlSink{
		conflictDetector: conflictdetector.NewConflictDetector[*Event](DefaultConflictDetectorSlots, conflictdetector.TxnCacheOption{
			Count:         workerCount,
			Size:          1024,
			BlockStrategy: causality.BlockStrategyWaitEmpty,
		}),
		tableProgressMap: make(map[*Span]TableProgress),
		eventChs:         make(map[*Span]chan *Event),
		tasks:            make(map[*Span]*MysqlSinkTask),
	}
	for i := 0; i < workerCount; i++ {
		mysqlSink.workers = append(mysqlSink.workers, worker.NewMysqlWorker(mysqlSink.conflictDetector.GetOutChByCacheID(int64(i))))
	}
	// TODO: add worker task into thread pool
	return &mysqlSink
}

func (s *MysqlSink) addTableSpan(tableSpan *Span) {
	tableProgress := newTableProgress(tableSpan)
	ch := make(chan *Event, 100) // 先瞎拍
	task := newMysqlSinkTask(tableSpan, tableProgress, ch, s.conflictDetector)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.tableProgressMap[tableSpan] = tableProgress
	s.eventChs[tableSpan] = ch
	s.tasks[tableSpan] = task

	// 塞入 threadpool 中
}

func (s *MysqlSink) RemoveTableSpan(tableSpan *Span) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.tableProgressMap, tableSpan)
	delete(s.eventChs, tableSpan)
	delete(s.tasks, tableSpan)

	// 从 thread pool 中删除这个任务 -- TODO:这个后续看一下是从 scheduler 里面进去还是直接操作 thread pool 做删除
}

// 先写一个插入数据的 task，看看后面 reactor 的要不要写一个新的 task
// 同个 table 的 add event 是需要满足前后顺序的，所以这里的 task 还是只应该有 dispatcher 个
// 那也就是她的 task 是每一个新的 table 出现就创建，然后永远在轮训，直到 dispatcher 被移除了 -- 所以要有个 table 和 task 的 map
type MysqlSinkTask struct {
	conflictDetector *conflictdetector.ConflictDetector[*Event] // 这个会被多个任务共用，所以需要做到内部 thread safe
	tableSpan        *Span
	tableProgress    *TableProgress
	eventCh          chan *Event
}

// 这个任务本身就是把
func newMysqlSinkTask(tableSpan *TableSpan, tableProgress *TableProgress, eventCh chan *Event, conflictDetector *ConflictDetector) *MysqlSinkTask {
	return &MysqlSinkTask{
		conflictDetector: conflictDetector,
		tableSpan:        tableSpan,
		tableProgress:    tableProgress,
		eventCh:          eventCh,
	}
}

func (t *MysqlSinkTask) execute(timeout time.Duration) threadpool.TaskStatus {
	// 从 pending 的 task 里面拿出来塞下去，指导超时或者没有 event 了
	timer := time.NewTimer(timeout)
	for {
		select {
		case event := <-t.eventCh:
			t.tableProgress.Add(event)
			t.conflictDetector.Add(event, t.tableProgress) // 这个要测过一次要多少，有可能直接超时了
		case <-timer.C:
			return threadpool.Running
		default:
			// 也就是还没超时，但是 event 也没有，那就直接把任务扔回去
			if !timer.Stop() {
				<-timer.C
			}
			return threadpool.Running
		}
	}
}

func (t *MysqlSinkTask) await() threadpool.TaskStatus {
	log.Error("MysqlSinkTask should not call await()")
	return threadpool.Failed
}

func (t *MysqlSinkTask) release() {
	//?给我整不会了感觉啥也不用干啊，是不是因为 go 自动回收所以啥也不用干？
}
