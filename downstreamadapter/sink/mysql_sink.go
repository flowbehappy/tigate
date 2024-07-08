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
	"net/url"
	"sync"

	"github.com/flowbehappy/tigate/downstreamadapter/sink/conflictdetector"
	"github.com/flowbehappy/tigate/downstreamadapter/worker"
	"github.com/flowbehappy/tigate/downstreamadapter/writer"
	"github.com/flowbehappy/tigate/utils/threadpool"

	"github.com/pingcap/log"
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
// 实现 Sink 的接口
type MysqlSink struct {
	changefeedID     uint64
	conflictDetector *conflictdetector.ConflictDetector[*Event]
	// TableProgress 里面维护了目前正在 sink 中的 event ts 信息
	// TableProgress 对外提供查询当前 table checkpointTs 的能力
	// TableProgress 对外提供当前 table 是否有 event 在 sink 中等待被 flush 的能力--用于判断 ddl 是否达到下推条件
	tableProgressMap map[*Span]*TableProgress
	// 主要是要保持一样的生命周期？不然 channel 会对应不上
	// workers  []*worker.MysqlWorker
	ddlWorker *worker.MysqlDDLWorker
	eventChs  map[*Span]chan *Event // 这个感觉最好也不要用 channel，用一个代表 channal 的 struct
	tasks     map[*Span]*MysqlSinkTask

	mutex sync.Mutex // 用于新插入 dispatcher 或者 remove dispatcher 时保护 tableProgressMap，eventChs，tasks 对象
}

// event dispatcher manager 初始化的时候创建 mysqlSink 对象
func NewMysqlSink(workerCount int, sinkURI *url.URL) *MysqlSink {
	mysqlSink := MysqlSink{
		conflictDetector: conflictdetector.NewConflictDetector[*Event](DefaultConflictDetectorSlots, conflictdetector.TxnCacheOption{
			Count:         workerCount,
			Size:          1024,
			BlockStrategy: causality.BlockStrategyWaitEmpty,
		}),
		tableProgressMap: make(map[*Span]*TableProgress),
		eventChs:         make(map[*Span]chan *Event),
		tasks:            make(map[*Span]*MysqlSinkTask),
	}

	mysqlSink.initWorker(workerCount, sinkURI)

	return &mysqlSink
}

func (s *MysqlSink) initWorker(workerCount int, sinkURI *url.URL) {
	cfg, db, err := writer.NewMysqlConfigAndDB(sinkURI)
	if err != nil {
		log.Error("newMysqlConfigAndDB failed", err)
		return
	}
	// 初始化 ddl/syncpoint 用的 worker
	s.ddlWorker = &worker.MysqlDDLWorker{MysqlWriter: writer.NewMysqlWriter(db, cfg)}

	// 初始化 dml worker 相关 task -- 这些是长时间 run 的 task
	for i := 0; i < workerCount; i++ {
		threadpool.GetTaskSchedulerInstance().WorkerTaskScheduler.Submit(worker.NewMysqlWorkerDMLEventTask(&s.conflictDetector.GetOutChByCacheID(int64(i)), db, cfg, 128))
	}
}

func (s *MysqlSink) AddDMLEvent(tableSpan *Span, event *Event) {
	s.mutex.Lock() // TODO:改成读写锁
	defer s.mutex.Unlock()
	if ch, ok := s.eventChs[tableSpan]; ok {
		if tableProgress, ok := s.tableProgressMap[tableSpan]; ok {
			tableProgress.Add(event)
		}
		ch <- event
	} else {
		log.Error("unknown Span for Mysql Sink: ", tableSpan)
		// TODO: return error here
	}
}

func (s *MysqlSink) AddDDLAndSyncPointEvent(tableSpan *Span, event *Event) { // 或许 ddl 也可以考虑有专用的 worker？
	s.mutex.Lock() // TODO:改成读写锁
	defer s.mutex.Unlock()

	if tableProgress, ok := s.tableProgressMap[tableSpan]; ok { // 这里就可以释放锁了吧？
		tableProgress.Add(event)
		task := worker.NewMysqlWorkerDDLEventTask(s.ddlWorker, event, func() { tableProgress.Remove(event) }) // 先固定用 0 号 worker
		threadpool.GetTaskSchedulerInstance().WorkerTaskScheduler.Submit(task)
	}
}

func (s *MysqlSink) AddTableSpan(tableSpan *Span) {
	tableProgress := NewTableProgress(tableSpan)
	ch := make(chan *Event, 100) // 先瞎拍
	task := newMysqlSinkTask(tableSpan, tableProgress, ch, s.conflictDetector)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.tableProgressMap[tableSpan] = tableProgress
	s.eventChs[tableSpan] = ch
	s.tasks[tableSpan] = task

	threadpool.GetTaskSchedulerInstance().SinkTaskScheduler.Submit(task)
}

func (s *MysqlSink) RemoveTableSpan(tableSpan *Span) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if task, ok := s.tasks[tableSpan]; ok {
		task.Cancel()

		delete(s.tableProgressMap, tableSpan)
		delete(s.eventChs, tableSpan)
		delete(s.tasks, tableSpan)

	} else {
		// Error
	}
}

func (s *MysqlSink) IsEmpty(tableSpan *Span) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if tableProgress, ok := s.tableProgressMap[tableSpan]; ok {
		return tableProgress.Empty()
	}

	log.Error("Invalid table span in MysqlSink::isEmpty", tableSpan)
	//return error
	return false
}

func (s *MysqlSink) GetSmallestCommitTs(tableSpan *Span) uint64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if tableProgress, ok := s.tableProgressMap[tableSpan]; ok {
		return tableProgress.SmallestCommitTs()
	}

	log.Error("Invalid table span in MysqlSink::isEmpty", tableSpan)
	//return error
	return 0 //给个 error 最后
}
