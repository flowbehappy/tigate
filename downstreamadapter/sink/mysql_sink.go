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
	"database/sql"
	"math"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/downstreamadapter/sink/conflictdetector"
	"github.com/flowbehappy/tigate/downstreamadapter/sink/types"
	"github.com/flowbehappy/tigate/downstreamadapter/worker"
	"github.com/flowbehappy/tigate/downstreamadapter/writer"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"go.uber.org/zap"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/causality"
)

const (
	// DefaultConflictDetectorSlots indicates the default slot count of conflict detector. TODO:check this
	DefaultConflictDetectorSlots uint64 = 16 * 1024
)

type TableStatus struct {
	ch   chan *common.TxnEvent
	task *MysqlSinkTask
	//TableProgress 里面维护了目前正在 sink 中的 event ts 信息
	// // TableProgress 对外提供查询当前 table checkpointTs 的能力
	// // TableProgress 对外提供当前 table 是否有 event 在 sink 中等待被 flush 的能力--用于判断 ddl 是否达到下推条件
	progress *types.TableProgress
}

func (s *TableStatus) getCh() chan *common.TxnEvent {
	return s.ch
}

func (s *TableStatus) getTask() *MysqlSinkTask {
	return s.task
}

func (s *TableStatus) getProgress() *types.TableProgress {
	return s.progress
}

// mysql sink 负责 mysql 类型下游的 sink 模块
// sink 接收已经达到下推资格（该资格指的是不需要等其他 ddl 或者 sync point 语句下推，
// 可以进入 conflict detector 开始计算冲突，没有冲突就可以下推了
// 一个 event dispatcher manager 对应一个 mysqlSink
// 实现 Sink 的接口
type MysqlSink struct {
	changefeedID     string
	conflictDetector *conflictdetector.ConflictDetector

	// 主要是要保持一样的生命周期？不然 channel 会对应不上
	// workers  []*worker.MysqlWorker
	ddlWorker *worker.MysqlDDLWorker

	// Protect tableStatus
	mutex         sync.RWMutex
	tableStatuses map[*common.TableSpan]TableStatus
}

// event dispatcher manager 初始化的时候创建 mysqlSink 对象
func NewMysqlSink(workerCount int, cfg *writer.MysqlConfig, db *sql.DB) *MysqlSink {
	mysqlSink := MysqlSink{
		conflictDetector: conflictdetector.NewConflictDetector(DefaultConflictDetectorSlots, conflictdetector.TxnCacheOption{
			Count:         workerCount,
			Size:          1024,
			BlockStrategy: causality.BlockStrategyWaitEmpty,
		}),
		tableStatuses: make(map[*common.TableSpan]TableStatus),
	}

	mysqlSink.initWorker(workerCount, cfg, db)

	return &mysqlSink
}

func (s *MysqlSink) initWorker(workerCount int, cfg *writer.MysqlConfig, db *sql.DB) {
	// init ddl worker, which is for ddl event and sync point event
	s.ddlWorker = &worker.MysqlDDLWorker{MysqlWriter: writer.NewMysqlWriter(db, cfg)}

	// dml worker task will deal with all the dml events
	for i := 0; i < workerCount; i++ {
		threadpool.GetTaskSchedulerInstance().WorkerTaskScheduler.Submit(worker.NewMysqlWorkerDMLEventTask(s.conflictDetector.GetOutChByCacheID(int64(i)), db, cfg, 128), threadpool.CPUTask, time.Time{})
	}
}

func (s *MysqlSink) AddDMLEvent(tableSpan *common.TableSpan, event *common.TxnEvent) {
	s.mutex.RLock()
	tableStatus, ok := s.tableStatuses[tableSpan]
	s.mutex.Unlock()
	if !ok {
		log.Error("unknown Span for Mysql Sink: ", zap.Any("tableSpan", tableSpan))
		return
	}
	tableStatus.getProgress().Add(event)
	tableStatus.getCh() <- event
}

func (s *MysqlSink) AddDDLAndSyncPointEvent(tableSpan *common.TableSpan, event *common.TxnEvent) { // 或许 ddl 也可以考虑有专用的 worker？
	s.mutex.RLock() // TODO:改成读写锁
	tableStatus, ok := s.tableStatuses[tableSpan]
	s.mutex.Unlock()
	if !ok {
		log.Error("unknown Span for Mysql Sink: ", zap.Any("tableSpan", tableSpan))
		return
	}

	tableStatus.getProgress().Add(event)
	event.PostTxnFlushed = func() { tableStatus.getProgress().Remove(event) }
	task := worker.NewMysqlWorkerDDLEventTask(s.ddlWorker, event) // 先固定用 0 号 worker
	threadpool.GetTaskSchedulerInstance().WorkerTaskScheduler.Submit(task, threadpool.IOTask, time.Time{})

}

func (s *MysqlSink) AddTableSpan(tableSpan *common.TableSpan) {
	tableProgress := types.NewTableProgress()
	ch := make(chan *common.TxnEvent, 100) // 先瞎拍
	task := newMysqlSinkTask(tableSpan, tableProgress, ch, s.conflictDetector)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.tableStatuses[tableSpan] = TableStatus{
		ch:       ch,
		task:     task,
		progress: tableProgress,
	}

	threadpool.GetTaskSchedulerInstance().SinkTaskScheduler.Submit(task, threadpool.CPUTask, time.Time{})
}

// Called when the dispatcher should be moved.
// We will cancel the sink task, and not move the tableStatus now.
// We need to wait the MysqlSink.Empty(tableSpan) to be true, which means all the events in the sink have been flushed.
// 应该是心跳收集的时候，check 了这个表的 sink.IsEmpty 空的时候，改成 removed 状态，然后把这些剩下的删光
func (s *MysqlSink) StopTableSpan(tableSpan *common.TableSpan) {
	s.mutex.Lock()
	tableStatus, ok := s.tableStatuses[tableSpan]
	if !ok {
		log.Error("unknown Span for Mysql Sink: ", zap.Any("tableSpan", tableSpan))
		s.mutex.Unlock()
		return
	}
	tableStatus.getTask().Cancel()
	delete(s.tableStatuses, tableSpan)
	s.mutex.Unlock()
}

// Called when the MysqlSink.Empty(tableSpan) to be true, and remove the tableSpan from Sink now.
func (s *MysqlSink) RemoveTableSpan(tableSpan *common.TableSpan) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.tableStatuses, tableSpan)
}

func (s *MysqlSink) IsEmpty(tableSpan *common.TableSpan) bool {
	s.mutex.RLock()
	tableStatus, ok := s.tableStatuses[tableSpan]
	s.mutex.RUnlock()
	if !ok {
		log.Error("unknown Span for Mysql Sink: ", zap.Any("tableSpan", tableSpan))
		return false
	}
	return tableStatus.getProgress().Empty()
}

func (s *MysqlSink) GetSmallestCommitTs(tableSpan *common.TableSpan) uint64 {
	s.mutex.RLock()
	tableStatus, ok := s.tableStatuses[tableSpan]
	s.mutex.RUnlock()

	if !ok {
		log.Error("unknown Span for Mysql Sink: ", zap.Any("tableSpan", tableSpan))
		return math.MaxUint64
	}

	return tableStatus.getProgress().SmallestCommitTs()
}
