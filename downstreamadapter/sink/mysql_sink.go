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
	"context"
	"database/sql"
	"math"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/downstreamadapter/sink/conflictdetector"
	"github.com/flowbehappy/tigate/downstreamadapter/sink/types"
	"github.com/flowbehappy/tigate/downstreamadapter/worker"
	"github.com/flowbehappy/tigate/downstreamadapter/writer"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/utils"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/causality"
)

const (
	// DefaultConflictDetectorSlots indicates the default slot count of conflict detector. TODO:check this
	DefaultConflictDetectorSlots uint64 = 16 * 1024
)

type TableStatusMap struct {
	mutex         sync.RWMutex
	tableStatuses *utils.BtreeMap[*common.TableSpan, TableStatus]
}

func NewTableStatusMap() *TableStatusMap {
	return &TableStatusMap{
		tableStatuses: utils.NewBtreeMap[*common.TableSpan, TableStatus](),
	}
}

func (m *TableStatusMap) Get(tableSpan *common.TableSpan) (TableStatus, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.tableStatuses.Get(tableSpan)
}

func (m *TableStatusMap) Set(tableSpan *common.TableSpan, tableStatus TableStatus) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.tableStatuses.ReplaceOrInsert(tableSpan, tableStatus)
}

func (m *TableStatusMap) Delete(tableSpan *common.TableSpan) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.tableStatuses.Delete(tableSpan)
}

type TableStatus struct {
	cancel context.CancelFunc
	ch     chan *common.TxnEvent
	//TableProgress 里面维护了目前正在 sink 中的 event ts 信息
	// // TableProgress 对外提供查询当前 table checkpointTs 的能力
	// // TableProgress 对外提供当前 table 是否有 event 在 sink 中等待被 flush 的能力--用于判断 ddl 是否达到下推条件
	progress *types.TableProgress
}

func (s *TableStatus) getCh() chan *common.TxnEvent {
	return s.ch
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
	changefeedID     model.ChangeFeedID
	conflictDetector *conflictdetector.ConflictDetector

	// 主要是要保持一样的生命周期？不然 channel 会对应不上
	//ddlWorker      *worker.MysqlDDLWorker
	//dmlWorkerTasks []*worker.MysqlWorkerDMLEventTask

	tableStatuses *TableStatusMap
	wg            sync.WaitGroup
	cancel        context.CancelFunc

	flushRows prometheus.Gauge
}

// event dispatcher manager 初始化的时候创建 mysqlSink 对象
func NewMysqlSink(changefeedID model.ChangeFeedID, workerCount int, cfg *writer.MysqlConfig, db *sql.DB) *MysqlSink {
	mysqlSink := MysqlSink{
		changefeedID: changefeedID,
		conflictDetector: conflictdetector.NewConflictDetector(DefaultConflictDetectorSlots, conflictdetector.TxnCacheOption{
			Count:         workerCount,
			Size:          1024,
			BlockStrategy: causality.BlockStrategyWaitEmpty,
		}),
		tableStatuses: NewTableStatusMap(),
		//dmlWorkerTasks: make([]*worker.MysqlWorkerDMLEventTask, workerCount),
	}

	mysqlSink.flushRows = FlushRows.WithLabelValues(mysqlSink.changefeedID.String())

	mysqlSink.initWorker(workerCount, cfg, db)

	return &mysqlSink
}

func (s *MysqlSink) initWorker(workerCount int, cfg *writer.MysqlConfig, db *sql.DB) {
	// init ddl worker, which is for ddl event and sync point event
	//s.ddlWorker = &worker.MysqlDDLWorker{MysqlWriter: writer.NewMysqlWriter(db, cfg)}

	// dml worker task will deal with all the dml events
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	for i := 0; i < workerCount; i++ {
		s.wg.Add(1)
		workerId := i
		// s.dmlWorkerTasks = append(s.dmlWorkerTasks, worker.NewMysqlWorkerDMLEventTask(s.conflictDetector.GetOutChByCacheID(int64(i)), db, cfg, 128))
		go func(ctx context.Context, eventChan <-chan *common.TxnEvent, db *sql.DB, config *writer.MysqlConfig, maxRows int) {
			defer s.wg.Done()
			worker := worker.NewMysqlWorker(eventChan, db, config, workerId, s.changefeedID)
			events := make([]*common.TxnEvent, 0)
			rows := 0
			for {
				needFlush := false
				select {
				case <-ctx.Done():
					return
				case txnEvent := <-worker.GetEventChan():
					log.Info("worker get event", zap.Any("workerID", workerId))
					events = append(events, txnEvent)
					rows += len(txnEvent.Rows)
					if rows > maxRows {
						needFlush = true
					}
					if !needFlush {
						delay := time.NewTimer(10 * time.Millisecond)
						for !needFlush {
							select {
							case txnEvent := <-worker.GetEventChan():
								events = append(events, txnEvent)
								rows += len(txnEvent.Rows)
								if rows > maxRows {
									needFlush = true
								}
							case <-delay.C:
								needFlush = true
							}
						}
						// Release resources promptly
						if !delay.Stop() {
							select {
							case <-delay.C:
							default:
							}
						}
					}
					//log.Info("Ready to Flush Events", zap.Int("count", len(events)), zap.Int("rows", rows), zap.Any("workerID", workerId))
					start := time.Now()
					err := worker.GetMysqlWriter().Flush(events)
					if err != nil {
						log.Error("Failed to flush events", zap.Error(err), zap.Any("workerID", workerId), zap.Any("events", events))
						return
					}
					s.flushRows.Add(float64(rows))
					worker.WorkerFlushDuration.Observe(time.Since(start).Seconds())

					//log.Info("Flush events", zap.Int("count", len(events)), zap.Int("rows", rows), zap.Duration("duration", time.Since(start)), zap.Any("workerID", workerId))

					events = events[:0]
					rows = 0
				}
			}
		}(ctx, s.conflictDetector.GetOutChByCacheID(int64(i)), db, cfg, 256)
	}
}

func (s *MysqlSink) AddDMLEvent(tableSpan *common.TableSpan, event *common.TxnEvent) {
	tableStatus, ok := s.tableStatuses.Get(tableSpan)
	if !ok {
		log.Error("unknown Span for Mysql Sink: ", zap.Any("tableSpan", tableSpan))
		return
	}
	//log.Info("mysql sink recv event", zap.Any("tableID", tableSpan.TableID))
	tableStatus.getProgress().Add(event)
	tableStatus.getCh() <- event
}

/*
func (s *MysqlSink) AddDDLAndSyncPointEvent(tableSpan *common.TableSpan, event *common.TxnEvent) { // 或许 ddl 也可以考虑有专用的 worker？
	tableStatus, ok := s.tableStatuses.Get(tableSpan)
	if !ok {
		log.Error("unknown Span for Mysql Sink: ", zap.Any("tableSpan", tableSpan))
		return
	}

	tableStatus.getProgress().Add(event)
	event.PostTxnFlushed = func() { tableStatus.getProgress().Remove(event) }
	task := worker.NewMysqlWorkerDDLEventTask(s.ddlWorker, event) // 先固定用 0 号 worker
	threadpool.GetTaskSchedulerInstance().WorkerTaskScheduler.Submit(task, threadpool.IOTask, time.Time{})

}*/

func (s *MysqlSink) AddTableSpan(tableSpan *common.TableSpan) {
	tableProgress := types.NewTableProgress()
	ch := make(chan *common.TxnEvent, 1024) // 先瞎拍
	ctx, cancel := context.WithCancel(context.Background())

	s.wg.Add(1)
	go func(ctx context.Context, tableProgress *types.TableProgress, eventCh chan *common.TxnEvent, conflictDetector *conflictdetector.ConflictDetector) {
		defer s.wg.Done()
		for {
			select {
			case event := <-eventCh:
				conflictDetector.Add(event, tableProgress)
			case <-ctx.Done():
				return
			}
		}
	}(ctx, tableProgress, ch, s.conflictDetector)

	s.tableStatuses.Set(tableSpan, TableStatus{
		cancel:   cancel,
		ch:       ch,
		progress: tableProgress,
	})
}

// Called when the dispatcher should be moved.
// We will cancel the sink task, and not move the tableStatus now.
// We need to wait the MysqlSink.Empty(tableSpan) to be true, which means all the events in the sink have been flushed.
// 应该是心跳收集的时候，check 了这个表的 sink.IsEmpty 空的时候，改成 removed 状态，然后把这些剩下的删光
func (s *MysqlSink) StopTableSpan(tableSpan *common.TableSpan) {
	tableStatus, ok := s.tableStatuses.Get(tableSpan)
	if !ok {
		log.Error("unknown Span for Mysql Sink: ", zap.Any("tableSpan", tableSpan))
		return
	}
	tableStatus.cancel()
}

// Called when the MysqlSink.Empty(tableSpan) to be true, and remove the tableSpan from Sink now.
func (s *MysqlSink) RemoveTableSpan(tableSpan *common.TableSpan) {
	s.tableStatuses.Delete(tableSpan)
}

func (s *MysqlSink) IsEmpty(tableSpan *common.TableSpan) bool {
	tableStatus, ok := s.tableStatuses.Get(tableSpan)
	if !ok {
		log.Error("unknown Span for Mysql Sink: ", zap.Any("tableSpan", tableSpan))
		return false
	}
	return tableStatus.getProgress().Empty()
}

func (s *MysqlSink) GetSmallestCommitTs(tableSpan *common.TableSpan) uint64 {
	tableStatus, ok := s.tableStatuses.Get(tableSpan)

	if !ok {
		log.Error("unknown Span for Mysql Sink: ", zap.Any("tableSpan", tableSpan))
		return math.MaxUint64
	}

	return tableStatus.getProgress().SmallestCommitTs()
}
