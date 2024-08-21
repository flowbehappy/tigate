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
	"sync"
	"time"

	"github.com/flowbehappy/tigate/downstreamadapter/sink/types"
	"github.com/flowbehappy/tigate/downstreamadapter/worker"
	"github.com/flowbehappy/tigate/downstreamadapter/writer"
	"github.com/flowbehappy/tigate/pkg/common"
	"go.uber.org/zap"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
)

const (
	// DefaultConflictDetectorSlots indicates the default slot count of conflict detector. TODO:check this
	DefaultConflictDetectorSlots uint64 = 16 * 1024
)

// mysql sink 负责 mysql 类型下游的 sink 模块
// 一个 event dispatcher manager 对应一个 mysqlSink
// 实现 Sink 的接口
type MysqlSink struct {
	changefeedID model.ChangeFeedID

	ddlWorker *worker.MysqlDDLWorker

	wg     sync.WaitGroup
	cancel context.CancelFunc

	eventChans   []chan *common.TxnEvent
	ddlEventChan chan *common.TxnEvent
	workerCount  int
}

// event dispatcher manager 初始化的时候创建 mysqlSink 对象
func NewMysqlSink(changefeedID model.ChangeFeedID, workerCount int, cfg *writer.MysqlConfig, db *sql.DB) *MysqlSink {
	mysqlSink := MysqlSink{
		changefeedID: changefeedID,
		eventChans:   make([]chan *common.TxnEvent, workerCount),
		ddlEventChan: make(chan *common.TxnEvent, 16),
		workerCount:  workerCount,
	}

	for i := 0; i < workerCount; i++ {
		mysqlSink.eventChans[i] = make(chan *common.TxnEvent, 16)
	}

	mysqlSink.initWorker(workerCount, cfg, db)

	return &mysqlSink
}

func (s *MysqlSink) initWorker(workerCount int, cfg *writer.MysqlConfig, db *sql.DB) {
	ctx, cancel := context.WithCancel(context.Background())
	s.ddlWorker = worker.NewMysqlDDLWorker(db, cfg, s.changefeedID)

	s.cancel = cancel
	for i := 0; i < workerCount; i++ {
		s.wg.Add(1)
		workerId := i
		go func(ctx context.Context, eventChan chan *common.TxnEvent, db *sql.DB, config *writer.MysqlConfig, maxRows int) {
			defer s.wg.Done()
			totalStart := time.Now()
			worker := worker.NewMysqlWorker(eventChan, db, config, workerId, s.changefeedID)
			events := make([]*common.TxnEvent, 0)
			rows := 0
			for {
				needFlush := false
				select {
				case <-ctx.Done():
					return
				case txnEvent := <-worker.GetEventChan():
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
								worker.MetricWorkerHandledRows.Add(float64(len(txnEvent.Rows)))
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
					start := time.Now()
					err := worker.GetMysqlWriter().Flush(events, workerId)
					if err != nil {
						log.Error("Failed to flush events", zap.Error(err), zap.Any("workerID", workerId), zap.Any("events", events))
						return
					}
					worker.MetricWorkerFlushDuration.Observe(time.Since(start).Seconds())
					// we record total time to calcuate the worker busy ratio.
					// so we record the total time after flushing, to unified statistics on
					// flush time and total time
					worker.MetricWorkerTotalDuration.Observe(time.Since(totalStart).Seconds())
					totalStart = time.Now()
					log.Info("Flush events", zap.Int("count", len(events)), zap.Int("rows", rows), zap.Duration("duration", time.Since(start)), zap.Any("workerID", workerId))

					events = events[:0]
					rows = 0
				}
			}
		}(ctx, s.eventChans[workerId], db, cfg, 256)
	}

	// ddl flush goroutine
	s.wg.Add(1)
	go func(ctx context.Context, ddleEventChan chan *common.TxnEvent) {
		defer s.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-ddleEventChan:
				s.ddlWorker.GetMysqlWriter().FlushDDLEvent(event)
			}
		}
	}(ctx, s.ddlEventChan)
}

func (s *MysqlSink) AddDMLEvent(event *common.TxnEvent, tableProgress *types.TableProgress) {
	if len(event.GetRows()) == 0 {
		return
	}

	tableProgress.Add(event)

	// TODO:后续再优化这里的逻辑，目前有个问题是 physical table id 好像都是偶数？这个后面改个能见人的方法
	index := event.GetRows()[0].PhysicalTableID % int64(s.workerCount)
	s.eventChans[index] <- event
}

func (s *MysqlSink) PassDDLAndSyncPointEvent(event *common.TxnEvent, tableProgress *types.TableProgress) {
	tableProgress.Pass(event)
}

func (s *MysqlSink) AddDDLAndSyncPointEvent(event *common.TxnEvent, tableProgress *types.TableProgress) {
	// TODO:这个 ddl 可以并发写么？如果不行的话，后面还要加锁或者排队
	tableProgress.Add(event)
	//s.ddlWorker.GetMysqlWriter().FlushDDLEvent(event)
	s.ddlEventChan <- event
}

func (s *MysqlSink) Close() {
	s.cancel()
	s.wg.Wait()
}
