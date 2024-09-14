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

package worker

import (
	"context"
	"database/sql"
	"strconv"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/downstreamadapter/writer"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// MysqlWorker is use to flush the event downstream
type MysqlWorker struct {
	eventChan   chan *common.DMLEvent // 获取到能往下游写的 events
	mysqlWriter *writer.MysqlWriter   // 实际负责做 flush 操作
	id          int
	// Metrics.
	MetricConflictDetectDuration prometheus.Observer
	MetricQueueDuration          prometheus.Observer
	MetricWorkerFlushDuration    prometheus.Observer
	MetricWorkerTotalDuration    prometheus.Observer
	// MetricWorkerHandledRows is the number of rows this worker received from conflict detector.
	MetricWorkerHandledRows prometheus.Counter

	wg      sync.WaitGroup
	maxRows int
}

func NewMysqlWorker(db *sql.DB, config *writer.MysqlConfig, id int, changefeedID model.ChangeFeedID, ctx context.Context, maxRows int) *MysqlWorker {
	wid := strconv.Itoa(id)

	worker := &MysqlWorker{
		mysqlWriter: writer.NewMysqlWriter(db, config, changefeedID),
		id:          id,
		maxRows:     maxRows,
		eventChan:   make(chan *common.DMLEvent, 16),

		MetricConflictDetectDuration: metrics.ConflictDetectDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		MetricQueueDuration:          metrics.QueueDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		MetricWorkerFlushDuration:    metrics.WorkerFlushDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID, wid),
		MetricWorkerTotalDuration:    metrics.WorkerTotalDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID, wid),
		MetricWorkerHandledRows:      metrics.WorkerHandledRows.WithLabelValues(changefeedID.Namespace, changefeedID.ID, wid),
	}
	worker.wg.Add(1)
	go worker.Run(ctx)

	return worker
}

func (t *MysqlWorker) GetEventChan() chan *common.DMLEvent {
	return t.eventChan
}

func (t *MysqlWorker) Run(ctx context.Context) {
	defer t.wg.Done()
	totalStart := time.Now()

	events := make([]*common.DMLEvent, 0)
	rows := 0
	for {
		needFlush := false
		select {
		case <-ctx.Done():
			return
		case txnEvent := <-t.eventChan:
			events = append(events, txnEvent)
			rows += txnEvent.Len()
			if rows > t.maxRows {
				needFlush = true
			}
			if !needFlush {
				delay := time.NewTimer(10 * time.Millisecond)
				for !needFlush {
					select {
					case txnEvent := <-t.eventChan:
						t.MetricWorkerHandledRows.Add(float64(txnEvent.Len()))
						events = append(events, txnEvent)
						rows += txnEvent.Len()
						if rows > t.maxRows {
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
			err := t.mysqlWriter.Flush(events, t.id)
			if err != nil {
				log.Error("Failed to flush events", zap.Error(err), zap.Any("workerID", t.id), zap.Any("events", events))
				return
			}
			t.MetricWorkerFlushDuration.Observe(time.Since(start).Seconds())
			// we record total time to calcuate the worker busy ratio.
			// so we record the total time after flushing, to unified statistics on
			// flush time and total time
			t.MetricWorkerTotalDuration.Observe(time.Since(totalStart).Seconds())
			totalStart = time.Now()
			log.Info("Flush events", zap.Int("count", len(events)), zap.Int("rows", rows), zap.Duration("duration", time.Since(start)), zap.Any("workerID", t.id))

			events = events[:0]
			rows = 0
		}
	}
}

type MysqlDDLWorker struct {
	mysqlWriter  *writer.MysqlWriter
	ddlEventChan chan *common.DDLEvent
	wg           sync.WaitGroup
}

func NewMysqlDDLWorker(db *sql.DB, config *writer.MysqlConfig, changefeedID model.ChangeFeedID, ctx context.Context) *MysqlDDLWorker {
	worker := &MysqlDDLWorker{
		mysqlWriter:  writer.NewMysqlWriter(db, config, changefeedID),
		ddlEventChan: make(chan *common.DDLEvent, 16),
	}
	worker.wg.Add(1)
	go worker.Run(ctx)
	return worker
}

func (t *MysqlDDLWorker) Run(ctx context.Context) {
	defer t.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-t.ddlEventChan:
			t.mysqlWriter.FlushDDLEvent(event)
		}
	}
}

func (t *MysqlDDLWorker) GetDDLEventChan() chan *common.DDLEvent {
	return t.ddlEventChan
}
