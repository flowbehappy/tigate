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
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// MysqlWorker is use to flush the event downstream
type MysqlWorker struct {
	eventChan    chan *commonEvent.DMLEvent
	mysqlWriter  *writer.MysqlWriter
	id           int
	changefeedID model.ChangeFeedID

	wg      sync.WaitGroup
	maxRows int
}

func NewMysqlWorker(db *sql.DB, config *writer.MysqlConfig, id int, changefeedID model.ChangeFeedID, ctx context.Context, maxRows int) *MysqlWorker {
	worker := &MysqlWorker{
		mysqlWriter:  writer.NewMysqlWriter(db, config, changefeedID),
		id:           id,
		maxRows:      maxRows,
		eventChan:    make(chan *commonEvent.DMLEvent, 16),
		changefeedID: changefeedID,
	}
	worker.wg.Add(1)
	go worker.Run(ctx)

	return worker
}

func (t *MysqlWorker) GetEventChan() chan *commonEvent.DMLEvent {
	return t.eventChan
}

func (t *MysqlWorker) Run(ctx context.Context) {
	defer t.wg.Done()
	workerFlushDuration := metrics.WorkerFlushDuration.WithLabelValues(t.changefeedID.Namespace, t.changefeedID.ID, strconv.Itoa(t.id))
	workerTotalDuration := metrics.WorkerTotalDuration.WithLabelValues(t.changefeedID.Namespace, t.changefeedID.ID, strconv.Itoa(t.id))
	workerHandledRows := metrics.WorkerHandledRows.WithLabelValues(t.changefeedID.Namespace, t.changefeedID.ID, strconv.Itoa(t.id))

	defer func() {
		metrics.WorkerFlushDuration.DeleteLabelValues(t.changefeedID.Namespace, t.changefeedID.ID, strconv.Itoa(t.id))
		metrics.WorkerTotalDuration.DeleteLabelValues(t.changefeedID.Namespace, t.changefeedID.ID, strconv.Itoa(t.id))
		metrics.WorkerHandledRows.DeleteLabelValues(t.changefeedID.Namespace, t.changefeedID.ID, strconv.Itoa(t.id))
	}()

	totalStart := time.Now()

	events := make([]*commonEvent.DMLEvent, 0)
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
						workerHandledRows.Add(float64(txnEvent.Len()))
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
			workerFlushDuration.Observe(time.Since(start).Seconds())
			// we record total time to calcuate the worker busy ratio.
			// so we record the total time after flushing, to unified statistics on
			// flush time and total time
			workerTotalDuration.Observe(time.Since(totalStart).Seconds())
			totalStart = time.Now()
			log.Info("Flush events", zap.Int("count", len(events)), zap.Int("rows", rows), zap.Duration("duration", time.Since(start)), zap.Any("workerID", t.id))

			events = events[:0]
			rows = 0
		}
	}
}

// MysqlDDLWorker is use to flush the ddl event and sync point eventdownstream
type MysqlDDLWorker struct {
	mysqlWriter  *writer.MysqlWriter
	ddlEventChan chan commonEvent.BlockEvent
	wg           sync.WaitGroup
}

func NewMysqlDDLWorker(db *sql.DB, config *writer.MysqlConfig, changefeedID model.ChangeFeedID, ctx context.Context) *MysqlDDLWorker {
	worker := &MysqlDDLWorker{
		mysqlWriter:  writer.NewMysqlWriter(db, config, changefeedID),
		ddlEventChan: make(chan commonEvent.BlockEvent, 16),
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
			switch event.(type) {
			case *commonEvent.DDLEvent:
				t.mysqlWriter.FlushDDLEvent(event.(*commonEvent.DDLEvent))
			case *commonEvent.SyncPointEvent:
				err := t.mysqlWriter.FlushSyncPointEvent(event.(*commonEvent.SyncPointEvent))
				if err != nil {
					log.Error("Failed to flush sync point event", zap.Error(err), zap.Any("event", event))
				}
			default:
				log.Error("unknown event type", zap.Any("event", event))
			}
		}
	}
}

func (t *MysqlDDLWorker) GetDDLEventChan() chan commonEvent.BlockEvent {
	return t.ddlEventChan
}
