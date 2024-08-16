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
	"database/sql"
	"fmt"

	"github.com/flowbehappy/tigate/downstreamadapter/writer"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/prometheus/client_golang/prometheus"
)

// MysqlWorker is use to flush the event downstream
type MysqlWorker struct {
	eventChan   <-chan *common.TxnEvent // 获取到能往下游写的 events
	mysqlWriter *writer.MysqlWriter     // 实际负责做 flush 操作
	id          int
	// Metrics.
	MetricConflictDetectDuration prometheus.Observer
	MetricQueueDuration          prometheus.Observer
	MetricWorkerFlushDuration    prometheus.Observer
	MetricWorkerTotalDuration    prometheus.Observer
	// MetricWorkerHandledRows is the number of rows this worker received from conflict detector.
	MetricWorkerHandledRows prometheus.Counter
}

func NewMysqlWorker(eventChan <-chan *common.TxnEvent, db *sql.DB, config *writer.MysqlConfig, id int, changefeedID model.ChangeFeedID) *MysqlWorker {
	wid := fmt.Sprintf("%d", id)

	return &MysqlWorker{
		eventChan:   eventChan,
		mysqlWriter: writer.NewMysqlWriter(db, config, changefeedID),
		id:          id,

		MetricConflictDetectDuration: metrics.ConflictDetectDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		MetricQueueDuration:          metrics.QueueDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		MetricWorkerFlushDuration:    metrics.WorkerFlushDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID, wid),
		MetricWorkerTotalDuration:    metrics.WorkerTotalDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID, wid),
		MetricWorkerHandledRows:      metrics.WorkerHandledRows.WithLabelValues(changefeedID.Namespace, changefeedID.ID, wid),
	}
}

func (t *MysqlWorker) GetEventChan() <-chan *common.TxnEvent {
	return t.eventChan
}

func (t *MysqlWorker) GetMysqlWriter() *writer.MysqlWriter {
	return t.mysqlWriter
}

func (t *MysqlWorker) GetID() int {
	return t.id
}

type MysqlDDLWorker struct {
	mysqlWriter *writer.MysqlWriter
}

func NewMysqlDDLWorker(db *sql.DB, config *writer.MysqlConfig, changefeedID model.ChangeFeedID) *MysqlDDLWorker {
	return &MysqlDDLWorker{
		mysqlWriter: writer.NewMysqlWriter(db, config, changefeedID),
	}
}

func (t *MysqlDDLWorker) GetMysqlWriter() *writer.MysqlWriter {
	return t.mysqlWriter
}
