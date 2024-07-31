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

package metrics

import "github.com/prometheus/client_golang/prometheus"

// LargeRowSizeLowBound is set to 2K, only track data event with size not smaller than it.
const LargeRowSizeLowBound = 2 * 1024

// ---------- Metrics used in Statistics. ---------- //
var (
	// ExecBatchHistogram records batch size of a txn.
	ExecBatchHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "batch_row_count",
			Help:      "Row count number for a given batch.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		}, []string{"namespace", "changefeed", "type"}) // type is for `sinkType`

	// ExecWriteBytesGauge records the total number of bytes written by sink.
	TotalWriteBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "write_bytes_total",
			Help:      "Total number of bytes written by sink",
		}, []string{"namespace", "changefeed", "type"}) // type is for `sinkType`

	// LargeRowSizeHistogram records size of large rows.
	LargeRowSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "large_row_size",
			Help:      "The size of all received row changed events (in bytes).",
			Buckets:   prometheus.ExponentialBuckets(LargeRowSizeLowBound, 2, 15), // 2K~32M
		}, []string{"namespace", "changefeed", "type"}) // type is for `sinkType`

	// ExecDDLHistogram records the exexution time of a DDL.
	ExecDDLHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "ddl_exec_duration",
			Help:      "Bucketed histogram of processing time (s) of a ddl.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 18),
		}, []string{"namespace", "changefeed", "type"}) // type is for `sinkType`

	// ExecutionErrorCounter is the counter of execution errors.
	ExecutionErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "execution_error",
			Help:      "Total count of execution errors.",
		}, []string{"namespace", "changefeed", "type"}) // type is for `sinkType`

	CreateDispatcherDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "create_dispatcher_duration",
			Help:      "Bucketed histogram of create dispatcher time (s) for table span.",
			Buckets:   prometheus.ExponentialBuckets(0.000001, 2, 20), // 1us~524ms
		}, []string{"namespace", "changefeed"})

	HandleDispatcherRequsetCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "handle_dispatcher_request",
			Help:      "Total count of dispatcher request.",
		}, []string{"namespace", "changefeed", "type"})
)

// ---------- Metrics for txn sink and backends. ---------- //
var (
	// ConflictDetectDuration records the duration of detecting conflict.
	ConflictDetectDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_conflict_detect_duration",
			Help:      "Bucketed histogram of conflict detect time (s) for single DML statement.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{"namespace", "changefeed"})

	// QueueDuration = ConflictDetectDuration + (queue time in txn workers).
	QueueDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_queue_duration",
			Help:      "Bucketed histogram of queue time (s) for single DML statement.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{"namespace", "changefeed"})

	WorkerFlushDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_worker_flush_duration",
			Help:      "Flush duration (s) for txn worker.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{"namespace", "changefeed", "id"})

	WorkerTotalDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_worker_total_duration",
			Help:      "total duration (s) for txn worker.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20), // 1ms~524s
		}, []string{"namespace", "changefeed", "id"})

	WorkerHandledRows = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_worker_handled_rows",
			Help:      "Busy ratio (X ms in 1s) for all workers.",
		}, []string{"namespace", "changefeed", "id"})

	SinkDMLBatchCommit = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_sink_dml_batch_commit",
			Help:      "Duration of committing a DML batch",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 18), // 10ms~1310s
		}, []string{"namespace", "changefeed"})

	SinkDMLBatchCallback = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_sink_dml_batch_callback",
			Help:      "Duration of execuing a batch of callbacks",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 18), // 10ms~1300s
		}, []string{"namespace", "changefeed"})

	PrepareStatementErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_prepare_statement_errors",
			Help:      "Prepare statement errors",
		}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics in this file.
func InitSinkMetrics(registry *prometheus.Registry) {
	// common sink metrics
	registry.MustRegister(ExecBatchHistogram)
	registry.MustRegister(TotalWriteBytesCounter)
	registry.MustRegister(ExecDDLHistogram)
	registry.MustRegister(LargeRowSizeHistogram)
	registry.MustRegister(ExecutionErrorCounter)
	registry.MustRegister(CreateDispatcherDuration)
	registry.MustRegister(HandleDispatcherRequsetCounter)

	// txn sink metrics
	registry.MustRegister(ConflictDetectDuration)
	registry.MustRegister(QueueDuration)
	registry.MustRegister(WorkerFlushDuration)
	registry.MustRegister(WorkerTotalDuration)
	registry.MustRegister(WorkerHandledRows)
	registry.MustRegister(SinkDMLBatchCommit)
	registry.MustRegister(SinkDMLBatchCallback)
	registry.MustRegister(PrepareStatementErrors)
}
