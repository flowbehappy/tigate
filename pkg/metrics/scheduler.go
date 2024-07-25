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

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ScheduleTaskGuage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "task",
			Help:      "The total number of scheduler tasks",
		}, []string{"namespace", "changefeed"})

	TableGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "table",
			Help:      "The total number of tables",
		}, []string{"namespace", "changefeed"})
	TableStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "table_replication_state",
			Help:      "The total number of tables in different replication states",
		}, []string{"namespace", "changefeed", "state"})
	AcceptScheduleTaskCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "task_accept",
			Help:      "The total number of accepted scheduler tasks",
		}, []string{"namespace", "changefeed", "task"})
	RunningScheduleTaskGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "task_running",
			Help:      "The total number of running scheduler tasks",
		}, []string{"namespace", "changefeed"})
	SlowestTableIDGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_id",
			Help:      "The table ID of the slowest table",
		}, []string{"namespace", "changefeed"})
	SlowestTableCheckpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_checkpoint_ts",
			Help:      "The checkpoint ts of the slowest table",
		}, []string{"namespace", "changefeed"})
	SlowestTableResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_resolved_ts",
			Help:      "The resolved ts of the slowest table",
		}, []string{"namespace", "changefeed"})
	SlowestTableStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_replication_state",
			Help:      "The replication state of the slowest table",
		}, []string{"namespace", "changefeed"})
	SlowestTableStageCheckpointTsGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_checkpoint_ts",
			Help:      "Checkpoint ts of each stage of the slowest table",
		}, []string{"namespace", "changefeed", "stage"})
	SlowestTableStageResolvedTsGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_resolved_ts",
			Help:      "Resolved ts of each stage of the slowest table",
		}, []string{"namespace", "changefeed", "stage"})
	SlowestTableStageCheckpointTsLagGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_checkpoint_ts_lag",
			Help:      "Checkpoint ts lag of each stage of the slowest table",
		}, []string{"namespace", "changefeed", "stage"})
	SlowestTableStageResolvedTsLagGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_resolved_ts_lag",
			Help:      "Resolved ts lag of each stage of the slowest table",
		}, []string{"namespace", "changefeed", "stage"})
	SlowestTableStageCheckpointTsLagHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_checkpoint_ts_lag_histogram",
			Help:      "Histogram of the slowest table checkpoint ts lag of each stage",
			Buckets:   prometheus.LinearBuckets(0.5, 0.5, 36),
		}, []string{"namespace", "changefeed", "stage"})
	SlowestTableStageResolvedTsLagHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_resolved_ts_lag_histogram",
			Help:      "Histogram of the slowest table resolved ts lag of each stage",
			Buckets:   prometheus.LinearBuckets(0.5, 0.5, 36),
		}, []string{"namespace", "changefeed", "stage"})
	SlowestTableRegionGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_region_count",
			Help:      "The number of regions captured by the slowest table",
		}, []string{"namespace", "changefeed"})

	SlowestTablePullerResolvedTs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_puller_resolved_ts",
			Help:      "Puller Slowest ResolvedTs",
		}, []string{"namespace", "changefeed"})
	SlowestTablePullerResolvedTsLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_puller_resolved_ts_lag",
			Help:      "Puller Slowest ResolvedTs lag",
		}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics used in scheduler
func InitSchedulerMetrics(registry *prometheus.Registry) {
	registry.MustRegister(ScheduleTaskGuage)

	registry.MustRegister(TableGauge)
	registry.MustRegister(TableStateGauge)
	registry.MustRegister(AcceptScheduleTaskCounter)
	registry.MustRegister(RunningScheduleTaskGauge)

	registry.MustRegister(SlowestTableIDGauge)
	registry.MustRegister(SlowestTableCheckpointTsGauge)
	registry.MustRegister(SlowestTableResolvedTsGauge)
	registry.MustRegister(SlowestTableStateGauge)
	registry.MustRegister(SlowestTableStageCheckpointTsGaugeVec)
	registry.MustRegister(SlowestTableStageResolvedTsGaugeVec)
	registry.MustRegister(SlowestTableStageCheckpointTsLagGaugeVec)
	registry.MustRegister(SlowestTableStageResolvedTsLagGaugeVec)
	registry.MustRegister(SlowestTableStageCheckpointTsLagHistogramVec)
	registry.MustRegister(SlowestTableStageResolvedTsLagHistogramVec)
	registry.MustRegister(SlowestTableRegionGaugeVec)

	registry.MustRegister(SlowestTablePullerResolvedTs)
	registry.MustRegister(SlowestTablePullerResolvedTsLag)
}
