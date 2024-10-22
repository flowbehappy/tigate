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

var (
	CoordinatorCreatedOperatorCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "coordinator",
			Name:      "operator_created_count",
			Help:      "number of created operators",
		}, []string{"type"})

	CoordinatorFinishedOperatorCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "coordinator",
			Name:      "operator_finished_count",
			Help:      "number of finished operators",
		}, []string{"type"})

	CoordinatorOperatorDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "coordinator",
			Name:      "finish_operators_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of finished operator.",
			Buckets:   []float64{0.5, 1, 2, 4, 8, 16, 20, 40, 60, 90, 120, 180, 240, 300, 480, 600, 720, 900, 1200, 1800, 3600},
		}, []string{"type"})

	ChangefeedStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "coordinator",
			Name:      "changefeed_state",
			Help:      "The total number of changefeed in different replication states",
		}, []string{"state"})
)

func InitCoordinatorMetrics(registry *prometheus.Registry) {
	registry.MustRegister(CoordinatorCreatedOperatorCount)
	registry.MustRegister(CoordinatorFinishedOperatorCount)
	registry.MustRegister(CoordinatorOperatorDuration)
	registry.MustRegister(ChangefeedStateGauge)
}
