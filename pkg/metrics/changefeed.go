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
	ChangefeedBarrierTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "barrier_ts",
			Help:      "barrier ts of changefeeds",
		}, []string{"namespace", "changefeed"})

	ChangefeedCheckpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "checkpoint_ts",
			Help:      "checkpoint ts of changefeeds",
		}, []string{"namespace", "changefeed"})

	ChangefeedCheckpointTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "checkpoint_ts_lag",
			Help:      "checkpoint ts lag of changefeeds in seconds",
		}, []string{"namespace", "changefeed"})

	CurrentPDTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "current_pd_ts",
			Help:      "The current PD ts",
		}, []string{"namespace", "changefeed"})

	ChangefeedResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "resolved_ts",
			Help:      "resolved ts of changefeeds",
		}, []string{"namespace", "changefeed"})
	ChangefeedResolvedTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "resolved_ts_lag",
			Help:      "resolved ts lag of changefeeds in seconds",
		}, []string{"namespace", "changefeed"})

	CoordinatorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "ownership_counter",
			Help:      "The counter of ownership increases every 5 seconds on a owner capture",
		})

	MaintainerGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "changefeed",
			Name:      "maintainer_counter",
			Help:      "The counter of changefeed maintainer",
		}, []string{"namespace", "changefeed"})

	HandleMaintainerRequsetCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "changefeed",
			Name:      "handle_maintainer_request",
			Help:      "Total count of dispatcher request.",
		}, []string{"namespace", "changefeed", "type"})

	ChangefeedStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "status",
			Help:      "The status of changefeeds",
		}, []string{"namespace", "changefeed"})

	ChangefeedTickDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "changefeed_tick_duration",
			Help:      "Bucketed histogram of owner tick changefeed reactor time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.01 /* 10 ms */, 2, 18),
		}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics used in owner
func InitChangefeedMetrics(registry *prometheus.Registry) {
	registry.MustRegister(ChangefeedBarrierTsGauge)
	registry.MustRegister(ChangefeedCheckpointTsGauge)
	registry.MustRegister(ChangefeedCheckpointTsLagGauge)
	registry.MustRegister(ChangefeedResolvedTsGauge)
	registry.MustRegister(ChangefeedResolvedTsLagGauge)
	registry.MustRegister(CurrentPDTsGauge)
	registry.MustRegister(CoordinatorCounter)
	registry.MustRegister(MaintainerGauge)
	registry.MustRegister(HandleMaintainerRequsetCounter)
	registry.MustRegister(ChangefeedStatusGauge)
	registry.MustRegister(ChangefeedTickDuration)
}
