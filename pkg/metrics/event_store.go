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
	EventStoreDispatcherResolvedTsLagHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "dispatcher_resolved_ts_lag",
			Help:      "Resolved Ts lag histogram of registered dispatchers for event store.",
			Buckets:   LagBucket(),
		})

	EventStoreDispatcherWatermarkLagHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "dispatcher_watermark_lag",
			Help:      "Watermark lag histogram of registered dispatchers for event store.",
			Buckets:   LagBucket(),
		})
)

func InitEventStoreMetrics(registry *prometheus.Registry) {
	registry.MustRegister(EventStoreDispatcherResolvedTsLagHist)
	registry.MustRegister(EventStoreDispatcherWatermarkLagHist)
}
