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
	EventStoreSubscriptionGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "subscription_num",
			Help:      "The number of subscriptions in event store",
		})

	EventStoreReceivedEventCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "input_event_count",
			Help:      "The number of events received by event store.",
		}, []string{"type"}) // types : kv, resolved.

	EventStoreWriteBytes = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "write_bytes",
			Help:      "The number of bytes written by event store.",
		})

	EventStoreWriteDurationHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "write_duration",
		Help:      "Bucketed histogram of event store write duration",
		Buckets:   prometheus.ExponentialBuckets(0.004, 2.0, 10),
	})

	EventStoreScanRequestsCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "scan_requests_count",
			Help:      "The number of scan requests received by event store.",
		})

	EventStoreScanBytes = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "scan_bytes",
			Help:      "The number of bytes scanned by event store.",
		})

	EventStoreDeleteRangeCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "delete_range_count",
			Help:      "The number of delete range received by event store.",
		})

	EventStoreDispatcherResolvedTsLagHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "dispatcher_resolved_ts_lag",
			Help:      "Resolved Ts lag histogram of registered dispatchers for event store.",
			Buckets:   LagBucket(),
		})

	EventStoreResolvedTsLagGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "resolved_ts_lag",
			Help:      "The resolved ts lag of event store.",
		})

	EventStoreDispatcherWatermarkLagHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "dispatcher_watermark_lag",
			Help:      "Watermark lag histogram of registered dispatchers for event store.",
			Buckets:   LagBucket(),
		})

	EventStoreCompressRatio = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "compress_ratio",
			Help:      "The compression ratio of the event data.",
		})

	EventStoreWriteBatchEventsCountHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "write_batch_events_count",
			Help:      "Batch event count histogram for write task pool.",
			Buckets:   prometheus.ExponentialBuckets(8, 2, 20),
		})

	EventStoreWriteBatchSizeHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "write_batch_size",
			Help:      "Batch event size histogram for write task pool.",
			Buckets:   prometheus.ExponentialBuckets(32, 2, 20),
		})

	EventStoreWriteRequestsCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "write_requests_count",
			Help:      "The number of write requests received by event store.",
		})

	EventStoreReadDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "read_duration",
		Help:      "Bucketed histogram of event store sorter iterator read duration",
		Buckets:   prometheus.ExponentialBuckets(0.004, 2.0, 20),
	}, []string{"type"})
)

func InitEventStoreMetrics(registry *prometheus.Registry) {
	registry.MustRegister(EventStoreSubscriptionGauge)
	registry.MustRegister(EventStoreReceivedEventCount)
	registry.MustRegister(EventStoreWriteBytes)
	registry.MustRegister(EventStoreWriteDurationHistogram)
	registry.MustRegister(EventStoreScanRequestsCount)
	registry.MustRegister(EventStoreScanBytes)
	registry.MustRegister(EventStoreDeleteRangeCount)
	registry.MustRegister(EventStoreDispatcherResolvedTsLagHist)
	registry.MustRegister(EventStoreResolvedTsLagGauge)
	registry.MustRegister(EventStoreDispatcherWatermarkLagHist)
	registry.MustRegister(EventStoreCompressRatio)
	registry.MustRegister(EventStoreWriteBatchEventsCountHist)
	registry.MustRegister(EventStoreWriteBatchSizeHist)
	registry.MustRegister(EventStoreWriteRequestsCount)
	registry.MustRegister(EventStoreReadDurationHistogram)
}
