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
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	grpcMetrics = grpc_prometheus.NewClientMetrics()

	EventStoreReceivedEventCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "log_service",
			Name:      "puller_event_count",
			Help:      "The number of events received by event store.",
		}, []string{"type"}) // types : kv, resolved.

	EventFeedErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "event_feed_error_count",
			Help:      "The number of error return by tikv",
		}, []string{"type"})

	eventSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "event_size_bytes",
			Help:      "Size of KV events.",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 25),
		}, []string{"type"})
	pullEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "pull_event_count",
			Help:      "event count received by this puller",
		}, []string{"type", "namespace", "changefeed"})
	clientChannelSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "channel_size",
			Help:      "size of each channel in kv client",
		}, []string{"channel"})
	BatchResolvedEventSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "batch_resolved_event_size",
			Help:      "The number of region in one batch resolved ts event",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
		}, []string{"type"})
	LockResolveDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "lock_resolve_duration",
			Help:      "time of lock resolve in ms",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
		},
		// actions: wait, run.
		[]string{"type", "action"})

	regionWorkerQueueDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "region_worker_queue_duration",
			Help:      "time of queue in region worker",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
		},
		// actions: wait, run.
		[]string{"namespace", "changefeed"})

	RegionWorkerProcessDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "worker_process_duration",
			Help:      "Process duration (s) for changeEventProcessor worker.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{"type", "id"})

	RegionWorkerTotalDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "worker_total_duration",
			Help:      "total duration (s) for changeEventProcessor worker.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20), // 1ms~524s
		}, []string{"type", "id"})
)

func GetGlobalGrpcMetrics() *grpc_prometheus.ClientMetrics {
	return grpcMetrics
}

func InitPullerMetrics(registry *prometheus.Registry) {
	registry.MustRegister(grpcMetrics)
	registry.MustRegister(RegionWorkerProcessDuration)
	registry.MustRegister(RegionWorkerTotalDuration)
	registry.MustRegister(EventStoreReceivedEventCount)
	registry.MustRegister(EventFeedErrorCounter)
	registry.MustRegister(BatchResolvedEventSize)

	registry.MustRegister(eventSize)
	registry.MustRegister(pullEventCounter)
	registry.MustRegister(clientChannelSize)
	registry.MustRegister(LockResolveDuration)
	registry.MustRegister(regionWorkerQueueDuration)
}
