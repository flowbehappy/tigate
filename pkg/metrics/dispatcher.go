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
	// EventDispatcherManagerGauge is the metrics collector related to dispatcher manager.
	EventDispatcherManagerGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tigate",
			Subsystem: "dispatchermanagermanager",
			Name:      "event_dispatcher_manager_count",
			Help:      "The number of event dispatcher managers",
		}, []string{"namespace", "changefeed"})

	// TableEventDispatcherGauge is the metrics collector related to dispatcher manager.
	TableEventDispatcherGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tigate",
			Subsystem: "dispatchermanager",
			Name:      "table_event_dispatcher_count",
			Help:      "The number of table event dispatchers",
		}, []string{"namespace", "changefeed"})

	CreateDispatcherDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "create_dispatcher_duration",
			Help:      "Bucketed histogram of create dispatcher time (s) for table span.",
			Buckets:   prometheus.ExponentialBuckets(0.000001, 2, 20), // 1us~524ms
		}, []string{"namespace", "changefeed"})

	EventDispatcherManagerResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "event_dispatcher_manager_resolved_ts",
			Help:      "Resolved ts of event dispatcher manager(changefeed)",
		}, []string{"namespace", "changefeed"})

	EventDispatcherManagerCheckpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "event_dispatcher_manager_checkpoint_ts",
			Help:      "Checkpoint ts of event dispatcher manager(changefeed)",
		}, []string{"namespace", "changefeed"})

	HandleDispatcherRequsetCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "handle_dispatcher_request",
			Help:      "Total count of dispatcher request.",
		}, []string{"namespace", "changefeed", "type"})

	DispatcherReceivedEventCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "dispatcher",
		Name:      "received_event_count",
		Help:      "The number of events received by the dispatcher",
	}, []string{"type"})

	EventCollectorRegisteredDispatcherCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "dispatcher",
		Name:      "event_collector_registered_dispatcher_count",
		Help:      "The number of registered dispatchers in the event collector",
	})
)

func InitDisaptcherMetrics(registry *prometheus.Registry) {
	registry.MustRegister(EventDispatcherManagerGauge)
	registry.MustRegister(TableEventDispatcherGauge)
	registry.MustRegister(CreateDispatcherDuration)
	registry.MustRegister(EventDispatcherManagerResolvedTsGauge)
	registry.MustRegister(EventDispatcherManagerCheckpointTsGauge)
	registry.MustRegister(HandleDispatcherRequsetCounter)
	registry.MustRegister(DispatcherReceivedEventCount)
	registry.MustRegister(EventCollectorRegisteredDispatcherCount)

	// for test
}
