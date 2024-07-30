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
	// SorterOutputEventCount is the metric that counts events output by the sorter.
	SorterOutputEventCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "output_event_count",
		Help:      "The number of events output by the sorter",
	}, []string{"namespace", "changefeed", "type"})

	// EventServiceSendEventCount is the metric that counts events sent by the event service.
	EventServiceSendEventCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "event_service",
		Name:      "send_event_count",
		Help:      "The number of events sent by the event service",
	}, []string{"namespace", "changefeed", "type"})

	// EventServiceSendEventDuration is the metric that records the duration of sending events by the event service.
	EventServiceSendEventDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "event_service",
		Name:      "send_event_duration",
		Help:      "The duration of sending events by the event service",
		Buckets:   prometheus.DefBuckets,
	}, []string{"type"})
)

// InitMetrics registers all metrics in this file.
func InitEventServiceMetrics(registry *prometheus.Registry) {
	registry.MustRegister(SorterOutputEventCount)
	registry.MustRegister(EventServiceSendEventCount)
	registry.MustRegister(EventServiceSendEventDuration)
}
