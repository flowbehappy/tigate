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
	DynamicStreamMemoryUsage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dynamic_stream",
			Name:      "memory_usage",
		}, []string{"type"})
	DynamicStreamEventChanSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dynamic_stream",
			Name:      "event_chan_size",
		}, []string{"component"})
	DynamicStreamPendingQueueLen = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dynamic_stream",
			Name:      "pending_queue_len",
		}, []string{"component"})
	DynamicStreamAddPathNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dynamic_stream",
			Name:      "add_path_num",
			Help:      "The number of add path command",
		}, []string{"component"})
	DynamicStreamRemovePathNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dynamic_stream",
			Name:      "remove_path_num",
			Help:      "The number of remove path command",
		}, []string{"component"})
	DynamicStreamArrangeStreamNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dynamic_stream",
			Name:      "arrange_stream_num",
			Help:      "The number of arrange stream command",
		}, []string{"component"})
)

func InitDynamicStreamMetrics(registry *prometheus.Registry) {
	registry.MustRegister(DynamicStreamMemoryUsage)
	registry.MustRegister(DynamicStreamEventChanSize)
	registry.MustRegister(DynamicStreamPendingQueueLen)
	registry.MustRegister(DynamicStreamAddPathNum)
	registry.MustRegister(DynamicStreamRemovePathNum)
	registry.MustRegister(DynamicStreamArrangeStreamNum)
}
