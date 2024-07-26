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

package worker

import "github.com/prometheus/client_golang/prometheus"

var (
	WorkerFlushDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tigate",
			Subsystem: "sink",
			Name:      "worker_flush_duration",
			Help:      "Flush duration (s) for txn worker.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{"changefeed", "id"})
)

func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(WorkerFlushDuration)
}
