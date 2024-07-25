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

package dispatchermanager

import "github.com/prometheus/client_golang/prometheus"

var (
	// Metrics is the metrics collector related to dispatcher manager.
	TableEventDispatcherCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tigate",
			Subsystem: "dispatchermanager",
			Name:      "table_event_dispatcher_count",
			Help:      "The number of table event dispatchers",
		}, []string{"changefeed"})
)

func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(TableEventDispatcherCount)
}
