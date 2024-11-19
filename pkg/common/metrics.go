// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package common

import "github.com/prometheus/client_golang/prometheus"

var sharedColumnSchemaCountGauge = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "common",
		Name:      "shared_column_schema_count",
		Help:      "the count of shared column schema in the cluster",
	})

// InitMetrics registers the etcd request counter.
func InitMetrics(registry *prometheus.Registry) {
	prometheus.MustRegister(sharedColumnSchemaCountGauge)
}
