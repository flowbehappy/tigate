// Copyright 2020 PingCAP, Inc.
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
	"os"
	"runtime"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

var (
	GoGC = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "go_gc",
			Help:      "The value of GOGC",
		})

	GoMaxProcs = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "go_max_procs",
			Help:      "The value of GOMAXPROCS",
		})
)

// RecordGoRuntimeSettings records GOGC settings.
func RecordGoRuntimeSettings() {
	// The default GOGC value is 100. See debug.SetGCPercent.
	gogcValue := 100
	if val, err := strconv.Atoi(os.Getenv("GOGC")); err == nil {
		gogcValue = val
	}
	GoGC.Set(float64(gogcValue))

	maxProcs := runtime.GOMAXPROCS(0)
	GoMaxProcs.Set(float64(maxProcs))
}

// InitServerMetrics registers all metrics used in processor
func InitServerMetrics(registry *prometheus.Registry) {
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGoCollector(
		collectors.WithGoCollections(collectors.GoRuntimeMemStatsCollection | collectors.GoRuntimeMetricsCollection)))
	registry.MustRegister(GoGC)
	registry.MustRegister(GoMaxProcs)
}
