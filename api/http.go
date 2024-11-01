// Copyright 2022 PingCAP, Inc.
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

package api

import (
	"net/http/pprof"

	"github.com/pingcap/ticdc/pkg/node"

	"github.com/gin-gonic/gin"
	v2 "github.com/pingcap/ticdc/api/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RegisterRoutes create a router for OpenAPI
func RegisterRoutes(
	router *gin.Engine,
	server node.Server,
	registry prometheus.Gatherer,
) {

	// Open API V2
	v2.RegisterOpenAPIV2Routes(router, v2.NewOpenAPIV2(server))

	// pprof debug API
	pprofGroup := router.Group("/debug/pprof/")
	pprofGroup.GET("", gin.WrapF(pprof.Index))
	pprofGroup.GET("/:any", gin.WrapF(pprof.Index))
	pprofGroup.GET("/cmdline", gin.WrapF(pprof.Cmdline))
	pprofGroup.GET("/profile", gin.WrapF(pprof.Profile))
	pprofGroup.GET("/symbol", gin.WrapF(pprof.Symbol))
	pprofGroup.GET("/trace", gin.WrapF(pprof.Trace))
	pprofGroup.GET("/threadcreate", gin.WrapF(pprof.Handler("threadcreate").ServeHTTP))

	// Promtheus metrics API
	prometheus.DefaultGatherer = registry
	router.Any("/metrics", gin.WrapH(promhttp.Handler()))
}
