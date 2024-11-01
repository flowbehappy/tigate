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

package v2

import (
	"github.com/gin-gonic/gin"
	"github.com/pingcap/ticdc/api/middleware"
	"github.com/pingcap/ticdc/pkg/node"
)

// OpenAPIV2 provides CDC v2 APIs
type OpenAPIV2 struct {
	server node.Server
}

// NewOpenAPIV2 creates a new OpenAPIV2.
func NewOpenAPIV2(c node.Server) OpenAPIV2 {
	return OpenAPIV2{c}
}

// RegisterOpenAPIV2Routes registers routes for OpenAPI
func RegisterOpenAPIV2Routes(router *gin.Engine, api OpenAPIV2) {
	v2 := router.Group("/api/v2")

	v2.Use(middleware.LogMiddleware())
	v2.Use(middleware.ErrorHandleMiddleware())

	v2.GET("status", api.serverStatus)
	// For compatibility with the old API,
	// TiDB Operator relies on this API to determine whether the TiCDC node is healthy.
	router.GET("/status", api.serverStatus)

	coordinatorMiddleware := middleware.ForwardToCoordinatorMiddleware(api.server)

	// changefeed apis
	changefeedGroup := v2.Group("/changefeeds")
	changefeedGroup.GET("/:changefeed_id", coordinatorMiddleware, api.getChangeFeed)
	changefeedGroup.POST("", coordinatorMiddleware, api.createChangefeed)
	changefeedGroup.GET("", coordinatorMiddleware, api.listChangeFeeds)
	changefeedGroup.PUT("/:changefeed_id", coordinatorMiddleware, api.updateChangefeed)
	changefeedGroup.POST("/:changefeed_id/resume", coordinatorMiddleware, api.resumeChangefeed)
	changefeedGroup.POST("/:changefeed_id/pause", coordinatorMiddleware, api.pauseChangefeed)
	changefeedGroup.DELETE("/:changefeed_id", coordinatorMiddleware, api.deleteChangefeed)

	// capture apis
	captureGroup := v2.Group("/captures")
	captureGroup.Use(coordinatorMiddleware)
	captureGroup.GET("", api.listCaptures)

	verifyTableGroup := v2.Group("/verify_table")
	verifyTableGroup.POST("", api.verifyTable)

	// common APIs
	v2.POST("/tso", api.QueryTso)
}
