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

package middleware

import (
	"bufio"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/httputil"
	"go.uber.org/zap"
)

const (
	// forwardFrom is a header to be set when forwarding requests to owner
	forwardFrom = "TiCDC-ForwardFrom"
	// forwardTimes is a header to identify how many times the request has been forwarded
	forwardTimes = "TiCDC-ForwardTimes"
	// maxForwardTimes is the max time a request can be forwarded,  non-controller->controller->changefeed owner
	maxForwardTimes = 2
)

// ClientVersionHeader is the header name of client version
const ClientVersionHeader = "X-client-version"

// ErrorHandleMiddleware puts the error into response
func ErrorHandleMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		// because we will return immediately after an error occurs in http_handler
		// there wil be only one error in c.Errors
		lastError := c.Errors.Last()
		if lastError != nil {
			err := lastError.Err
			// put the error into response
			if api.IsHTTPBadRequestError(err) {
				c.IndentedJSON(http.StatusBadRequest, model.NewHTTPError(err))
			} else {
				c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
			}
			c.Abort()
			return
		}
	}
}

// LogMiddleware logs the api requests
func LogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery
		user, _, _ := c.Request.BasicAuth()
		c.Next()

		cost := time.Since(start)

		err := c.Errors.Last()
		var stdErr error
		if err != nil {
			stdErr = err.Err
		}
		version := c.Request.Header.Get(ClientVersionHeader)
		log.Info("cdc open api request",
			zap.Int("status", c.Writer.Status()),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.String("ip", c.ClientIP()),
			zap.String("user-agent", c.Request.UserAgent()), zap.String("client-version", version),
			zap.String("username", user),
			zap.Error(stdErr),
			zap.Duration("duration", cost),
		)
	}
}

// ForwardToCoordinatorMiddleware forward a request to controller
func ForwardToCoordinatorMiddleware(server node.Server) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		if !server.IsCoordinator() {
			ForwardToOwner(ctx, server)

			// Without calling Abort(), Gin will continue to process the next handler,
			// execute code which should only be run by the owner, and cause a panic.
			// See https://github.com/pingcap/tiflow/issues/5888
			ctx.Abort()
			return
		}
		ctx.Next()
	}
}

// ForwardToOwner forwards a request to the controller
func ForwardToOwner(c *gin.Context, server node.Server) {
	ctx := c.Request.Context()
	info, err := server.SelfInfo()
	if err != nil {
		_ = c.Error(err)
		return
	}

	var node *node.Info
	// get coordinator info
	node, err = server.GetCoordinatorInfo(ctx)
	if err != nil {
		log.Info("get owner failed", zap.Error(err))
		_ = c.Error(err)
		return
	}
	ForwardToServer(c, info.ID, node.AdvertiseAddr)
}

// ForwardToServer forward request to another
func ForwardToServer(c *gin.Context, fromID node.ID, toAddr string) {
	ctx := c.Request.Context()

	timeStr := c.GetHeader(forwardTimes)
	var (
		err              error
		lastForwardTimes uint64
	)
	if len(timeStr) != 0 {
		lastForwardTimes, err = strconv.ParseUint(timeStr, 10, 64)
		if err != nil {
			_ = c.Error(err)
			return
		}
		if lastForwardTimes > maxForwardTimes {
			_ = c.Error(err)
			return
		}
	}

	security := config.GetGlobalServerConfig().Security

	// init a request
	req, err := http.NewRequestWithContext(
		ctx, c.Request.Method, c.Request.RequestURI, c.Request.Body)
	if err != nil {
		_ = c.Error(err)
		return
	}

	req.URL.Host = toAddr
	// we should check tls config instead of security here because
	// security will never be nil
	if tls, _ := security.ToTLSConfigWithVerify(); tls != nil {
		req.URL.Scheme = "https"
	} else {
		req.URL.Scheme = "http"
	}
	for k, v := range c.Request.Header {
		for _, vv := range v {
			req.Header.Add(k, vv)
		}
	}
	log.Info("forwarding request to server",
		zap.String("url", c.Request.RequestURI),
		zap.String("method", c.Request.Method),
		zap.Any("fromID", fromID),
		zap.String("toAddr", toAddr),
		zap.String("forwardTimes", timeStr))

	req.Header.Add(forwardFrom, string(fromID))
	lastForwardTimes++
	req.Header.Add(forwardTimes, strconv.Itoa(int(lastForwardTimes)))
	// forward toAddr owner
	cli, err := httputil.NewClient(security)
	if err != nil {
		_ = c.Error(err)
		return
	}
	resp, err := cli.Do(req)
	if err != nil {
		_ = c.Error(err)
		return
	}

	// write header
	for k, values := range resp.Header {
		for _, v := range values {
			c.Header(k, v)
		}
	}

	// write status code
	c.Status(resp.StatusCode)

	// write response body
	defer resp.Body.Close()
	_, err = bufio.NewReader(resp.Body).WriteTo(c.Writer)
	if err != nil {
		_ = c.Error(err)
		return
	}
}
