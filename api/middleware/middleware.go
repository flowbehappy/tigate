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
	"github.com/pingcap/tiflow/pkg/httputil"
	"net/http"
	"strconv"
	"time"

	"github.com/flowbehappy/tigate/capture"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
)

const (
	// forwardFromCapture is a header to be set when forwarding requests to owner
	forwardFromCapture = "TiCDC-ForwardFromCapture"
	// forwardTimes is a header to identify how many times the request has been forwarded
	forwardTimes = "TiCDC-ForwardTimes"
	// maxForwardTimes is the max time a request can be forwarded,  non-controller->controller->changefeed owner
	maxForwardTimes = 2
)

// ClientVersionHeader is the header name of client version
const ClientVersionHeader = "X-client-version"

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
func ForwardToCoordinatorMiddleware(p capture.Capture) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		if !p.IsOwner() {
			ForwardToOwner(ctx, p)

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
func ForwardToOwner(c *gin.Context, p capture.Capture) {
	ctx := c.Request.Context()
	info, err := p.Info()
	if err != nil {
		_ = c.Error(err)
		return
	}

	var owner *model.CaptureInfo
	// get owner info
	owner, err = p.GetOwnerCaptureInfo(ctx)
	if err != nil {
		log.Info("get owner failed", zap.Error(err))
		_ = c.Error(err)
		return
	}
	ForwardToCapture(c, info.ID, owner.AdvertiseAddr)
}

// ForwardToCapture forward request to another
func ForwardToCapture(c *gin.Context, fromID, toAddr string) {
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
	log.Info("forwarding request to capture",
		zap.String("url", c.Request.RequestURI),
		zap.String("method", c.Request.Method),
		zap.String("fromID", fromID),
		zap.String("toAddr", toAddr),
		zap.String("forwardTimes", timeStr))

	req.Header.Add(forwardFromCapture, fromID)
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
