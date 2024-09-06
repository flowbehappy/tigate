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

package server

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/flowbehappy/tigate/api"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	clogutil "github.com/pingcap/tiflow/pkg/logutil"
	"go.uber.org/zap"
	"golang.org/x/net/netutil"
)

const (
	// maxHTTPConnection is used to limit the max concurrent connections of http server.
	maxHTTPConnection = 1000
	// httpConnectionTimeout is used to limit a connection max alive time of http server.
	httpConnectionTimeout = 10 * time.Minute
)

type HttpServer struct {
	server   *http.Server
	listener net.Listener
}

// NewHttpServer create the HTTP server.
// `lis` is a listener that gives us plain-text HTTP requests.
func NewHttpServer(c *server, lis net.Listener) common.SubModule {
	// LimitListener returns a Listener that accepts at most n simultaneous
	// connections from the provided Listener. Connections that exceed the
	// limit will wait in a queue and no new goroutines will be created until
	// a connection is processed.
	// We use it here to limit the max concurrent connections of statusServer.
	lis = netutil.LimitListener(lis, maxHTTPConnection)

	logWritter := clogutil.InitGinLogWritter()
	router := gin.New()
	// add gin.RecoveryWithWriter() to handle unexpected panic (logging and
	// returning status code 500)
	router.Use(gin.RecoveryWithWriter(logWritter))
	// router.
	// Register APIs.
	api.RegisterRoutes(router, c, registry)

	// No need to configure TLS because it is already handled by `s.tcpServer`.
	// Add ReadTimeout and WriteTimeout to avoid some abnormal connections never close.
	return &HttpServer{
		listener: lis,
		server: &http.Server{
			Handler:      router,
			ReadTimeout:  httpConnectionTimeout,
			WriteTimeout: httpConnectionTimeout,
		},
	}
}

func (s *HttpServer) Run(ctx context.Context) error {
	log.Info("http server is running", zap.String("addr", s.listener.Addr().String()))
	err := s.server.Serve(s.listener)
	if err != nil {
		log.Error("http server error", zap.Error(cerror.WrapError(cerror.ErrServeHTTP, err)))
	}
	return err
}

func (s *HttpServer) Close(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *HttpServer) Name() string {
	return "http-server"
}
