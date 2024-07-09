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

	"google.golang.org/grpc"
)

type GrpcModule struct {
	grpcServer *grpc.Server
	lis        net.Listener
}

func NewGrpcServer(lis net.Listener) *GrpcModule {
	grpcServer := grpc.NewServer()
	return &GrpcModule{
		grpcServer: grpcServer,
		lis:        lis,
	}
}

func (g *GrpcModule) Run(ctx context.Context) error {
	return g.grpcServer.Serve(g.lis)
}

func (g *GrpcModule) Close(ctx context.Context) error {
	g.grpcServer.Stop()
	return nil
}

func (g *GrpcModule) Name() string {
	return "grpc"
}
