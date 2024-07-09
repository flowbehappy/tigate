package config

import "time"

const (
	// These are the default configuration for the gRPC server.
	defaultMaxRecvMsgSize   = 256 * 1024 * 1024 // 256MB
	defaultKeepaliveTime    = 30 * time.Second
	defaultKeepaliveTimeout = 10 * time.Second
)

type GrpcServerConfig struct {
	// After a duration of this time if the server doesn't see any activity it
	// pings the client to see if the transport is still alive.
	KeepAliveTime time.Duration
	// After having pinged for keepalive check, the server waits for a duration
	// of Timeout and if no activity is seen even after that the connection is
	// closed.
	KeepAliveTimeout time.Duration
	// MaxRecvMsgSize is the maximum message size in bytes the gRPC server can receive.
	MaxRecvMsgSize int
}

func NewDefaultGrpcServerConfig() *GrpcServerConfig {
	return &GrpcServerConfig{
		KeepAliveTime:    defaultKeepaliveTime,
		KeepAliveTimeout: defaultKeepaliveTimeout,
		MaxRecvMsgSize:   defaultMaxRecvMsgSize,
	}
}
