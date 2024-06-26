// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package heartbeatpb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// HeartBeatClient is the client API for HeartBeat service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type HeartBeatClient interface {
	HeartBeat(ctx context.Context, opts ...grpc.CallOption) (HeartBeat_HeartBeatClient, error)
}

type heartBeatClient struct {
	cc grpc.ClientConnInterface
}

func NewHeartBeatClient(cc grpc.ClientConnInterface) HeartBeatClient {
	return &heartBeatClient{cc}
}

func (c *heartBeatClient) HeartBeat(ctx context.Context, opts ...grpc.CallOption) (HeartBeat_HeartBeatClient, error) {
	stream, err := c.cc.NewStream(ctx, &HeartBeat_ServiceDesc.Streams[0], "/heartbeatpb.HeartBeat/HeartBeat", opts...)
	if err != nil {
		return nil, err
	}
	x := &heartBeatHeartBeatClient{stream}
	return x, nil
}

type HeartBeat_HeartBeatClient interface {
	Send(*HeartBeatRequest) error
	Recv() (*HeartBeatResponse, error)
	grpc.ClientStream
}

type heartBeatHeartBeatClient struct {
	grpc.ClientStream
}

func (x *heartBeatHeartBeatClient) Send(m *HeartBeatRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *heartBeatHeartBeatClient) Recv() (*HeartBeatResponse, error) {
	m := new(HeartBeatResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// HeartBeatServer is the server API for HeartBeat service.
// All implementations must embed UnimplementedHeartBeatServer
// for forward compatibility
type HeartBeatServer interface {
	HeartBeat(HeartBeat_HeartBeatServer) error
	mustEmbedUnimplementedHeartBeatServer()
}

// UnimplementedHeartBeatServer must be embedded to have forward compatible implementations.
type UnimplementedHeartBeatServer struct {
}

func (UnimplementedHeartBeatServer) HeartBeat(HeartBeat_HeartBeatServer) error {
	return status.Errorf(codes.Unimplemented, "method HeartBeat not implemented")
}
func (UnimplementedHeartBeatServer) mustEmbedUnimplementedHeartBeatServer() {}

// UnsafeHeartBeatServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to HeartBeatServer will
// result in compilation errors.
type UnsafeHeartBeatServer interface {
	mustEmbedUnimplementedHeartBeatServer()
}

func RegisterHeartBeatServer(s grpc.ServiceRegistrar, srv HeartBeatServer) {
	s.RegisterService(&HeartBeat_ServiceDesc, srv)
}

func _HeartBeat_HeartBeat_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(HeartBeatServer).HeartBeat(&heartBeatHeartBeatServer{stream})
}

type HeartBeat_HeartBeatServer interface {
	Send(*HeartBeatResponse) error
	Recv() (*HeartBeatRequest, error)
	grpc.ServerStream
}

type heartBeatHeartBeatServer struct {
	grpc.ServerStream
}

func (x *heartBeatHeartBeatServer) Send(m *HeartBeatResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *heartBeatHeartBeatServer) Recv() (*HeartBeatRequest, error) {
	m := new(HeartBeatRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// HeartBeat_ServiceDesc is the grpc.ServiceDesc for HeartBeat service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var HeartBeat_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "heartbeatpb.HeartBeat",
	HandlerType: (*HeartBeatServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "HeartBeat",
			Handler:       _HeartBeat_HeartBeat_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "heartbeatpb/heartbeat.proto",
}

// DispatcherManagerClient is the client API for DispatcherManager service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type DispatcherManagerClient interface {
	// 为 changefeed 创建 dispatcher manager（bootstrap）
	CreateEventDispatcherManager(ctx context.Context, opts ...grpc.CallOption) (DispatcherManager_CreateEventDispatcherManagerClient, error)
	// remove changefeed 的时候发送，删除 event dispatcher manager
	RemoveEventDispatcherManager(ctx context.Context, opts ...grpc.CallOption) (DispatcherManager_RemoveEventDispatcherManagerClient, error)
}

type dispatcherManagerClient struct {
	cc grpc.ClientConnInterface
}

func NewDispatcherManagerClient(cc grpc.ClientConnInterface) DispatcherManagerClient {
	return &dispatcherManagerClient{cc}
}

func (c *dispatcherManagerClient) CreateEventDispatcherManager(ctx context.Context, opts ...grpc.CallOption) (DispatcherManager_CreateEventDispatcherManagerClient, error) {
	stream, err := c.cc.NewStream(ctx, &DispatcherManager_ServiceDesc.Streams[0], "/heartbeatpb.DispatcherManager/CreateEventDispatcherManager", opts...)
	if err != nil {
		return nil, err
	}
	x := &dispatcherManagerCreateEventDispatcherManagerClient{stream}
	return x, nil
}

type DispatcherManager_CreateEventDispatcherManagerClient interface {
	Send(*CreateEventDispatcherManagerRequest) error
	Recv() (*CreateEventDispatcherManagerResponse, error)
	grpc.ClientStream
}

type dispatcherManagerCreateEventDispatcherManagerClient struct {
	grpc.ClientStream
}

func (x *dispatcherManagerCreateEventDispatcherManagerClient) Send(m *CreateEventDispatcherManagerRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *dispatcherManagerCreateEventDispatcherManagerClient) Recv() (*CreateEventDispatcherManagerResponse, error) {
	m := new(CreateEventDispatcherManagerResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *dispatcherManagerClient) RemoveEventDispatcherManager(ctx context.Context, opts ...grpc.CallOption) (DispatcherManager_RemoveEventDispatcherManagerClient, error) {
	stream, err := c.cc.NewStream(ctx, &DispatcherManager_ServiceDesc.Streams[1], "/heartbeatpb.DispatcherManager/RemoveEventDispatcherManager", opts...)
	if err != nil {
		return nil, err
	}
	x := &dispatcherManagerRemoveEventDispatcherManagerClient{stream}
	return x, nil
}

type DispatcherManager_RemoveEventDispatcherManagerClient interface {
	Send(*RemoveEventDispatcherManagerRequest) error
	Recv() (*RemoveEventDispatcherManagerResponse, error)
	grpc.ClientStream
}

type dispatcherManagerRemoveEventDispatcherManagerClient struct {
	grpc.ClientStream
}

func (x *dispatcherManagerRemoveEventDispatcherManagerClient) Send(m *RemoveEventDispatcherManagerRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *dispatcherManagerRemoveEventDispatcherManagerClient) Recv() (*RemoveEventDispatcherManagerResponse, error) {
	m := new(RemoveEventDispatcherManagerResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// DispatcherManagerServer is the server API for DispatcherManager service.
// All implementations must embed UnimplementedDispatcherManagerServer
// for forward compatibility
type DispatcherManagerServer interface {
	// 为 changefeed 创建 dispatcher manager（bootstrap）
	CreateEventDispatcherManager(DispatcherManager_CreateEventDispatcherManagerServer) error
	// remove changefeed 的时候发送，删除 event dispatcher manager
	RemoveEventDispatcherManager(DispatcherManager_RemoveEventDispatcherManagerServer) error
	mustEmbedUnimplementedDispatcherManagerServer()
}

// UnimplementedDispatcherManagerServer must be embedded to have forward compatible implementations.
type UnimplementedDispatcherManagerServer struct {
}

func (UnimplementedDispatcherManagerServer) CreateEventDispatcherManager(DispatcherManager_CreateEventDispatcherManagerServer) error {
	return status.Errorf(codes.Unimplemented, "method CreateEventDispatcherManager not implemented")
}
func (UnimplementedDispatcherManagerServer) RemoveEventDispatcherManager(DispatcherManager_RemoveEventDispatcherManagerServer) error {
	return status.Errorf(codes.Unimplemented, "method RemoveEventDispatcherManager not implemented")
}
func (UnimplementedDispatcherManagerServer) mustEmbedUnimplementedDispatcherManagerServer() {}

// UnsafeDispatcherManagerServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to DispatcherManagerServer will
// result in compilation errors.
type UnsafeDispatcherManagerServer interface {
	mustEmbedUnimplementedDispatcherManagerServer()
}

func RegisterDispatcherManagerServer(s grpc.ServiceRegistrar, srv DispatcherManagerServer) {
	s.RegisterService(&DispatcherManager_ServiceDesc, srv)
}

func _DispatcherManager_CreateEventDispatcherManager_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(DispatcherManagerServer).CreateEventDispatcherManager(&dispatcherManagerCreateEventDispatcherManagerServer{stream})
}

type DispatcherManager_CreateEventDispatcherManagerServer interface {
	Send(*CreateEventDispatcherManagerResponse) error
	Recv() (*CreateEventDispatcherManagerRequest, error)
	grpc.ServerStream
}

type dispatcherManagerCreateEventDispatcherManagerServer struct {
	grpc.ServerStream
}

func (x *dispatcherManagerCreateEventDispatcherManagerServer) Send(m *CreateEventDispatcherManagerResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *dispatcherManagerCreateEventDispatcherManagerServer) Recv() (*CreateEventDispatcherManagerRequest, error) {
	m := new(CreateEventDispatcherManagerRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _DispatcherManager_RemoveEventDispatcherManager_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(DispatcherManagerServer).RemoveEventDispatcherManager(&dispatcherManagerRemoveEventDispatcherManagerServer{stream})
}

type DispatcherManager_RemoveEventDispatcherManagerServer interface {
	Send(*RemoveEventDispatcherManagerResponse) error
	Recv() (*RemoveEventDispatcherManagerRequest, error)
	grpc.ServerStream
}

type dispatcherManagerRemoveEventDispatcherManagerServer struct {
	grpc.ServerStream
}

func (x *dispatcherManagerRemoveEventDispatcherManagerServer) Send(m *RemoveEventDispatcherManagerResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *dispatcherManagerRemoveEventDispatcherManagerServer) Recv() (*RemoveEventDispatcherManagerRequest, error) {
	m := new(RemoveEventDispatcherManagerRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// DispatcherManager_ServiceDesc is the grpc.ServiceDesc for DispatcherManager service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var DispatcherManager_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "heartbeatpb.DispatcherManager",
	HandlerType: (*DispatcherManagerServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "CreateEventDispatcherManager",
			Handler:       _DispatcherManager_CreateEventDispatcherManager_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
		{
			StreamName:    "RemoveEventDispatcherManager",
			Handler:       _DispatcherManager_RemoveEventDispatcherManager_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "heartbeatpb/heartbeat.proto",
}
