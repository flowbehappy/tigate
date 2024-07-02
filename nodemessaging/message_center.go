package nodemessaging

import (
	"context"
	"log"
	"net"

	"github.com/flowbehappy/tigate/nodemessaging/proto"
	"google.golang.org/grpc"
)

// This struct implements the gRPC `service MessageCenter` defined in the proto file
// It handles the gRPC requests from the clients, and then calls the methods in MessageCenter struct to handle the requests.
type MessageService struct {
	proto.UnimplementedMessageCenterServer
	messageCenter *MessageCenter
}

func (s *MessageService) SubscribeEvents(*proto.CallerInfo, proto.MessageCenter_SubscribeEventsServer) error {
	// TODO
	return nil
}

func (s *MessageService) PushCommands(proto.MessageCenter_SendCommandsServer) error {
	// TODO
	return nil
}

type ServiceType int

const (
	Local  ServiceType = 1
	Remote ServiceType = 2
)

type IsService interface{ GetServiceType() ServiceType }
type LocalService struct{}
type RemoteService struct{}

func (l *LocalService) GetServiceType() ServiceType  { return Local }
func (r *RemoteService) GetServiceType() ServiceType { return Remote }

// Here we have 4 interfaces. Each of them represents a stream of messages.
// Two messages types are defined: EventMessage and CommandMessage. And we have two directions: send and receive.
// So we have 4 interfaces: SendEvenStream, ReceiveEventStream, SendCommandStream, ReceiveCommandStream.
//
// And the stream could be connected to a remote service via gRPC or to the local service via golang channel.
// So we have 8 structs.
//
// Here is the workflow for gRPC:
// 1. For event messages
// 	1.a The client side calls `SubscribeEvents` to the server.
// 	1.b The server side listens on `SubscribeEvents` and returns a stream.
//	1.c The server side keeps sending messages to the stream.
// 	1.d The client side keeps receiving messages from the stream.
// 2. For command messages
// 	2.a The client side calls `SendCommands` to the server.
// 	2.b The server side listens on `SendCommands` and returns a stream.
//	2.c The client side keeps sending messages to the stream.
// 	2.d The server side keeps receiving messages from the stream.
//
// You can find that the events are only sent from server to client, and the commands are only sent from client to server.
//
// Node that the stream is not thread safe. Don't use it in multiple goroutines!

// For server side.
type SendEvenStream interface {
	IsService
	Send(*proto.EventMessage) error
}

// For client side.
type ReceiveEventStream interface {
	IsService
	Recv() (*proto.EventMessage, error)
}

// For server side
type SendCommandStream interface {
	IsService
	Send(*proto.CommandMessage) error
}

// For client side.
type ReceiveCommandStream interface {
	IsService
	Recv() (*proto.CommandMessage, error)
}

type RemoteSendEventStream struct {
	RemoteService
	proto.MessageCenter_SubscribeEventsServer
}

type LocalSendEventStream struct {
	LocalService
	c chan *proto.EventMessage
}

func (s *LocalSendEventStream) Send(m *proto.EventMessage) error { s.c <- m; return nil }

type RemoteReceiveEventStream struct {
	RemoteService
	proto.MessageCenter_SubscribeEventsClient
}

type LocalReceiveEventStream struct {
	LocalService
	c <-chan *proto.EventMessage
}

func (s *LocalReceiveEventStream) Recv() (*proto.EventMessage, error) {
	m, ok := <-s.c
	return m, ok
}

type RemoteSendCommandStream struct {
	RemoteService
	proto.MessageCenter_SendCommandsServer
}

type LocalSendCommandStream struct {
	LocalService
	c chan *proto.CommandMessage
}

type RemoteReceiveCommandStream struct {
	RemoteService
	c proto.MessageCenter_SendCommandsClient
}

type LocalReceiveCommandStream struct {
	LocalService
	c <-chan *proto.CommandMessage
}

type MessageCenter struct {
	// The local service
	localAddr  string
	grpcServer *grpc.Server

	// TODO: We might use mutiple connections to on remote service later.
	sendEvents      map[string]SendEvenStream
	receiveEvents   map[string]ReceiveEventStream
	sendCommands    map[string]SendCommandStream
	receiveCommands map[string]ReceiveCommandStream
}

func NewMessageCenter(addr string) *MessageCenter {
	return &MessageCenter{
		localAddr:       addr,
		sendEvents:      make(map[string]SendEvenStream),
		receiveEvents:   make(map[string]ReceiveEventStream),
		sendCommands:    make(map[string]SendCommandStream),
		receiveCommands: make(map[string]ReceiveCommandStream),
	}
}

// This method will block untill the server is stopped
func (mc *MessageCenter) StartService() {
	list, err := net.Listen("tcp", mc.localAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	mc.grpcServer = grpc.NewServer()
	proto.RegisterMessageCenterServer(mc.grpcServer, &MessageService{messageCenter: mc})
	log.Printf("server listening at %v", list.Addr())
	if err := mc.grpcServer.Serve(list); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
func (mc *MessageCenter) StopService() {
	if mc.grpcServer != nil {
		mc.grpcServer.Stop()
	}
	mc.grpcServer = nil
}

func (mc *MessageCenter) IsRunning() bool {
	return mc.grpcServer != nil && mc.grpcServer.GetServiceInfo() != nil
}

func (mc *MessageCenter) getRemoteReceiveEventStream(addr string) (SendEvenStream, error) {
	if s, ok := mc.sendEvents[addr]; ok {
		return s, nil
	}
	conn, err := grpc.NewClient(addr)
	if err != nil {
		return nil, err
	}
	client := proto.NewMessageCenterClient(conn)
	caller := &proto.CallerInfo{Address: mc.localAddr}
	stream, err := client.SubscribeEvents(context.Background(), caller)
	if err != nil {
		return nil, err
	}

	rs := &RemoteReceiveEventStream{RemoteService{}, stream}
	mc.receiveEvents[addr] = rs

}

func (mc *MessageCenter) GetReceiveEventStream(addr string) (SendEvenStream, error) {

}
