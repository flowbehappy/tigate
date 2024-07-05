package messaging

import (
	"io"
	"net"
	"sync"

	"github.com/flowbehappy/tigate/messaging/proto"
	"google.golang.org/grpc"
)

const (
	chanSize = 1024
)

// For sending events and commands.
// Note the slices passed in are referenced forward directly, so don't reuse the slice.
// The method in the interface should be thread-safe and non-blocking.
// If the message cannot be sent, the method will return an `ErrorTypeMessageCongested` error.
// TODO(dongmen): Make it support timeout later.
type SendMessageChannel interface {
	SendEvent(mtype IOType, eventBytes [][]byte) error
	SendCommand(mtype IOType, commandBytes [][]byte) error
}

// For Receiving events and commands.
type ReceiveMessageChannel interface {
	ReceiveEvent() (*TargetMessage, error)
	ReceiveCmd() (*TargetMessage, error)
}

// MessageCenter is the core of the messaging system.
// It hosts a local grpc server to receive messages (events and commands) from other targets (server).
// It hosts streaming channels to each other targets to send messages.
// Events and commands are sent by different channels.
//
// If the target is a remote server(the other process), the messages will be sent to the target by grpc streaming channel.
// If the target is the local (the same process), the messages will be sent to the local by golang channel directly.
//
// TODO:
// Currently, for each target, we only use one channel to send events. We might use multiple channels later.
type MessageCenter struct {
	// The local service id
	localId ServerId
	// The local target
	localTarget *localMessageTarget

	grpcServer *grpc.Server
	// The remote targets
	remoteTargets map[ServerId]*remoteMessageTarget

	// Messages from all targets are put into these channels.
	receiveEventCh chan *TargetMessage
	receiveCmdCh   chan *TargetMessage

	wg sync.WaitGroup
}

func NewMessageCenter(id ServerId) *MessageCenter {
	receiveEventCh := make(chan *TargetMessage, chanSize)
	receiveCmdCh := make(chan *TargetMessage, chanSize)
	return &MessageCenter{localId: id,
		localTarget:    newLocalMessageTarget(id, receiveEventCh, receiveCmdCh),
		remoteTargets:  make(map[ServerId]*remoteMessageTarget),
		receiveEventCh: receiveEventCh,
		receiveCmdCh:   receiveCmdCh,
	}
}

// Run starts the grpc server to receive messages from other targets.
// It listens on the given address, it will block until the server is stopped.
func (mc *MessageCenter) Run(addr string) error {
	if mc.grpcServer != nil {
		return nil
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	mc.grpcServer = grpc.NewServer()
	proto.RegisterMessageCenterServer(mc.grpcServer, &grpcServerImpl{messageCenter: mc})
	return mc.grpcServer.Serve(lis)
}

// Close stops the grpc server and stops all the connections to the remote targets.
func (mc *MessageCenter) Close() {
	for _, rt := range mc.remoteTargets {
		if rt.conn != nil {
			rt.conn.Close()
		}
		rt.cancel()
	}
	if mc.grpcServer != nil {
		mc.grpcServer.Stop()
	}
	mc.grpcServer = nil
	mc.wg.Wait()
}

func (mc *MessageCenter) IsRunning() bool {
	return mc.grpcServer != nil && mc.grpcServer.GetServiceInfo() != nil
}

// touchRemoteTarget returns the remote target by the id,
// if the target is not found, it will create a new one.
func (mc *MessageCenter) touchRemoteTarget(id ServerId) *remoteMessageTarget {
	if rt, ok := mc.remoteTargets[id]; ok {
		return rt
	}
	rt := newRemoteMessageTarget(id)
	rt.localId = mc.localId

	mc.remoteTargets[id] = rt
	return rt
}

func (mc *MessageCenter) addRemoteReceiveTarget(id ServerId, stream grpcReceiver, isEvent bool) {
	rt := mc.touchRemoteTarget(id)
	if isEvent {
		rt.setEventRecvStream(stream)
	} else {
		rt.setCommandRecvStream(stream)
	}
}

// AddSendTarget is called when a new remote target is discovered,
// to add the target to the message center.
func (mc *MessageCenter) AddSendTarget(id ServerId, addr string) {
	if id == mc.localId {
		return
	}
	rt := mc.touchRemoteTarget(id)
	rt.targetAddr = addr
	rt.initSendStreams()
}

func (mc *MessageCenter) RemoveSendTarget(id ServerId) {
	if rt, ok := mc.remoteTargets[id]; ok {
		rt.close()
		delete(mc.remoteTargets, id)
	}
}

func (mc *MessageCenter) ReceiveEvent() (*TargetMessage, error) {
	return <-mc.receiveEventCh, nil
}

func (mc *MessageCenter) ReceiveCmd() (*TargetMessage, error) {
	return <-mc.receiveCmdCh, nil
}

func (mc *MessageCenter) GetReceiveTarget() ReceiveMessageChannel { return mc }

// GetSendTarget returns the SendMessageChannel for the target server.
func (mc *MessageCenter) GetSendTarget(id ServerId) SendMessageChannel {
	if id == mc.localId {
		return mc.localTarget
	}
	if rt, ok := mc.remoteTargets[id]; ok {
		return rt
	}
	return nil
}

// gRPC generates two different interfaces, MessageCenter_SendEventsServer
// and MessageCenter_SendCommandsServer.
// We use the interface to unite them, to simplify the code.
type grpcReceiver interface {
	Recv() (*proto.Message, error)
	SendAndClose(*proto.MessageSummary) error
}

type grpcSender interface {
	Send(*proto.Message) error
	CloseAndRecv() (*proto.MessageSummary, error)
}

// grpcServerImpl implements the gRPC `service MessageCenter` defined in the proto file
// It handles the gRPC requests from the clients,
// and then calls the methods in MessageCenter struct to handle the requests.
type grpcServerImpl struct {
	proto.UnimplementedMessageCenterServer
	messageCenter *MessageCenter
}

func (s *grpcServerImpl) SendEvents(stream proto.MessageCenter_SendEventsServer) error {
	return s.handleClientConnect(stream, true)
}

func (s *grpcServerImpl) SendCommands(stream proto.MessageCenter_SendCommandsServer) error {
	return s.handleClientConnect(stream, false)
}

// handleClientConnect registers the client as a target in the message center.
// So the message center can stream messages to the client.
func (s *grpcServerImpl) handleClientConnect(stream grpcReceiver, isEvent bool) error {
	// The first message is an empty message without payload, to identify the client server id.
	msg, err := stream.Recv()
	if err == io.EOF {
		return stream.SendAndClose(&proto.MessageSummary{SentBytes: 0 /*TODO*/})
	}
	if err != nil {
		return err
	}
	sid := ServerId(msg.From)
	s.messageCenter.addRemoteReceiveTarget(sid, stream, isEvent)
	return nil
}
