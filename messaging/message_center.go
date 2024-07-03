package messaging

import (
	"context"
	"fmt"
	"net"
	"time"

	. "github.com/flowbehappy/tigate/apperror"
	"github.com/flowbehappy/tigate/messaging/proto"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// This struct implements the gRPC `service MessageCenter` defined in the proto file
// It handles the gRPC requests from the clients, and then calls the methods in MessageCenter struct to handle the requests.
type grpcServerImpl struct {
	proto.UnimplementedMessageCenterServer
	messageCenter *MessageCenter
}

func (s *grpcServerImpl) SendEvents(stream proto.MessageCenter_SendEventsServer) error {
	// The first message is an empty message without pyload, to identify the client server id.
	return nil
}

func (s *grpcServerImpl) SendCommands(stream proto.MessageCenter_SendCommandsServer) error {
	// The first message is an empty message without pyload, to identify the client server id.
	return nil
}

type ServiceType int

const (
	Local  ServiceType = 1
	Remote ServiceType = 2
)

// For sending events and commands.
// Note the slices passed in are referenced forward directly, so don't reuse the slice.
type SendMessageChannel interface {
	SendEvent(mtype IOType, eventBytes [][]byte) error
	SendCommand(mtype IOType, commandBytes [][]byte) error
}

// For Receiving events and commands.
type ReceiveMessageChannel interface {
	ReceiveEvent() (*TargetMessage, error)
	ReceiveCmd() (*TargetMessage, error)
}

type grpcReceiver interface {
	Recv() (*proto.Message, error)
}

// TODO: handle the connection error during the messages sending
type remoteMessageTarget struct {
	localId    ServerId
	targetId   ServerId
	targetAddr string

	// For sending events and commands
	conn              *grpc.ClientConn
	eventSendStream   proto.MessageCenter_SendEventsClient
	commandSendStream proto.MessageCenter_SendCommandsClient

	// For receiving events and commands
	eventRecvStream   proto.MessageCenter_SendEventsServer
	commandRecvStream proto.MessageCenter_SendCommandsServer

	// We pull the events and commands from remote receive streams,
	// and push to the message center.
	gatherRecvEventChan chan *TargetMessage
	gatherRecvCmdChan   chan *TargetMessage
}

func newRemoteMessageTarget(id ServerId) *remoteMessageTarget {
	return &remoteMessageTarget{targetId: id}
}

// TODO: handle the timeout during connection initialization
func (s *remoteMessageTarget) initSendEventStreams(localId ServerId, addr string, timeout time.Duration) error {
	conn, err := grpc.NewClient(addr)
	if err != nil {
		return AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot create grpc client on address %s, error: %s", addr, err.Error())}
	}
	client := proto.NewMessageCenterClient(conn)
	ctx := context.Background()
	eventStream, err := client.SendEvents(ctx)
	if err != nil {
		conn.Close()
		return AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot open event grpc stream, error: %s", err.Error())}
	}

	bsm := &proto.Message{From: localId.slice(), To: s.targetId.slice()}
	if err := eventStream.Send(bsm); err != nil {
		conn.Close()
		return AppError{
			Type:   ErrorTypeSendMessageFailed,
			Reason: fmt.Sprintf("Cannot send boostrap message, error: %s", err.Error())}
	}

	commandStream, err := client.SendCommands(ctx)
	if err != nil {
		conn.Close()
		return AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot open event grpc stream, error: %s", err.Error())}
	}
	if err := commandStream.Send(bsm); err != nil {
		conn.Close()
		return AppError{
			Type:   ErrorTypeSendMessageFailed,
			Reason: fmt.Sprintf("Cannot send boostrap message, error: %s", err.Error())}
	}

	s.conn = conn
	s.eventSendStream = eventStream
	s.commandSendStream = commandStream
	return nil
}

func runGatherMessages(stream grpcReceiver, gatherChan chan *TargetMessage) {
	for {
		message, err := stream.Recv()
		if err != nil {
			log.Warn("Error when receiving message from remote", zap.Error(err))
			break
		}
		for _, payload := range message.Payload {
			m, err := decodeIOType(IOType(message.Type), payload)
			if err != nil {
				log.Warn("Error when deserializing message from remote", zap.Error(err))
				continue
			}
			tm := &TargetMessage{From: ServerId(message.From), To: ServerId(message.To), Type: IOType(message.Type), Message: m}
			gatherChan <- tm
		}
	}
}

func (s *remoteMessageTarget) setEventRecvStream(eventStream proto.MessageCenter_SendEventsServer) {
	s.eventRecvStream = eventStream
	// Use a goroutine to pull the messages from the stream,
	// assamble them into TargeMessage, and send them to the message center.
	runGatherMessages(eventStream, s.gatherRecvEventChan)
}

func (s *remoteMessageTarget) setCommandRecvStream(commandStream proto.MessageCenter_SendCommandsServer) {
	s.commandRecvStream = commandStream
	runGatherMessages(commandStream, s.gatherRecvCmdChan)
}

func (s *remoteMessageTarget) SendEvent(mtype IOType, eventBytes [][]byte) error {
	if s.eventSendStream == nil {
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: "Event send stream is not set"}
	}
	message := &proto.Message{From: s.localId.slice(), To: s.targetId.slice(), Type: int32(mtype), Payload: eventBytes}
	return s.eventSendStream.Send(message)
}

func (s *remoteMessageTarget) SendCommand(mtype IOType, commandBytes [][]byte) error {
	if s.commandSendStream == nil {
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: "Command send stream is not set"}
	}
	message := &proto.Message{From: s.localId.slice(), To: s.targetId.slice(), Type: int32(mtype), Payload: commandBytes}
	return s.commandSendStream.Send(message)
}

type localMessageTarget struct {
	localId ServerId

	// The gather channel from the message center.
	// We only need to push and pull the messages from those channel.
	gatherRecvEventChan chan *TargetMessage
	gatherRecvCmdChan   chan *TargetMessage
}

func newLocalMessageTarget(id ServerId,
	gatherRecvEventChan chan *TargetMessage,
	gatherRecvCmdChan chan *TargetMessage) *localMessageTarget {
	// TODO: make the buffer size configurable
	return &localMessageTarget{
		localId:             id,
		gatherRecvEventChan: gatherRecvEventChan,
		gatherRecvCmdChan:   gatherRecvCmdChan,
	}
}

func sendTargeMessagesToLocalChan(sid ServerId, mtype IOType, eventBytes [][]byte, channel chan *TargetMessage) {
	for _, eventBytes := range eventBytes {
		m, err := decodeIOType(mtype, eventBytes)
		if err != nil {
			log.Warn("Error when deserializing message from remote", zap.Error(err))
			continue
		}
		message := &TargetMessage{From: sid, To: sid, Type: mtype, Message: m}
		channel <- message
	}
}

func (s *localMessageTarget) SendEvent(mtype IOType, eventBytes [][]byte) error {
	sendTargeMessagesToLocalChan(s.localId, mtype, eventBytes, s.gatherRecvEventChan)
	return nil
}

func (s *localMessageTarget) SendCommand(mtype IOType, commandBytes [][]byte) error {
	sendTargeMessagesToLocalChan(s.localId, mtype, commandBytes, s.gatherRecvCmdChan)
	return nil
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
	// The local service
	localId    ServerId
	grpcServer *grpc.Server

	// The local target
	localTarget *localMessageTarget

	// The remote targets
	remoteTargets map[ServerId]*remoteMessageTarget

	// The channels to gather messages from all targets' receive channels
	gatherRecvEventChan chan *TargetMessage
	gatherRecvCmdChan   chan *TargetMessage
}

func NewMessageCenter(id ServerId) *MessageCenter {
	gatherRecvEventChan := make(chan *TargetMessage, 100)
	gatherRecvCmdChan := make(chan *TargetMessage, 100)
	return &MessageCenter{localId: id,
		localTarget:         newLocalMessageTarget(id, gatherRecvEventChan, gatherRecvCmdChan),
		remoteTargets:       make(map[ServerId]*remoteMessageTarget),
		gatherRecvEventChan: gatherRecvEventChan,
		gatherRecvCmdChan:   gatherRecvCmdChan,
	}
}

// This method will bock untill the grpc server is stopped.
func (mc *MessageCenter) StartGRPCService(addr string) error {
	if mc.grpcServer != nil {
		return nil
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	proto.RegisterMessageCenterServer(grpcServer, &grpcServerImpl{messageCenter: mc})
	grpcServer.Serve(lis)
	return nil
}

func (mc *MessageCenter) StopGRPCService() {
	if mc.grpcServer != nil {
		mc.grpcServer.Stop()
	}
	mc.grpcServer = nil
}

func (mc *MessageCenter) IsGRPCServiceRunning() bool {
	return mc.grpcServer != nil && mc.grpcServer.GetServiceInfo() != nil
}

// If the message channel for the target server is not created, create it.
func (mc *MessageCenter) TouchRemoteServer(id ServerId, addr string, timeout time.Duration) error {
	if _, ok := mc.remoteTargets[id]; ok {
		return nil
	}
	rt := newRemoteMessageTarget(id)
	if err := rt.initSendEventStreams(mc.localId, addr, timeout); err != nil {
		return err
	}
	mc.remoteTargets[id] = rt
	return nil
}

func (mc *MessageCenter) ReceiveEvent() (*TargetMessage, error) { return <-mc.gatherRecvEventChan, nil }
func (mc *MessageCenter) ReceiveCmd() (*TargetMessage, error)   { return <-mc.gatherRecvCmdChan, nil }

func (mc *MessageCenter) GetReceiveTarget(id ServerId) ReceiveMessageChannel { return mc }
func (mc *MessageCenter) GetSendTarget(id ServerId) SendMessageChannel {
	if id == mc.localId {
		return mc.localTarget
	}
	if rt, ok := mc.remoteTargets[id]; ok {
		return rt
	}
	return nil
}
