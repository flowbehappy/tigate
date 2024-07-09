package messaging

import (
	"fmt"
	"io"

	"github.com/flowbehappy/tigate/pkg/apperror"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/messaging/proto"
	"google.golang.org/grpc"
)

// MessageCenter is the interface to send and receive messages to/from other targets.
// Note(dongmen): All the methods in the interface should be thread-safe.
type MessageCenter interface {
	MessageSender
	MessageReceiver
	AddTarget(id ServerId, addr string)
	RemoveTarget(id ServerId)
	//GetTarget(id ServerId) MessageSender
	Close()
}

// MessageSender is the interface for sending messages to the target.
// The method in the interface should be thread-safe and non-blocking.
// If the message cannot be sent, the method will return an `ErrorTypeMessageCongested` error.
type MessageSender interface {
	// TODO(dongmen): Make these methods support timeout later.
	SendEvent(msg ...*TargetMessage) error
	SendCommand(cmd ...*TargetMessage) error
}

// MessageReceiver is the interface to receive messages from other targets.
type MessageReceiver interface {
	ReceiveEvent() (*TargetMessage, error)
	ReceiveCmd() (*TargetMessage, error)
}

// gRPC generates two different interfaces, MessageCenter_SendEventsServer
// and MessageCenter_SendCommandsServer.
// We use these two interfaces to unite them, to simplify the code.
type grpcReceiver interface {
	Recv() (*proto.Message, error)
	SendAndClose(*proto.MessageSummary) error
}

type grpcSender interface {
	Send(*proto.Message) error
	CloseAndRecv() (*proto.MessageSummary, error)
}

// messageCenterImpl is the core of the messaging system.
// It hosts a local grpc server to receive messages (events and commands) from other targets (server).
// It hosts streaming channels to each other targets to send messages.
// Events and commands are sent by different channels.
//
// If the target is a remote server(the other process), the messages will be sent to the target by grpc streaming channel.
// If the target is the local (the same process), the messages will be sent to the local by golang channel directly.
//
// TODO: Currently, for each target, we only use one channel to send events.
// We might use multiple channels later.
type messageCenterImpl struct {
	// The server id of the message center
	id ServerId
	// The current epoch of the message center,
	// when every time the message center is restarted, the epoch will be increased by 1.
	epoch uint64
	cfg   *config.MessageCenterConfig
	// The local target, which is the message center itself.
	localTarget *localMessageTarget
	// The remote targets, which are the other message centers in remote servers.
	remoteTargets map[ServerId]*remoteMessageTarget

	grpcServer *grpc.Server

	// Messages from all targets are put into these channels.
	receiveEventCh chan *TargetMessage
	receiveCmdCh   chan *TargetMessage
}

func NewMessageCenter(id ServerId, epoch uint64, cfg *config.MessageCenterConfig) *messageCenterImpl {
	receiveEventCh := make(chan *TargetMessage, cfg.CacheChannelSize)
	receiveCmdCh := make(chan *TargetMessage, cfg.CacheChannelSize)
	return &messageCenterImpl{
		id:             id,
		epoch:          epoch,
		cfg:            cfg,
		localTarget:    newLocalMessageTarget(id, receiveEventCh, receiveCmdCh),
		remoteTargets:  make(map[ServerId]*remoteMessageTarget),
		receiveEventCh: receiveEventCh,
		receiveCmdCh:   receiveCmdCh,
	}
}

// AddTarget is called when a new remote target is discovered,
// to add the target to the message center.
func (mc *messageCenterImpl) AddTarget(id ServerId, addr string) {
	// If the target is the message center itself, we don't need to add it.
	if id == mc.id {
		return
	}
	rt := mc.touchRemoteTarget(id)
	rt.targetAddr = addr
	rt.initSendStreams()
}

func (mc *messageCenterImpl) RemoveTarget(id ServerId) {
	if rt, ok := mc.remoteTargets[id]; ok {
		rt.close()
		delete(mc.remoteTargets, id)
	}
}

func (mc *messageCenterImpl) SendEvent(msg ...*TargetMessage) error {
	if len(msg) == 0 {
		return nil
	}

	to := msg[0].To

	if to == mc.id {
		return mc.localTarget.sendEvent(msg...)
	}

	target, ok := mc.remoteTargets[to]
	if !ok {
		return apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %d not found", to)}
	}
	return target.sendEvent(msg...)
}

func (mc *messageCenterImpl) SendCommand(cmd ...*TargetMessage) error {
	if len(cmd) == 0 {
		return nil
	}

	to := cmd[0].To

	if to == mc.id {
		return mc.localTarget.sendCommand(cmd...)
	}

	target, ok := mc.remoteTargets[to]
	if !ok {
		return apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %d not found", to)}
	}
	return target.sendCommand(cmd...)
}

func (mc *messageCenterImpl) ReceiveEvent() (*TargetMessage, error) {
	return <-mc.receiveEventCh, nil
}

func (mc *messageCenterImpl) ReceiveCmd() (*TargetMessage, error) {
	return <-mc.receiveCmdCh, nil
}

// Close stops the grpc server and stops all the connections to the remote targets.
func (mc *messageCenterImpl) Close() {
	for _, rt := range mc.remoteTargets {
		rt.close()
	}
	if mc.grpcServer != nil {
		mc.grpcServer.Stop()
	}
	mc.grpcServer = nil
}

// touchRemoteTarget returns the remote target by the id,
// if the target is not found, it will create a new one.
func (mc *messageCenterImpl) touchRemoteTarget(id ServerId) *remoteMessageTarget {
	if rt, ok := mc.remoteTargets[id]; ok {
		return rt
	}
	rt := newRemoteMessageTarget(mc.id, id, mc.epoch, mc.cfg)
	mc.remoteTargets[id] = rt
	return rt
}

func (mc *messageCenterImpl) addRemoteReceiveTarget(id ServerId, stream grpcReceiver, isEvent bool) {
	rt := mc.touchRemoteTarget(id)
	if isEvent {
		rt.setEventRecvStream(stream)
	} else {
		rt.setCommandRecvStream(stream)
	}
}

// grpcServerImpl implements the gRPC `service MessageCenter` defined in the proto file
// It handles the gRPC requests from the clients,
// and then calls the methods in MessageCenter struct to handle the requests.
type grpcServerImpl struct {
	proto.UnimplementedMessageCenterServer
	messageCenter *messageCenterImpl
}

func NewMessageCenterServer(messageCenter MessageCenter) proto.MessageCenterServer {
	mc := messageCenter.(*messageCenterImpl)
	return &grpcServerImpl{messageCenter: mc}
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
