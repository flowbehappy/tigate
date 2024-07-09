package messaging

import (
	"fmt"
	"io"
	"sync"

	"github.com/flowbehappy/tigate/pkg/apperror"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/messaging/proto"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// MessageCenter is the interface to send and receive messages to/from other targets.
// Note: Methods of MessageCenter and MessageSender are thread-safe.
// AddTarget and RemoveTarget are not thread-safe, and should be called in the main thread of a server.
type MessageCenter interface {
	MessageSender
	MessageReceiver
	AddTarget(id ServerId, epoch uint64, addr string)
	RemoveTarget(id ServerId)
	//GetTarget(id ServerId) MessageSender
	Close()
}

// MessageSender is the interface for sending messages to the target.
// The method in the interface should be thread-safe and non-blocking.
// If the message cannot be sent, the method will return an `ErrorTypeMessageCongested` error.
type MessageSender interface {
	// TODO: Make these methods support timeout later.
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
	remoteTargets sync.Map

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
		remoteTargets:  sync.Map{},
		receiveEventCh: receiveEventCh,
		receiveCmdCh:   receiveCmdCh,
	}
}

// AddTarget is called when a new remote target is discovered,
// to add the target to the message center.
func (mc *messageCenterImpl) AddTarget(id ServerId, epoch uint64, addr string) {
	// If the target is the message center itself, we don't need to add it.
	if id == mc.id {
		return
	}
	log.Info("Add remote target", zap.Stringer("id", id), zap.Uint64("epoch", epoch), zap.String("addr", addr))
	rt := mc.touchRemoteTarget(id, epoch, addr)
	rt.initSendStreams()
}

func (mc *messageCenterImpl) RemoveTarget(id ServerId) {
	if target, ok := mc.remoteTargets.LoadAndDelete(id); ok {
		target.(*remoteMessageTarget).close()
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

	target, ok := mc.remoteTargets.Load(to)
	if !ok {
		return apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %d not found", to)}
	}
	return target.(*remoteMessageTarget).sendEvent(msg...)
}

func (mc *messageCenterImpl) SendCommand(cmd ...*TargetMessage) error {
	if len(cmd) == 0 {
		return nil
	}

	to := cmd[0].To

	if to == mc.id {
		return mc.localTarget.sendCommand(cmd...)
	}

	target, ok := mc.remoteTargets.Load(to)
	if !ok {
		return apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %d not found", to)}
	}
	return target.(*remoteMessageTarget).sendCommand(cmd...)
}

func (mc *messageCenterImpl) ReceiveEvent() (*TargetMessage, error) {
	return <-mc.receiveEventCh, nil
}

func (mc *messageCenterImpl) ReceiveCmd() (*TargetMessage, error) {
	return <-mc.receiveCmdCh, nil
}

// Close stops the grpc server and stops all the connections to the remote targets.
func (mc *messageCenterImpl) Close() {
	mc.remoteTargets.Range(func(key, value interface{}) bool {
		value.(*remoteMessageTarget).close()
		return true
	})
	if mc.grpcServer != nil {
		mc.grpcServer.Stop()
	}
	mc.grpcServer = nil
}

// touchRemoteTarget returns the remote target by the id,
// if the target is not found, it will create a new one.
func (mc *messageCenterImpl) touchRemoteTarget(id ServerId, epoch uint64, addr string) *remoteMessageTarget {
	if v, ok := mc.remoteTargets.Load(id); ok {
		target := v.(*remoteMessageTarget)
		if target.targetEpoch.Load() >= epoch {
			log.Info("Remote target already exists", zap.Stringer("id", id))
			return target
		}

		if target.targetAddr == addr {
			log.Info("Remote target already exists, but the epoch is old, update the epoch", zap.Stringer("id", id))
			target.targetEpoch.Store(epoch)
			return target
		}

		log.Info("Remote target epoch and addr changed, close it and create a new one",
			zap.Stringer("id", id),
			zap.Uint64("oldEpoch", target.targetEpoch.Load()),
			zap.Uint64("newEpoch", epoch),
			zap.String("oldAddr", target.targetAddr),
			zap.String("newAddr", addr))
		target.close()
		mc.remoteTargets.Delete(id)
	}

	rt := newRemoteMessageTarget(mc.id, id, mc.epoch, epoch, addr, mc.receiveEventCh, mc.receiveCmdCh, mc.cfg)
	mc.remoteTargets.Store(id, rt)
	return rt
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
	//return s.handleClientConnectInTarget(stream, true)
	return s.handleClientConnect(stream, true)
}

func (s *grpcServerImpl) SendCommands(stream proto.MessageCenter_SendCommandsServer) error {
	//return s.handleClientConnectInTarget(stream, false)
	return s.handleClientConnect(stream, false)
}

// handleClientConnect registers the client as a target in the message center.
// So the message center can received messages from the client.
func (s *grpcServerImpl) handleClientConnect(stream grpcReceiver, isEvent bool) error {
	// The first message is an empty message without payload, to identify the client server id.
	msg, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return stream.SendAndClose(&proto.MessageSummary{SentBytes: 0 /*TODO*/})
		}
		return err
	}
	to := ServerId(msg.To)
	if to != s.messageCenter.id {
		return apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %d not found", to)}
	}

	log.Info("Start to received message from remote target",
		zap.Stringer("local", s.messageCenter.id),
		zap.Stringer("remote", to),
		zap.Bool("isEvent", isEvent))

	var receiveCh chan *TargetMessage
	if isEvent {
		receiveCh = s.messageCenter.receiveEventCh
	} else {
		receiveCh = s.messageCenter.receiveCmdCh
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return stream.SendAndClose(&proto.MessageSummary{SentBytes: 0 /*TODO*/})
			}
			// The client is responsible for reconnecting if it needs to.
			return err
		}

		mt := IOType(msg.Type)
		for _, b := range msg.Payload {
			targetMsg := &TargetMessage{
				From:     ServerId(msg.From),
				To:       ServerId(msg.To),
				Epoch:    msg.Epoch,
				Sequence: msg.Seqnum,
				Type:     mt,
				Message:  decodeIOType(mt, b),
			}
			receiveCh <- targetMsg
		}
	}
}

// handleClientConnect registers the client as a target in the message center.
// So the message center can received messages from the client.
func (s *grpcServerImpl) handleClientConnectInTarget(stream grpcReceiver, isEvent bool) error {
	// The first message is an empty message without payload, to identify the client server id.
	msg, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return stream.SendAndClose(&proto.MessageSummary{SentBytes: 0 /*TODO*/})
		}
		return err
	}
	to := ServerId(msg.To)
	if to != s.messageCenter.id {
		return apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %d not found", to)}
	}

	log.Info("Start to received message from remote target",
		zap.Stringer("local", s.messageCenter.id),
		zap.Stringer("remote", to),
		zap.Bool("isEvent", isEvent))

	from := ServerId(msg.From)
	if v, ok := s.messageCenter.remoteTargets.Load(from); ok {
		log.Info("Remote target found", zap.Stringer("from", from))
		target := v.(*remoteMessageTarget)
		// The handshake message's epoch should be the same as the target's epoch.
		if msg.Epoch != target.targetEpoch.Load() {
			return apperror.AppError{Type: apperror.ErrorTypeEpochMismatch, Reason: fmt.Sprintf("Target %d epoch mismatch, expect %d, got %d", from, target.targetEpoch, msg.Epoch)}
		}
		if isEvent {
			return target.runEventRecvStream(stream)
		} else {
			return target.runCommandRecvStream(stream)
		}
	} else {
		log.Info("Remote target not found", zap.Stringer("from", from))
		return apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %d not found", from)}
	}
}
