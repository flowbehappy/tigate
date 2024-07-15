package messaging

import (
	"context"
	"fmt"
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
	//MessageReceiver
	RegisterHandler(topic string, handler MessageHandler)
	DeRegisterHandler(topic string)
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
// TODO: Seems this interface is unnecessary, we can remove it later?
type MessageReceiver interface {
	ReceiveEvent() (*TargetMessage, error)
	ReceiveCmd() (*TargetMessage, error)
}

// gRPC generates two different interfaces, MessageCenter_SendEventsServer
// and MessageCenter_SendCommandsServer.
// We use these two interfaces to unite them, to simplify the code.
type grpcReceiver interface {
	Recv() (*proto.Message, error)
}

type grpcSender interface {
	Send(*proto.Message) error
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
	remoteTargets struct {
		sync.RWMutex
		m map[ServerId]*remoteMessageTarget
	}

	grpcServer *grpc.Server
	router     *router

	// Messages from all targets are put into these channels.
	receiveEventCh chan *TargetMessage
	receiveCmdCh   chan *TargetMessage
	wg             *sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
}

func NewMessageCenter(id ServerId, epoch uint64, cfg *config.MessageCenterConfig) *messageCenterImpl {
	receiveEventCh := make(chan *TargetMessage, cfg.CacheChannelSize)
	receiveCmdCh := make(chan *TargetMessage, cfg.CacheChannelSize)
	ctx, cancel := context.WithCancel(context.Background())
	mc := &messageCenterImpl{
		id:             id,
		epoch:          epoch,
		cfg:            cfg,
		localTarget:    newLocalMessageTarget(id, receiveEventCh, receiveCmdCh),
		receiveEventCh: receiveEventCh,
		receiveCmdCh:   receiveCmdCh,
		ctx:            ctx,
		cancel:         cancel,
		wg:             &sync.WaitGroup{},
		router:         newRouter(),
	}
	mc.remoteTargets.m = make(map[ServerId]*remoteMessageTarget)
	mc.router.runDispatch(mc.ctx, mc.wg, mc.receiveEventCh)
	mc.router.runDispatch(mc.ctx, mc.wg, mc.receiveCmdCh)
	log.Info("Create message center success, message router is running.", zap.Stringer("id", id), zap.Uint64("epoch", epoch))
	return mc
}

func (mc *messageCenterImpl) RegisterHandler(topic string, handler MessageHandler) {
	mc.router.registerHandler(topic, handler)
}

func (mc *messageCenterImpl) DeRegisterHandler(topic string) {
	mc.router.deRegisterHandler(topic)
}

// AddTarget is called when a new remote target is discovered,
// to add the target to the message center.
func (mc *messageCenterImpl) AddTarget(id ServerId, epoch uint64, addr string) {
	// If the target is the message center itself, we don't need to add it.
	if id == mc.id {
		log.Info("Add local target", zap.Stringer("id", id), zap.Uint64("epoch", epoch), zap.String("addr", addr))
		return
	}
	log.Info("Add remote target", zap.Stringer("local", mc.id), zap.Stringer("remote", id), zap.Uint64("epoch", epoch), zap.String("addr", addr))
	rt := mc.touchRemoteTarget(id, epoch, addr)
	rt.connect()
}

func (mc *messageCenterImpl) RemoveTarget(id ServerId) {
	mc.remoteTargets.Lock()
	defer mc.remoteTargets.Unlock()
	if target, ok := mc.remoteTargets.m[id]; ok {
		target.close()
		delete(mc.remoteTargets.m, id)
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

	mc.remoteTargets.RLock()
	defer mc.remoteTargets.RUnlock()
	target, ok := mc.remoteTargets.m[to]
	if !ok {
		return apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %s not found", to)}
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

	mc.remoteTargets.RLock()
	defer mc.remoteTargets.RUnlock()
	target, ok := mc.remoteTargets.m[to]
	if !ok {
		return apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %s not found", to)}
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
	mc.remoteTargets.RLock()
	defer mc.remoteTargets.RUnlock()
	for _, target := range mc.remoteTargets.m {
		target.close()
	}

	mc.cancel()
	if mc.grpcServer != nil {
		mc.grpcServer.Stop()
	}
	mc.grpcServer = nil
	mc.wg.Wait()
}

// touchRemoteTarget returns the remote target by the id,
// if the target is not found, it will create a new one.
func (mc *messageCenterImpl) touchRemoteTarget(id ServerId, epoch uint64, addr string) *remoteMessageTarget {
	mc.remoteTargets.Lock()
	defer mc.remoteTargets.Unlock()
	if target, ok := mc.remoteTargets.m[id]; ok {
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
		delete(mc.remoteTargets.m, id)
	}
	rt := newRemoteMessageTarget(mc.id, id, mc.epoch, epoch, addr, mc.receiveEventCh, mc.receiveCmdCh, mc.cfg)
	mc.remoteTargets.m[id] = rt
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

func (s *grpcServerImpl) SendEvents(msg *proto.Message, stream proto.MessageCenter_SendEventsServer) error {
	return s.handleConnect(msg, stream, true)
	//return s.handleClientConnect(stream, true)
}

func (s *grpcServerImpl) SendCommands(msg *proto.Message, stream proto.MessageCenter_SendCommandsServer) error {
	return s.handleConnect(msg, stream, false)
	//return s.handleClientConnect(stream, false)
}

// handleConnect registers the client as a target in the message center.
// So the message center can received messages from the client.
func (s *grpcServerImpl) handleConnect(msg *proto.Message, stream grpcSender, isEvent bool) error {
	// The first message is an empty message without payload, to identify the client server id.
	to := ServerId(msg.To)
	if to != s.messageCenter.id {
		err := apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %s not found", to)}
		log.Error("Target not found", zap.Error(err))
		return err
	}
	targetId := ServerId(msg.From)
	if target, ok := s.messageCenter.remoteTargets.m[targetId]; ok {
		log.Info("Start to received message from remote target",
			zap.Stringer("local", s.messageCenter.id),
			zap.String("remote", msg.From),
			zap.Bool("isEvent", isEvent))
		// The handshake message's epoch should be the same as the target's epoch.
		if msg.Epoch != target.targetEpoch.Load() {
			err := apperror.AppError{Type: apperror.ErrorTypeEpochMismatch, Reason: fmt.Sprintf("Target %s epoch mismatch, expect %d, got %d", targetId, target.targetEpoch.Load(), msg.Epoch)}
			log.Error("Epoch mismatch", zap.Error(err))
			return err
		}
		if isEvent {
			return target.runEventSendStream(stream)
		} else {
			return target.runCommandSendStream(stream)
		}
	} else {
		log.Info("Remote target not found", zap.Stringer("local", s.messageCenter.id), zap.Stringer("remote", targetId))
		err := &apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %s not found", targetId)}
		merr := NewMessageError(err)
		pMsg := &proto.Message{
			From:  s.messageCenter.id.String(),
			To:    targetId.String(),
			Epoch: s.messageCenter.epoch,
			Type:  int32(TypeMessageError),
		}
		buf, _ := merr.Marshal()
		pMsg.Payload = append(pMsg.Payload, buf)
		if err := stream.Send(pMsg); err != nil {
			log.Error("Failed to send message error", zap.Error(err))
		}
		return err
	}
}
