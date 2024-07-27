package messaging

import (
	"context"
	"fmt"
	"sync"

	"github.com/flowbehappy/tigate/pkg/apperror"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/messaging/proto"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// MessageCenter is the interface to send and receive messages to/from other targets.
// Note: Methods of MessageCenter and MessageSender are thread-safe.
// AddTarget and RemoveTarget are not thread-safe, and should be called in the main thread of a server.
type MessageCenter interface {
	MessageSender
	MessageReceiver
	AddTarget(id ServerId, epoch common.EpochType, addr common.AddressType)
	RemoveTarget(id ServerId)
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
	// ReceiveEvent() (*TargetMessage, error)
	// ReceiveCmd() (*TargetMessage, error)
	RegisterHandler(topic common.TopicType, handler MessageHandler)
	DeRegisterHandler(topic common.TopicType)
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

// messageCenter is the core of the messaging system.
// It hosts a local grpc server to receive messages (events and commands) from other targets (server).
// It hosts streaming channels to each other targets to send messages.
// Events and commands are sent by different channels.
//
// If the target is a remote server(the other process), the messages will be sent to the target by grpc streaming channel.
// If the target is the local (the same process), the messages will be sent to the local by golang channel directly.
//
// TODO: Currently, for each target, we only use one channel to send events.
// We might use multiple channels later.
type messageCenter struct {
	// The server id of the message center
	id ServerId
	// The current epoch of the message center,
	// when every time the message center is restarted, the epoch will be increased by 1.
	epoch common.EpochType
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
	cancel         context.CancelFunc

	targetGauge prometheus.Gauge
	streamGauge prometheus.Gauge
}

func NewMessageCenter(ctx context.Context, id ServerId, epoch common.EpochType, cfg *config.MessageCenterConfig) *messageCenter {
	receiveEventCh := make(chan *TargetMessage, cfg.CacheChannelSize)
	receiveCmdCh := make(chan *TargetMessage, cfg.CacheChannelSize)
	ctx, cancel := context.WithCancel(ctx)
	mc := &messageCenter{
		id:             id,
		epoch:          epoch,
		cfg:            cfg,
		localTarget:    newLocalMessageTarget(id, receiveEventCh, receiveCmdCh),
		receiveEventCh: receiveEventCh,
		receiveCmdCh:   receiveCmdCh,
		cancel:         cancel,
		wg:             &sync.WaitGroup{},
		router:         newRouter(),
	}
	mc.remoteTargets.m = make(map[ServerId]*remoteMessageTarget)
	mc.router.runDispatch(ctx, mc.wg, mc.receiveEventCh)
	mc.router.runDispatch(ctx, mc.wg, mc.receiveCmdCh)
	log.Info("Create message center success, message router is running.", zap.Stringer("id", id), zap.Any("epoch", epoch))
	return mc
}

func (mc *messageCenter) RegisterHandler(topic common.TopicType, handler MessageHandler) {
	mc.router.registerHandler(topic, handler)
}

func (mc *messageCenter) DeRegisterHandler(topic common.TopicType) {
	mc.router.deRegisterHandler(topic)
}

// AddTarget is called when a new remote target is discovered,
// to add the target to the message center.
func (mc *messageCenter) AddTarget(id ServerId, epoch common.EpochType, addr common.AddressType) {
	// If the target is the message center itself, we don't need to add it.
	if id == mc.id {
		log.Info("Add local target", zap.Stringer("id", id), zap.Any("epoch", epoch), zap.Any("addr", addr))
		return
	}
	log.Info("Add remote target", zap.Stringer("local", mc.id), zap.Stringer("remote", id), zap.Any("epoch", epoch), zap.Any("addr", addr))
	rt := mc.touchRemoteTarget(id, epoch, addr)
	rt.connect()
}

func (mc *messageCenter) RemoveTarget(id ServerId) {
	mc.remoteTargets.Lock()
	defer mc.remoteTargets.Unlock()
	if target, ok := mc.remoteTargets.m[id]; ok {
		log.Info("remove remote target from message center", zap.Stringer("local", mc.id), zap.Stringer("remote", id))
		target.close()
		delete(mc.remoteTargets.m, id)
	}
}

func (mc *messageCenter) SendEvent(msg ...*TargetMessage) error {
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

func (mc *messageCenter) SendCommand(cmd ...*TargetMessage) error {
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

func (mc *messageCenter) ReceiveEvent() (*TargetMessage, error) {
	return <-mc.receiveEventCh, nil
}

func (mc *messageCenter) ReceiveCmd() (*TargetMessage, error) {
	return <-mc.receiveCmdCh, nil
}

// Close stops the grpc server and stops all the connections to the remote targets.
func (mc *messageCenter) Close() {
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
func (mc *messageCenter) touchRemoteTarget(id ServerId, epoch common.EpochType, addr common.AddressType) *remoteMessageTarget {
	mc.remoteTargets.Lock()
	defer mc.remoteTargets.Unlock()
	if target, ok := mc.remoteTargets.m[id]; ok {
		if target.Epoch() >= epoch {
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
			zap.Any("oldEpoch", target.Epoch()),
			zap.Any("newEpoch", epoch),
			zap.Any("oldAddr", target.targetAddr),
			zap.Any("newAddr", addr))
		target.close()
		delete(mc.remoteTargets.m, id)
	}
	rt := newRemoteMessageTarget(mc.id, id, mc.epoch, epoch, addr, mc.receiveEventCh, mc.receiveCmdCh, mc.cfg)
	mc.remoteTargets.m[id] = rt
	return rt
}

// grpcServer implements the gRPC `service MessageCenter` defined in the proto file
// It handles the gRPC requests from the clients,
// and then calls the methods in MessageCenter struct to handle the requests.
type grpcServer struct {
	proto.UnimplementedMessageCenterServer
	messageCenter *messageCenter
}

func NewMessageCenterServer(mc MessageCenter) proto.MessageCenterServer {
	return &grpcServer{messageCenter: mc.(*messageCenter)}
}

// SendEvents implements the gRPC service MessageCenter.SendEvents
func (s *grpcServer) SendEvents(msg *proto.Message, stream proto.MessageCenter_SendEventsServer) error {
	metricsStreamGauge := metrics.MessagingStreamGauge.WithLabelValues(msg.GetFrom())
	metricsStreamGauge.Inc()
	defer metricsStreamGauge.Dec()
	return s.handleConnect(msg, stream, true)
	//return s.handleClientConnect(stream, true)
}

// SendCommands implements the gRPC service MessageCenter.SendCommands
func (s *grpcServer) SendCommands(msg *proto.Message, stream proto.MessageCenter_SendCommandsServer) error {
	metricsStreamGauge := metrics.MessagingStreamGauge.WithLabelValues(msg.GetFrom())
	metricsStreamGauge.Inc()
	return s.handleConnect(msg, stream, false)
	//return s.handleClientConnect(stream, false)
}

func (s *grpcServer) id() ServerId {
	return s.messageCenter.id
}

// handleConnect registers the client as a target in the message center.
// So the message center can receive messages from the client.
func (s *grpcServer) handleConnect(msg *proto.Message, stream grpcSender, isEvent bool) error {
	// The first message is an empty message without payload, to identify the client server id.
	to := ServerId(msg.To)
	if to != s.id() {
		err := apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %s not found", to)}
		log.Error("Target not found", zap.Error(err))
		return err
	}
	targetId := ServerId(msg.From)

	s.messageCenter.remoteTargets.RLock()
	defer s.messageCenter.remoteTargets.RUnlock()
	remoteTarget, ok := s.messageCenter.remoteTargets.m[targetId]
	if ok {
		log.Info("Start to sent message to remote target",
			zap.Any("messageCenterID", s.messageCenter.id),
			zap.String("remote", msg.From),
			zap.Bool("isEvent", isEvent))
		// The handshake message's epoch should be the same as the target's epoch.
		if common.EpochType(msg.Epoch) != remoteTarget.Epoch() {
			err := apperror.AppError{Type: apperror.ErrorTypeEpochMismatch, Reason: fmt.Sprintf("Target %s epoch mismatch, expect %d, got %d", targetId, remoteTarget.Epoch(), msg.Epoch)}
			log.Error("Epoch mismatch", zap.Error(err))
			return err
		}
		if isEvent {
			return remoteTarget.runEventSendStream(stream)
		} else {
			return remoteTarget.runCommandSendStream(stream)
		}
	} else {
		log.Info("Remote target not found", zap.Any("messageCenter", s.messageCenter.id), zap.Any("remote", targetId))
		err := &apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %s not found", targetId)}
		// merr := NewMessageError(err)
		// pMsg := &proto.Message{
		// 	From:  s.messageCenter.id.String(),
		// 	To:    targetId.String(),
		// 	Epoch: s.messageCenter.epoch,
		// 	Type:  int32(TypeMessageError),
		// }
		// buf, _ := merr.Marshal()
		// pMsg.Payload = append(pMsg.Payload, buf)
		// if err := stream.Send(pMsg); err != nil {
		// 	log.Error("Failed to send message error", zap.Error(err))
		// }
		return err
	}
}
