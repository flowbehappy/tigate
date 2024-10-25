package messaging

import (
	"context"
	"fmt"
	"sync"

	"github.com/flowbehappy/tigate/pkg/node"

	"github.com/flowbehappy/tigate/pkg/apperror"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/messaging/proto"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// MessageCenter is the interface to send and receive messages to/from other targets.
// Note: Methods of MessageCenter and MessageSender are thread-safe.
// OnNodeChanges is not thread-safe, and should be called in the main thread of a server.
type MessageCenter interface {
	MessageSender
	MessageReceiver
	// OnNodeChanges is called when the nodes in the cluster are changed. The message center should update the target list.
	OnNodeChanges(map[node.ID]*node.Info)
	Close()
}

// MessageSender is the interface for sending messages to the target.
// The method in the interface should be thread-safe and non-blocking.
// If the message cannot be sent, the method will return an `ErrorTypeMessageCongested` error.
type MessageSender interface {
	// TODO: Make these methods support timeout later.
	SendEvent(msg *TargetMessage) error
	SendCommand(cmd *TargetMessage) error
	IsReadyToSend(target node.ID) bool
}

// MessageReceiver is the interface to receive messages from other targets.
type MessageReceiver interface {
	RegisterHandler(topic string, handler MessageHandler)
	DeRegisterHandler(topic string)
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
	id node.ID
	// The current epoch of the message center,
	// when every time the message center is restarted, the epoch will be increased by 1.
	epoch uint64
	cfg   *config.MessageCenterConfig
	// The local target, which is the message center itself.
	localTarget *localMessageTarget
	// The remote targets, which are the other message centers in remote servers.
	remoteTargets struct {
		sync.RWMutex
		m map[node.ID]*remoteMessageTarget
	}

	grpcServer *grpc.Server
	router     *router

	// Messages from all targets are put into these channels.
	receiveEventCh chan *TargetMessage
	receiveCmdCh   chan *TargetMessage
	wg             *sync.WaitGroup
	cancel         context.CancelFunc
}

func NewMessageCenter(ctx context.Context, id node.ID, epoch uint64, cfg *config.MessageCenterConfig) *messageCenter {
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
	mc.remoteTargets.m = make(map[node.ID]*remoteMessageTarget)
	mc.router.runDispatch(ctx, mc.wg, mc.receiveEventCh)
	mc.router.runDispatch(ctx, mc.wg, mc.receiveCmdCh)
	log.Info("create message center success, message router is running.",
		zap.Stringer("id", id), zap.Any("epoch", epoch))
	return mc
}

func (mc *messageCenter) RegisterHandler(topic string, handler MessageHandler) {
	mc.router.registerHandler(topic, handler)
}

func (mc *messageCenter) DeRegisterHandler(topic string) {
	mc.router.deRegisterHandler(topic)
}

func (mc *messageCenter) OnNodeChanges(activeNode map[node.ID]*node.Info) {
	allTarget := make(map[node.ID]bool)
	allTarget[mc.id] = true
	mc.remoteTargets.RLock()
	for id := range mc.remoteTargets.m {
		allTarget[id] = true
	}
	mc.remoteTargets.RUnlock()

	for id, node := range activeNode {
		if _, ok := allTarget[id]; !ok {
			mc.addTarget(node.ID, node.Epoch, node.AdvertiseAddr)
		}
	}
	for id := range allTarget {
		if _, ok := activeNode[id]; !ok {
			mc.removeTarget(id)
		}
	}
}

// AddTarget is called when a new remote target is discovered,
// to add the target to the message center.
func (mc *messageCenter) addTarget(id node.ID, epoch uint64, addr string) {
	// If the target is the message center itself, we don't need to add it.
	if id == mc.id {
		log.Info("Add local target", zap.Stringer("id", id), zap.Any("epoch", epoch), zap.Any("addr", addr))
		return
	}
	log.Info("Add remote target", zap.Stringer("local", mc.id), zap.Stringer("remote", id), zap.Any("epoch", epoch), zap.Any("addr", addr))
	rt := mc.touchRemoteTarget(id, epoch, addr)
	rt.connect()
}

func (mc *messageCenter) removeTarget(id node.ID) {
	mc.remoteTargets.Lock()
	defer mc.remoteTargets.Unlock()
	if target, ok := mc.remoteTargets.m[id]; ok {
		log.Info("remove remote target from message center", zap.Stringer("local", mc.id), zap.Stringer("remote", id))
		target.close()
		delete(mc.remoteTargets.m, id)
	}
}

func (mc *messageCenter) IsReadyToSend(targetID node.ID) bool {
	if targetID == mc.id {
		return true
	}
	mc.remoteTargets.RLock()
	defer mc.remoteTargets.RUnlock()
	target, ok := mc.remoteTargets.m[targetID]
	if !ok {
		return false
	}
	return target.isReadyToSend()
}

func (mc *messageCenter) SendEvent(msg *TargetMessage) error {
	if msg == nil {
		return nil
	}

	if msg.To == mc.id {
		return mc.localTarget.sendEvent(msg)
	}

	mc.remoteTargets.RLock()
	target, ok := mc.remoteTargets.m[msg.To]
	mc.remoteTargets.RUnlock()
	if !ok {
		// If target not found, there are two cases:
		// 1. The target is not been discovered yet.
		// 2. The target is removed.
		// The caller should handle the error correctly.
		// For example, if the target is not been discovered yet, the caller can retry later.
		// If the target is removed, the caller must remove the objects that was sending
		// message to this target to avoid blocking.
		return apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %s not found", msg.To)}
	}
	return target.sendEvent(msg)
}

func (mc *messageCenter) SendCommand(msg *TargetMessage) error {
	if msg == nil {
		return nil
	}

	if msg.To == mc.id {
		return mc.localTarget.sendCommand(msg)
	}

	mc.remoteTargets.RLock()
	target, ok := mc.remoteTargets.m[msg.To]
	mc.remoteTargets.RUnlock()
	if !ok {
		return apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %s not found", msg.To)}
	}
	return target.sendCommand(msg)
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
func (mc *messageCenter) touchRemoteTarget(id node.ID, epoch uint64, addr string) *remoteMessageTarget {
	mc.remoteTargets.Lock()
	defer mc.remoteTargets.Unlock()

	target, ok := mc.remoteTargets.m[id]
	if !ok {
		// If the target is not found, create a new one.
		target = newRemoteMessageTarget(
			mc.id, id, mc.epoch,
			epoch, addr, mc.receiveEventCh,
			mc.receiveCmdCh, mc.cfg)
		mc.remoteTargets.m[id] = target
		return target
	}

	// If the target already exists and the epoch is not old, return the target.
	if target.Epoch() >= epoch {
		log.Info("Remote target already exists", zap.Stringer("id", id))
		return target
	}

	// If the target is old and the address is the same, update its epoch.
	if target.targetAddr == addr {
		log.Info("Remote target already exists, but the epoch is old, update the epoch", zap.Stringer("id", id))
		target.targetEpoch.Store(epoch)
		return target
	}

	// If the target is old and the address is different, close the old target and create a new one.
	log.Info("Remote target changed, creating a new one",
		zap.Stringer("id", id),
		zap.Any("oldEpoch", target.Epoch()),
		zap.Any("newEpoch", epoch),
		zap.Any("oldAddr", target.targetAddr),
		zap.Any("newAddr", addr))
	target.close()
	newTarget := newRemoteMessageTarget(
		mc.id, id, mc.epoch,
		epoch, addr, mc.receiveEventCh,
		mc.receiveCmdCh, mc.cfg)
	mc.remoteTargets.m[id] = newTarget
	return newTarget

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
}

// SendCommands implements the gRPC service MessageCenter.SendCommands
func (s *grpcServer) SendCommands(msg *proto.Message, stream proto.MessageCenter_SendCommandsServer) error {
	metricsStreamGauge := metrics.MessagingStreamGauge.WithLabelValues(msg.GetFrom())
	metricsStreamGauge.Inc()
	return s.handleConnect(msg, stream, false)
}

func (s *grpcServer) id() node.ID {
	return s.messageCenter.id
}

// handleConnect registers the client as a target in the message center.
// So the message center can receive messages from the client.
func (s *grpcServer) handleConnect(msg *proto.Message, stream grpcSender, isEvent bool) error {
	// The first message is an empty message without payload, to identify the client server id.
	to := node.ID(msg.To)
	if to != s.id() {
		err := apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %s not found", to)}
		log.Error("Target not found", zap.Error(err))
		return err
	}
	targetId := node.ID(msg.From)

	s.messageCenter.remoteTargets.RLock()
	remoteTarget, ok := s.messageCenter.remoteTargets.m[targetId]
	s.messageCenter.remoteTargets.RUnlock()

	if !ok {
		log.Info("Remote target not found", zap.Any("messageCenter", s.messageCenter.id), zap.Any("remote", targetId))
		err := &apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %s not found", targetId)}
		return err
	}

	// The handshake message's epoch should be the same as the target's epoch.
	if uint64(msg.Epoch) != remoteTarget.Epoch() {
		err := apperror.AppError{Type: apperror.ErrorTypeEpochMismatch, Reason: fmt.Sprintf("Target %s epoch mismatch, expect %d, got %d", targetId, remoteTarget.Epoch(), msg.Epoch)}
		log.Error("Epoch mismatch", zap.Error(err))
		return err
	}

	log.Info("Start to sent message to remote target",
		zap.Any("messageCenterID", s.messageCenter.id),
		zap.String("remote", msg.From),
		zap.Bool("isEvent", isEvent))

	if isEvent {
		return remoteTarget.runEventSendStream(stream)
	} else {
		return remoteTarget.runCommandSendStream(stream)
	}
}
