package messaging

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/flowbehappy/tigate/pkg/apperror"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/messaging/proto"
	"github.com/flowbehappy/tigate/utils/conn"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	reconnectInterval = 2 * time.Second
)

// remoteMessageTarget implements the SendMessageChannel interface.
// TODO: Reduce the goroutine number it spawns.
// Currently it spawns 2 goroutines for each remote target, and 2 goroutines for each local target,
// and 1 goroutine to handle grpc stream error.
type remoteMessageTarget struct {
	// The server id of the message center.
	localId ServerId
	// The current localEpoch of the message center.
	localEpoch uint64
	// The next message sendSequence number.
	sendSequence atomic.Uint64

	targetEpoch atomic.Uint64
	targetId    ServerId
	targetAddr  string

	mu sync.RWMutex
	// For sending events and commands
	eventSendStream   grpcSender
	commandSendStream grpcSender

	// For receiving events and commands
	conn              *grpc.ClientConn
	receivedSequence  atomic.Uint64
	eventRecvStream   grpcReceiver
	commandRecvStream grpcReceiver

	// We push the events and commands to remote send streams.
	// The send streams are created when the target is added to the message center.
	// These channels are used to cache the messages before sending.
	sendEventCh chan *proto.Message
	sendCmdCh   chan *proto.Message

	// We pull the events and commands from remote receive streams,
	// and push to the message center.
	recvEventCh chan *TargetMessage
	recvCmdCh   chan *TargetMessage

	wg *sync.WaitGroup
	// ctx is used to create the grpc stream.
	ctx context.Context
	// cancel is used to stop the grpc stream, and the goroutine spawned by remoteMessageTarget.
	cancel context.CancelFunc
	// errCh is used to gether the error from the goroutine spawned by remoteMessageTarget.
	errCh chan AppError
}

func (s *remoteMessageTarget) sendEvent(msg ...*TargetMessage) error {
	if s.eventSendStream == nil {
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: "Stream has not been initialized"}
	}
	select {
	case <-s.ctx.Done():
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: "Stream has been closed"}
	case s.sendEventCh <- s.newMessage(msg...):
		return nil
	default:
		return AppError{Type: ErrorTypeMessageCongested, Reason: "Send event message is congested"}
	}
}

func (s *remoteMessageTarget) sendCommand(msg ...*TargetMessage) error {
	if s.commandSendStream == nil {
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: "Stream has not been initialized"}
	}
	select {
	case <-s.ctx.Done():
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: "Stream has been closed"}
	case s.sendCmdCh <- s.newMessage(msg...):
		return nil
	default:
		return AppError{Type: ErrorTypeMessageCongested, Reason: "Send command message is congested"}
	}
}

func newRemoteMessageTarget(
	localID, targetId ServerId,
	localEpoch, targetEpoch uint64,
	addr string,
	recvEventCh, recvCmdCh chan *TargetMessage,
	cfg *config.MessageCenterConfig) *remoteMessageTarget {
	log.Info("Create remote target", zap.Stringer("local", localID), zap.Stringer("remote", targetId), zap.String("addr", addr), zap.Uint64("localEpoch", localEpoch), zap.Uint64("targetEpoch", targetEpoch))
	ctx, cancel := context.WithCancel(context.Background())
	rt := &remoteMessageTarget{
		localId:     localID,
		localEpoch:  localEpoch,
		targetAddr:  addr,
		targetId:    targetId,
		ctx:         ctx,
		cancel:      cancel,
		sendEventCh: make(chan *proto.Message, cfg.CacheChannelSize),
		sendCmdCh:   make(chan *proto.Message, cfg.CacheChannelSize),
		recvEventCh: recvEventCh,
		recvCmdCh:   recvCmdCh,
		errCh:       make(chan AppError, 1),
		wg:          &sync.WaitGroup{},
	}
	rt.targetEpoch.Store(targetEpoch)
	rt.runHandleErr(ctx)
	return rt
}

// close stops the grpc stream and the goroutine spawned by remoteMessageTarget.
func (s *remoteMessageTarget) close() {
	log.Info("Close remote target", zap.Stringer("local", s.localId), zap.Stringer("remote", s.targetId), zap.String("addr", s.targetAddr))
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
	s.cancel()
	s.wg.Wait()
}

func (s *remoteMessageTarget) runHandleErr(ctx context.Context) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-s.errCh:
				switch err.Type {
				case ErrorTypeMessageReceiveFailed, ErrorTypeConnectionFailed:
					log.Warn("received message from remote failed, will be reconnect", zap.Stringer("local", s.localId), zap.Stringer("remote", s.targetId), zap.Error(err))
					time.Sleep(reconnectInterval)
					s.resetConnect()
				default:
					log.Error("Error in remoteMessageTarget, error:", zap.Error(err))
				}

			}
		}
	}()
}

func (s *remoteMessageTarget) collectErr(err AppError) {
	select {
	case s.errCh <- err:
	default:
	}
}

func (s *remoteMessageTarget) connect() {
	if s.conn != nil {
		return
	}
	conn, err := conn.Connect(s.targetAddr, &security.Credential{})
	if err != nil {
		log.Info("Cannot create grpc client", zap.Stringer("local", s.localId), zap.Stringer("remote", s.targetId), zap.Error(err))
		s.collectErr(AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot create grpc client on address %s, error: %s", s.targetAddr, err.Error())})
		return
	}

	client := proto.NewMessageCenterClient(conn)
	handshake := &proto.Message{From: string(s.localId), To: string(s.targetId), Epoch: s.localEpoch, Type: int32(TypeMessageHandShake)}

	eventStream, err := client.SendEvents(s.ctx, handshake)
	if err != nil {
		log.Info("Cannot establish event grpc stream", zap.Stringer("local", s.localId), zap.Stringer("remote", s.targetId), zap.Error(err))
		s.collectErr(AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot open event grpc stream, error: %s", err.Error())})
		return
	}

	commandStream, err := client.SendCommands(s.ctx, handshake)
	if err != nil {
		log.Info("Cannot establish command grpc stream", zap.Stringer("local", s.localId), zap.Stringer("remote", s.targetId), zap.Error(err))
		s.collectErr(AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot open event grpc stream, error: %s", err.Error())})
		return
	}

	s.conn = conn
	s.eventRecvStream = eventStream
	s.commandRecvStream = commandStream
	s.runReceiveMessages(eventStream, s.recvEventCh)
	s.runReceiveMessages(commandStream, s.recvCmdCh)
	log.Info("Connected to remote target",
		zap.Stringer("local", s.localId),
		zap.Stringer("remote", s.targetId),
		zap.String("remoteAddr", s.targetAddr))
}

func (s *remoteMessageTarget) resetConnect() {
	log.Info("reconnect to remote target",
		zap.Stringer("local", s.localId),
		zap.Stringer("remote", s.targetId))
	// Close the old streams
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}

	s.eventRecvStream = nil
	s.commandRecvStream = nil
	for range s.errCh {
		// Drain the error channel
	}
	// Reconnect
	s.connect()
}

func (s *remoteMessageTarget) runEventSendStream(eventStream grpcSender) error {
	s.eventSendStream = eventStream
	return s.runSendMessages(s.ctx, eventStream, s.sendEventCh)
}

func (s *remoteMessageTarget) runCommandSendStream(commandStream grpcSender) error {
	s.commandSendStream = commandStream
	return s.runSendMessages(s.ctx, commandStream, s.sendCmdCh)
}

func (s *remoteMessageTarget) runSendMessages(sendCtx context.Context, stream grpcSender, sendChan chan *proto.Message) error {
	for {
		select {
		case <-sendCtx.Done():
			return sendCtx.Err()
		case message := <-sendChan:
			log.Debug("Send message to remote",
				zap.Stringer("local", s.localId),
				zap.Stringer("remote", s.targetId),
				zap.Stringer("message", message))
			if err := stream.Send(message); err != nil {
				log.Error("Error when sending message to remote",
					zap.Error(err),
					zap.Stringer("local", s.localId),
					zap.Stringer("remote", s.targetId))
				err := AppError{Type: ErrorTypeMessageSendFailed, Reason: err.Error()}
				return err
			}
		}
	}
}

func (s *remoteMessageTarget) runReceiveMessages(stream grpcReceiver, receiveCh chan *TargetMessage) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-s.ctx.Done():
				return
			default:
			}
			message, err := stream.Recv()
			if err != nil {
				err := AppError{Type: ErrorTypeMessageReceiveFailed, Reason: errors.Trace(err).Error()}
				// return the error to close the stream, the client side is responsible to reconnect.
				s.collectErr(err)
				return
			}
			mt := IOType(message.Type)
			if mt == TypeMessageHandShake {
				log.Info("Received handshake message", zap.Stringer("local", s.localId), zap.Stringer("remote", s.targetId))
				continue
			}
			for _, payload := range message.Payload {
				msg, err := decodeIOType(mt, payload)
				if err != nil {
					// TODO: handle this error properly.
					err := AppError{Type: ErrorTypeInvalidMessage, Reason: errors.Trace(err).Error()}
					log.Warn("Failed to decode message", zap.Error(err))
					continue
				}
				receiveCh <- &TargetMessage{
					From:     ServerId(message.From),
					To:       ServerId(message.To),
					Topic:    message.Topic,
					Epoch:    message.Epoch,
					Sequence: message.Seqnum,
					Type:     mt,
					Message:  msg,
				}
			}
		}
	}()
}

func (s *remoteMessageTarget) newMessage(msg ...*TargetMessage) *proto.Message {
	msgBytes := make([][]byte, 0, len(msg))
	for _, m := range msg {
		// TODO: use a buffer pool to reduce the memory allocation.
		buf, err := m.Message.Marshal()
		if err != nil {
			log.Panic("marshal message failed ",
				zap.Any("msg", m),
				zap.Error(err))
		}
		msgBytes = append(msgBytes, buf)
	}
	protoMsg := &proto.Message{
		From:    string(s.localId),
		To:      string(s.targetId),
		Epoch:   s.localEpoch,
		Topic:   msg[0].Topic,
		Seqnum:  s.sendSequence.Add(1),
		Type:    int32(msg[0].Type),
		Payload: msgBytes,
	}
	return protoMsg
}

// localMessageTarget implements the SendMessageChannel interface.
// It is used to send messages to the local server.
// It simply pushes the messages to the messageCenter's channel directly.
type localMessageTarget struct {
	localId  ServerId
	epoch    uint64
	sequence atomic.Uint64

	// The gather channel from the message center.
	// We only need to push and pull the messages from those channel.
	recvEventCh chan *TargetMessage
	recvCmdCh   chan *TargetMessage
}

func (s *localMessageTarget) sendEvent(msg ...*TargetMessage) error {
	return s.sendMsgToChan(s.recvEventCh, msg...)
}

func (s *localMessageTarget) sendCommand(msg ...*TargetMessage) error {
	return s.sendMsgToChan(s.recvCmdCh, msg...)
}

func newLocalMessageTarget(id ServerId,
	gatherRecvEventChan chan *TargetMessage,
	gatherRecvCmdChan chan *TargetMessage) *localMessageTarget {
	return &localMessageTarget{
		localId:     id,
		recvEventCh: gatherRecvEventChan,
		recvCmdCh:   gatherRecvCmdChan,
	}
}

func (s *localMessageTarget) sendMsgToChan(ch chan *TargetMessage, msg ...*TargetMessage) error {
	for _, m := range msg {
		m.To = s.localId
		m.From = s.localId
		m.Epoch = s.epoch
		m.Sequence = s.sequence.Add(1)
		select {
		case ch <- m:
		default:
			return AppError{Type: ErrorTypeMessageCongested, Reason: "Send message is congested"}
		}
	}
	return nil
}
