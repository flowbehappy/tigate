package messaging

import (
	"context"
	"fmt"
	"io"
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

	// For sending events and commands
	conn              *grpc.ClientConn
	eventSendStream   grpcSender
	commandSendStream grpcSender

	// For receiving events and commands
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
	// sendMsgCancel is used to stop the goroutine spawned by runSendMessages.
	sendMsgCancel context.CancelFunc
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
	log.Info("Create remote target", zap.Stringer("local", localID), zap.Stringer("target", targetId), zap.String("addr", addr), zap.Uint64("localEpoch", localEpoch), zap.Uint64("targetEpoch", targetEpoch))
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
	log.Info("Close remote target", zap.Stringer("local", s.localId), zap.Stringer("target", s.targetId), zap.String("addr", s.targetAddr))
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
				case ErrorTypeMessageSendFailed, ErrorTypeConnectionFailed:
					log.Warn("send message to target failed, will be reconnect", zap.Error(err))
					time.Sleep(reconnectInterval)
					s.resetSendStreams()
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

func (s *remoteMessageTarget) initSendStreams() {
	if s.conn != nil {
		return
	}

	conn, err := conn.Connect(s.targetAddr, &security.Credential{})
	if err != nil {
		s.collectErr(AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot create grpc client on address %s, error: %s", s.targetAddr, err.Error())})
		return
	}

	client := proto.NewMessageCenterClient(conn)
	eventStream, err := client.SendEvents(s.ctx)
	if err != nil {
		s.collectErr(AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot open event grpc stream, error: %s", err.Error())})
		return
	}

	handshake := &proto.Message{From: string(s.localId), To: string(s.targetId), Epoch: s.localEpoch}
	if err := eventStream.Send(handshake); err != nil {
		s.collectErr(AppError{
			Type:   ErrorTypeMessageSendFailed,
			Reason: fmt.Sprintf("Cannot send handshake message, error: %s", err.Error())})
		return
	}

	commandStream, err := client.SendCommands(s.ctx)
	if err != nil {
		s.collectErr(AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot open event grpc stream, error: %s", err.Error())})
		return
	}

	if err := commandStream.Send(handshake); err != nil {
		s.collectErr(AppError{
			Type:   ErrorTypeMessageSendFailed,
			Reason: fmt.Sprintf("Cannot send handshake message, error: %s", err.Error())})
		return
	}

	s.conn = conn
	s.eventSendStream = eventStream
	s.commandSendStream = commandStream

	sendCtx, sendCancel := context.WithCancel(s.ctx)
	s.sendMsgCancel = sendCancel
	s.runSendMessages(sendCtx, eventStream, s.sendEventCh)
	s.runSendMessages(sendCtx, commandStream, s.sendCmdCh)
	log.Info("Connected to remote target",
		zap.Stringer("localID", s.localId),
		zap.Stringer("targetID", s.targetId),
		zap.String("targetAddr", s.targetAddr))
}

func (s *remoteMessageTarget) resetSendStreams() {
	log.Info("Reset send streams",
		zap.Stringer("localID", s.localId),
		zap.Stringer("targetID", s.targetId))
	// Close the old streams
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}

	s.eventSendStream = nil
	s.commandSendStream = nil
	// Cancel the goroutine spawned by runSendMessages
	s.sendMsgCancel()

	for range s.errCh {
		// Drain the error channel
	}

	// Reconnect
	s.initSendStreams()
}

func (s *remoteMessageTarget) runEventRecvStream(eventStream grpcReceiver) error {
	s.eventRecvStream = eventStream
	return s.runReceiveMessages(eventStream, s.recvEventCh)
}

func (s *remoteMessageTarget) runCommandRecvStream(commandStream grpcReceiver) error {
	s.commandRecvStream = commandStream
	return s.runReceiveMessages(commandStream, s.recvCmdCh)
}

func (s *remoteMessageTarget) runSendMessages(sendCtx context.Context, stream grpcSender, sendChan chan *proto.Message) {
	// Use a goroutine to pull the messages from the sendChan,
	// and send them to the remote target.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-sendCtx.Done():
				return
			case message := <-sendChan:
				log.Info("Send message to remote",
					zap.Stringer("localID", s.localId),
					zap.Stringer("targetID", s.targetId),
					zap.Stringer("message", message))
				if err := stream.Send(message); err != nil {
					log.Error("Error when sending message to remote",
						zap.Error(err),
						zap.Stringer("localID", s.localId),
						zap.Stringer("targetID", s.targetId))
					if err == io.EOF {
						// The stream is closed by the remote target.
						stream.CloseAndRecv()
						return
					}
					err := AppError{Type: ErrorTypeMessageSendFailed, Reason: err.Error()}
					s.collectErr(err)
					return
				}
			}
		}
	}()
}

func (s *remoteMessageTarget) runReceiveMessages(stream grpcReceiver, receiveCh chan *TargetMessage) error {
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		default:
		}
		message, err := stream.Recv()
		if err != nil {
			err := AppError{Type: ErrorTypeMessageReceiveFailed, Reason: errors.Trace(err).Error()}
			// return the error to close the stream, the client side is responsible to reconnect.
			return err
		}
		mt := IOType(message.Type)
		for _, payload := range message.Payload {
			msg, err := decodeIOType(mt, payload)
			if err != nil {
				err := AppError{Type: ErrorTypeInvalidMessage, Reason: errors.Trace(err).Error()}
				return err
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
}

func (s *remoteMessageTarget) newMessage(msg ...*TargetMessage) *proto.Message {
	msgBytes := make([][]byte, 0, len(msg))
	for _, m := range msg {
		// TODO: use a buffer pool to reduce the memory allocation.
		buf := make([]byte, 0)
		msgBytes = append(msgBytes, m.encode(buf))
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
