package messaging

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	. "github.com/flowbehappy/tigate/apperror"
	"github.com/flowbehappy/tigate/messaging/proto"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

const (
	reconnectInterval = 2 * time.Second
)

// TODO(dongmen): Reduce the goroutine number it spawns.
// Currently it spawns 2 goroutines for each remote target, and 2 goroutines for each local target,
// and 1 goroutine to handle grpc stream error.
type remoteMessageTarget struct {
	localId    ServerId
	targetId   ServerId
	targetAddr string

	// For sending events and commands
	conn              *grpc.ClientConn
	eventSendStream   proto.MessageCenter_SendEventsClient
	commandSendStream proto.MessageCenter_SendCommandsClient

	// For receiving events and commands
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

func newRemoteMessageTarget(id ServerId) *remoteMessageTarget {
	ctx, cancel := context.WithCancel(context.Background())
	rt := &remoteMessageTarget{
		targetId:    id,
		ctx:         ctx,
		cancel:      cancel,
		sendEventCh: make(chan *proto.Message, chanSize),
		sendCmdCh:   make(chan *proto.Message, chanSize),
		recvEventCh: make(chan *TargetMessage, chanSize),
		recvCmdCh:   make(chan *TargetMessage, chanSize),
		errCh:       make(chan AppError, 1),
	}
	rt.runHandleErr(ctx)
	return rt
}

func (s *remoteMessageTarget) close() {
	// Close the old streams
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
	// TODO(dongmen): make it support tls
	dialOptions := []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  time.Second,
				Multiplier: 1.1,
				Jitter:     0.1,
				MaxDelay:   3 * time.Second,
			},
			MinConnectTimeout: 3 * time.Second,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	conn, err := grpc.NewClient(s.targetAddr, dialOptions...)
	if err != nil {
		s.collectErr(AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot create grpc client on address %s, error: %s", s.targetAddr, err.Error())})
	}

	client := proto.NewMessageCenterClient(conn)
	eventStream, err := client.SendEvents(s.ctx)
	if err != nil {
		s.collectErr(AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot open event grpc stream, error: %s", err.Error())})
	}

	handshake := &proto.Message{From: s.localId.slice(), To: s.targetId.slice()}
	if err := eventStream.Send(handshake); err != nil {
		s.collectErr(AppError{
			Type:   ErrorTypeMessageSendFailed,
			Reason: fmt.Sprintf("Cannot send handshake message, error: %s", err.Error())})
	}

	commandStream, err := client.SendCommands(s.ctx)
	if err != nil {
		s.collectErr(AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot open event grpc stream, error: %s", err.Error())})
	}

	if err := commandStream.Send(handshake); err != nil {
		s.collectErr(AppError{
			Type:   ErrorTypeMessageSendFailed,
			Reason: fmt.Sprintf("Cannot send handshake message, error: %s", err.Error())})
	}

	s.conn = conn
	s.eventSendStream = eventStream
	s.commandSendStream = commandStream

	sendCtx, sendCancel := context.WithCancel(s.ctx)
	s.sendMsgCancel = sendCancel
	runSendMessages(sendCtx, eventStream, s.sendEventCh, s.errCh, s.wg)
	runSendMessages(sendCtx, commandStream, s.sendCmdCh, s.errCh, s.wg)

	log.Info("Remote target is connected", zap.Any("targetID", s.targetId), zap.String("targetAddr", s.targetAddr))
}

func (s *remoteMessageTarget) resetSendStreams() {
	// Close the old streams
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}

	s.eventSendStream = nil
	s.commandSendStream = nil
	s.sendMsgCancel()

	for range s.errCh {
		// Drain the error channel
	}

	// Reconnect
	s.initSendStreams()
}

func runReceiveMessages(ctx context.Context, stream grpcReceiver, receiveCh chan *TargetMessage, errCh chan AppError, wg *sync.WaitGroup) {
	// Use a goroutine to pull the messages from the stream,
	// and send them to the message center.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			message, err := stream.Recv()
			if err != nil {
				log.Error("Error when receiving message from remote", zap.Error(err))
				err := AppError{Type: ErrorTypeMessageReceiveFailed, Reason: err.Error()}
				// Close the stream, and the client will reconnect.
				stream.SendAndClose(&proto.MessageSummary{SentBytes: 0})
				select {
				case errCh <- err:
				default:
				}
				return
			}
			for _, payload := range message.Payload {
				m, err := decodeIOType(IOType(message.Type), payload)
				if err != nil {
					log.Panic("Error when deserializing message from remote", zap.Error(err))
				}

				tm := &TargetMessage{From: ServerId(message.From), To: ServerId(message.To), Type: IOType(message.Type), Message: m}
				receiveCh <- tm
			}
		}
	}()
}

func runSendMessages(ctx context.Context, stream grpcSender, sendChan chan *proto.Message, errCh chan AppError, wg *sync.WaitGroup) {
	// Use a goroutine to pull the messages from the sendChan,
	// and send them to the remote target.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case message := <-sendChan:
				if err := stream.Send(message); err != nil {
					if err == io.EOF {
						// The stream is closed by the remote target.
						stream.CloseAndRecv()
						return
					}
					log.Error("Error when sending message to remote", zap.Error(err))
					err := AppError{Type: ErrorTypeMessageSendFailed, Reason: err.Error()}
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}
	}()
}

func (s *remoteMessageTarget) setEventRecvStream(eventStream grpcReceiver) {
	s.eventRecvStream = eventStream
	runReceiveMessages(s.ctx, eventStream, s.recvEventCh, s.errCh, s.wg)
}

func (s *remoteMessageTarget) setCommandRecvStream(commandStream grpcReceiver) {
	s.commandRecvStream = commandStream
	runReceiveMessages(s.ctx, commandStream, s.recvCmdCh, s.errCh, s.wg)
}

func (s *remoteMessageTarget) SendEvent(mtype IOType, eventBytes [][]byte) error {
	if s.eventSendStream == nil {
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: "Stream has not been initialized"}
	}
	message := &proto.Message{From: s.localId.slice(), To: s.targetId.slice(), Type: int32(mtype), Payload: eventBytes}
	select {
	case s.sendEventCh <- message:
		return nil
	default:
		return AppError{Type: ErrorTypeMessageCongested, Reason: "Send event message is congested"}
	}
}

func (s *remoteMessageTarget) SendCommand(mtype IOType, commandBytes [][]byte) error {
	if s.commandSendStream == nil {
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: "Stream has not been initialized"}
	}
	message := &proto.Message{From: s.localId.slice(), To: s.targetId.slice(), Type: int32(mtype), Payload: commandBytes}
	select {
	case s.sendCmdCh <- message:
		return nil
	default:
		return AppError{Type: ErrorTypeMessageCongested, Reason: "Send command message is congested"}
	}
}
