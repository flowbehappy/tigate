package main

import (
	"bytes"
	"fmt"
	"net"
	"sync"

	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/messaging/proto"
	"github.com/phayes/freeport"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	err := testMessageCenterBasic()
	if err != nil {
		log.Panic("test message center basic failed", zap.Error(err))
	}
}

func newServer() (mc messaging.MessageCenter, id messaging.ServerId, addr string, cancel func()) {
	port := freeport.GetPort()
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	mcConfig := config.NewDefaultMessageCenterConfig()
	id = messaging.NewServerId()
	mc = messaging.NewMessageCenter(id, uint64(1), mcConfig)

	mcs := messaging.NewMessageCenterServer(mc)
	proto.RegisterMessageCenterServer(grpcServer, mcs)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = grpcServer.Serve(lis)
	}()

	cancel = func() {
		grpcServer.Stop()
		wg.Wait()
	}
	return
}

func testMessageCenterBasic() error {
	s1, s1ID, s1Addr, s1Cancel := newServer()
	s2, s2ID, s2Addr, s2Cancel := newServer()
	defer s1Cancel()
	defer s2Cancel()

	s1.AddTarget(s2ID, s2Addr)
	s2.AddTarget(s1ID, s1Addr)

	// Send a message from s1 to s1, local message.
	msgBytes := []byte{1, 2, 3, 4}
	msg := messaging.Bytes(msgBytes)
	targetMsg := messaging.NewTargetMessage(s1ID, messaging.TypeBytes, msg)
	s1.SendEvent(targetMsg)
	receivedMsg, err := s1.ReceiveEvent()
	if err != nil {
		log.Panic("receive event failed", zap.Error(err))
	}
	if receivedMsg.To != targetMsg.To {
		log.Panic("received message's target is not correct", zap.Any("receivedMsg.To", receivedMsg.To), zap.Any("targetMsg.To", targetMsg.To))
	}
	if receivedMsg.Type != targetMsg.Type {
		log.Panic("received message's type is not correct", zap.Any("receivedMsg.Type", receivedMsg.Type), zap.Any("targetMsg.Type", targetMsg.Type))
	}
	receivedBytes := []byte(receivedMsg.Message.(messaging.Bytes))
	if !bytes.Equal(receivedBytes, msgBytes) {
		log.Panic("received message's message is not correct", zap.Any("receivedBytes", receivedBytes), zap.Any("msgBytes", msgBytes))
	}
	log.Info("Pass test 1: send and receive local message", zap.Any("receivedMsg", receivedMsg))

	// Send a message from s1 to s2, remote message.
	msgBytes = []byte{5, 6, 7, 8}
	msg = messaging.Bytes(msgBytes)
	targetMsg = messaging.NewTargetMessage(s2ID, messaging.TypeBytes, msg)
	s1.SendEvent(targetMsg)
	receivedMsg, err = s2.ReceiveEvent()
	if err != nil {
		log.Panic("receive event failed", zap.Error(err))
	}
	if receivedMsg.To != targetMsg.To {
		log.Panic("received message's target is not correct", zap.Any("receivedMsg.To", receivedMsg.To), zap.Any("targetMsg.To", targetMsg.To))
	}
	if receivedMsg.Type != targetMsg.Type {
		log.Panic("received message's type is not correct", zap.Any("receivedMsg.Type", receivedMsg.Type), zap.Any("targetMsg.Type", targetMsg.Type))
	}
	receivedBytes = []byte(receivedMsg.Message.(messaging.Bytes))
	if !bytes.Equal(receivedBytes, msgBytes) {
		log.Panic("received message's message is not correct", zap.Any("receivedBytes", receivedBytes), zap.Any("msgBytes", msgBytes))
	}
	log.Info("Pass test 2: send and receive remote message", zap.Any("receivedMsg", receivedMsg))

	return nil
}
