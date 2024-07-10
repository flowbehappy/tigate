package messaging

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/messaging/proto"
	"github.com/phayes/freeport"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func newMessageCenterForTest(t *testing.T, timeout time.Duration) (mc *messageCenterImpl, addr string, stop func()) {
	port := freeport.GetPort()
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	mcConfig := config.NewDefaultMessageCenterConfig()
	id := NewServerId()
	mc = NewMessageCenter(id, uint64(1), mcConfig)
	mcs := NewMessageCenterServer(mc)
	proto.RegisterMessageCenterServer(grpcServer, mcs)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = grpcServer.Serve(lis)
	}()

	timeoutCh := time.After(timeout)
	stop = func() {
		<-timeoutCh
		log.Info("Server has been running for timeout duration, force to stop it",
			zap.String("addr", addr),
			zap.Duration("timeout", timeout))
		grpcServer.Stop()
		wg.Wait()
	}
	return
}

func TestMessageCenterBasic(t *testing.T) {
	mc1, mc1Addr, mc1Stop := newMessageCenterForTest(t, time.Second*5)
	mc2, mc2Addr, mc2Stop := newMessageCenterForTest(t, time.Second*5)
	defer mc1Stop()
	defer mc2Stop()

	mc1.AddTarget(mc2.id, mc2.epoch, mc2Addr)
	ch1 := make(chan *TargetMessage, 1)
	h1 := func(msg *TargetMessage) error {
		ch1 <- msg
		log.Info("mc1 received message", zap.Any("msg", msg))
		return nil
	}
	mc1.RegisterHandler("test1", h1)

	mc2.AddTarget(mc1.id, mc1.epoch, mc1Addr)
	ch2 := make(chan *TargetMessage, 1)
	h2 := func(msg *TargetMessage) error {
		ch2 <- msg
		log.Info("mc2 received message", zap.Any("msg", msg))
		return nil
	}
	mc2.RegisterHandler("test2", h2)

	//Case1: Send a message from mc1 to mc1, local message.
	msgBytes := []byte{1, 2, 3, 4}
	msg := Bytes(msgBytes)
	targetMsg := NewTargetMessage(mc1.id, "test1", TypeBytes, msg)
	mc1.SendEvent(targetMsg)
	receivedMsg := <-ch1
	require.Equal(t, targetMsg.To, receivedMsg.To)
	require.Equal(t, targetMsg.From, receivedMsg.From)
	require.Equal(t, targetMsg.Type, receivedMsg.Type)
	require.Equal(t, targetMsg.Message, receivedMsg.Message)
	log.Info("Pass test 1: send and receive local message", zap.Any("receivedMsg", receivedMsg))

	//Case2: Send a message from mc1 to mc2, remote message.
	msgBytes = []byte{5, 6, 7, 8}
	msg = Bytes(msgBytes)
	targetMsg = NewTargetMessage(mc2.id, "test2", TypeBytes, msg)
	mc1.SendEvent(targetMsg)
	receivedMsg = <-ch2
	require.Equal(t, targetMsg.To, receivedMsg.To)
	require.Equal(t, mc1.id, receivedMsg.From)
	require.Equal(t, targetMsg.Type, receivedMsg.Type)
	require.Equal(t, targetMsg.Message, receivedMsg.Message)
	log.Info("Pass test 2: send and receive remote message", zap.Any("receivedMsg", receivedMsg))
}
