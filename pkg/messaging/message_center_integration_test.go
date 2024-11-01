package messaging

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common/event"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/node"

	"github.com/phayes/freeport"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging/proto"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var mockEpoch = uint64(1)

func NewMessageCenterForTest(t *testing.T) (*messageCenter, string, func()) {
	port := freeport.GetPort()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	ctx, cancel := context.WithCancel(context.Background())
	mcConfig := config.NewDefaultMessageCenterConfig()
	id := node.NewID()
	mc := NewMessageCenter(ctx, id, mockEpoch, mcConfig)
	mockEpoch++
	mcs := NewMessageCenterServer(mc)
	proto.RegisterMessageCenterServer(grpcServer, mcs)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = grpcServer.Serve(lis)
	}()

	stop := func() {
		grpcServer.Stop()
		cancel()
		wg.Wait()
	}
	return mc, string(addr), stop
}

func setupMessageCenters(t *testing.T) (*messageCenter, *messageCenter, *messageCenter, func()) {
	mc1, mc1Addr, mc1Stop := NewMessageCenterForTest(t)
	mc2, mc2Addr, mc2Stop := NewMessageCenterForTest(t)
	mc3, mc3Addr, mc3Stop := NewMessageCenterForTest(t)

	mc1.addTarget(mc2.id, mc2.epoch, mc2Addr)
	mc1.addTarget(mc3.id, mc3.epoch, mc3Addr)
	mc2.addTarget(mc1.id, mc1.epoch, mc1Addr)
	mc2.addTarget(mc3.id, mc3.epoch, mc3Addr)
	mc3.addTarget(mc1.id, mc1.epoch, mc1Addr)
	mc3.addTarget(mc2.id, mc2.epoch, mc2Addr)

	cleanup := func() {
		mc1Stop()
		mc2Stop()
		mc3Stop()
	}

	return mc1, mc2, mc3, cleanup
}

func registerHandler(mc *messageCenter, topic string) chan *TargetMessage {
	ch := make(chan *TargetMessage, 1)
	mc.RegisterHandler(topic, func(ctx context.Context, msg *TargetMessage) error {
		ch <- msg
		log.Info(fmt.Sprintf("%s received message", mc.id), zap.Any("msg", msg))
		return nil
	})
	return ch
}

func waitForTargetsReady(mc *messageCenter) {
	// wait for all targets to be ready
	time.Sleep(time.Second)
	for {
		allReady := true
		for _, target := range mc.remoteTargets.m {
			if !target.isReadyToSend() {
				log.Info("target is not ready, retry it later", zap.String("target", target.targetId.String()))
				allReady = false
				break
			}
		}
		if allReady {
			log.Info("All targets are ready")
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func sendAndReceiveMessage(t *testing.T, sender *messageCenter, receiver *messageCenter, topic string, event *commonEvent.DMLEvent) {
	targetMsg := NewSingleTargetMessage(receiver.id, topic, event)
	ch := make(chan *TargetMessage, 1)
	receiver.RegisterHandler(topic, func(ctx context.Context, msg *TargetMessage) error {
		ch <- msg
		return nil
	})

	timeoutCh := time.After(30 * time.Second)
	for {
		err := sender.SendEvent(targetMsg)
		require.NoError(t, err)
		select {
		case receivedMsg := <-ch:
			validateReceivedMessage(t, targetMsg, receivedMsg, sender.id, event)
			return
		case <-timeoutCh:
			t.Fatal("Timeout when sending message")
		default:
			log.Info("waiting for message, retry to send it later")
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func validateReceivedMessage(t *testing.T, targetMsg *TargetMessage, receivedMsg *TargetMessage, senderID node.ID, event *commonEvent.DMLEvent) {
	require.Equal(t, targetMsg.To, receivedMsg.To)
	require.Equal(t, senderID, receivedMsg.From)
	require.Equal(t, targetMsg.Type, receivedMsg.Type)
	receivedEvent := receivedMsg.Message[0].(*commonEvent.DMLEvent)
	receivedEvent.AssembleRows(event.TableInfo)
	require.Equal(t, event.Rows.ToString(event.TableInfo.GetFieldSlice()), receivedEvent.Rows.ToString(event.TableInfo.GetFieldSlice()))
}

func TestMessageCenterBasic(t *testing.T) {
	mc1, mc2, mc3, cleanup := setupMessageCenters(t)
	defer cleanup()

	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	_ = helper.DDL2Job("create table t1(id int primary key, a int, b int, c int)")
	dml1 := helper.DML2Event("test", "t1", "insert into t1 values (1, 1, 1, 1)")
	dml2 := helper.DML2Event("test", "t1", "insert into t1 values (2, 2, 2, 2)")
	dml3 := helper.DML2Event("test", "t1", "insert into t1 values (3, 3, 3, 3)")

	topic1 := "topic1"
	topic2 := "topic2"
	topic3 := "topic3"

	registerHandler(mc1, topic1)
	registerHandler(mc2, topic2)
	registerHandler(mc3, topic3)

	time.Sleep(time.Second)
	waitForTargetsReady(mc1)
	waitForTargetsReady(mc2)
	waitForTargetsReady(mc3)

	// Case 1: Send a message from mc1 to mc1 (local message)
	sendAndReceiveMessage(t, mc1, mc1, topic1, dml1)
	log.Info("Pass test 1: send and receive local message")

	// Case 2: Send a message from mc1 to mc2 (remote message)
	sendAndReceiveMessage(t, mc1, mc2, topic2, dml2)
	log.Info("Pass test 2: send and receive remote message")

	// Case 3: Send a message from mc2 to mc3 (remote message)
	sendAndReceiveMessage(t, mc2, mc3, topic3, dml3)
	log.Info("Pass test 3: send and receive remote message")
}
