package messaging

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/flowbehappy/tigate/pkg/common/event"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/pkg/node"

	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/messaging/proto"
	"github.com/phayes/freeport"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var epoch = uint64(1)

func newMessageCenterForTest(t *testing.T) (*messageCenter, string, func()) {
	port := freeport.GetPort()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	ctx, cancel := context.WithCancel(context.Background())
	mcConfig := config.NewDefaultMessageCenterConfig()
	id := node.NewID()
	mc := NewMessageCenter(ctx, id, epoch, mcConfig)
	epoch++
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

func TestMessageCenterBasic(t *testing.T) {
	mc1, mc1Addr, mc1Stop := newMessageCenterForTest(t)
	mc2, mc2Addr, mc2Stop := newMessageCenterForTest(t)
	mc3, mc3Addr, mc3Stop := newMessageCenterForTest(t)
	defer mc1Stop()
	defer mc2Stop()
	defer mc3Stop()
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	_ = helper.DDL2Job("create table t1(id int primary key, a int, b int, c int)")
	dml1 := helper.DML2Event("test", "t1", "insert into t1 values (1, 1, 1, 1)")
	require.NotNil(t, dml1)
	dml2 := helper.DML2Event("test", "t1", "insert into t1 values (2, 2, 2, 2)")
	require.NotNil(t, dml2)
	dml3 := helper.DML2Event("test", "t1", "insert into t1 values (3, 3, 3, 3)")
	require.NotNil(t, dml3)

	topic1 := dml1.DispatcherID.String()
	topic2 := dml2.DispatcherID.String()
	topic3 := dml3.DispatcherID.String()

	mc1.addTarget(mc2.id, mc2.epoch, mc2Addr)
	mc1.addTarget(mc3.id, mc3.epoch, mc3Addr)
	ch1 := make(chan *TargetMessage, 1)
	h1 := func(ctx context.Context, msg *TargetMessage) error {
		ch1 <- msg
		log.Info("mc1 received message", zap.Any("msg", msg))
		return nil
	}
	mc1.RegisterHandler(topic1, h1)

	mc2.addTarget(mc1.id, mc1.epoch, mc1Addr)
	mc2.addTarget(mc3.id, mc3.epoch, mc3Addr)
	ch2 := make(chan *TargetMessage, 1)
	h2 := func(ctx context.Context, msg *TargetMessage) error {
		ch2 <- msg
		log.Info("mc2 received message", zap.Any("msg", msg))
		return nil
	}
	mc2.RegisterHandler(topic2, h2)

	mc3.addTarget(mc1.id, mc1.epoch, mc1Addr)
	mc3.addTarget(mc2.id, mc2.epoch, mc2Addr)

	ch3 := make(chan *TargetMessage, 1)
	h3 := func(ctx context.Context, msg *TargetMessage) error {
		ch3 <- msg
		log.Info("mc3 received message", zap.Any("msg", msg))
		return nil
	}
	mc3.RegisterHandler(topic3, h3)

	// wait for all targets to be ready
	time.Sleep(time.Second)
LOOP:
	for {
		allReady := true
		for _, target := range mc1.remoteTargets.m {
			if !target.isReadyToSend() {
				log.Info("target is not ready, retry it later", zap.String("target", target.targetId.String()))
				allReady = false
				break
			}
		}
		if allReady {
			log.Info("All targets are ready")
			break LOOP
		}
		time.Sleep(time.Millisecond * 100)
	}

	//Case1: Send a message from mc1 to mc1, local message.
	targetMsg := NewSingleTargetMessage(mc1.id, topic1, dml1)
	var receivedMsg *TargetMessage
	timeoutCh := time.After(30 * time.Second)
LOOP2:
	for {
		err := mc1.SendEvent(targetMsg)
		require.NoError(t, err)
		select {
		case receivedMsg = <-ch1:
			break LOOP2
		case <-timeoutCh:
			t.Fatal("Timeout when sending message to local target")
		default:
			log.Info("waiting for message, retry to send it later")
			time.Sleep(time.Millisecond * 100)
			continue
		}
	}

	require.Equal(t, targetMsg.To, receivedMsg.To)
	require.Equal(t, mc1.id, receivedMsg.From)
	require.Equal(t, targetMsg.Type, receivedMsg.Type)
	require.Equal(t, targetMsg.Message, receivedMsg.Message)
	log.Info("Pass test 1: send and receive local message", zap.Any("receivedMsg", receivedMsg))

	//Case2: Send a message from mc1 to mc2, remote message.
	targetMsg = NewSingleTargetMessage(mc2.id, topic2, dml2)
	timeoutCh = time.After(30 * time.Second)
LOOP3:
	for {
		err := mc1.SendEvent(targetMsg)
		require.NoError(t, err)
		select {
		case receivedMsg = <-ch2:
			break LOOP3
		case <-timeoutCh:
			t.Fatal("Timeout when sending message to remote target")
		default:
			log.Info("waiting for message, retry to send it later")

			time.Sleep(time.Millisecond * 100)
			continue
		}
	}
	require.Equal(t, targetMsg.To, receivedMsg.To)
	require.Equal(t, mc1.id, receivedMsg.From)
	require.Equal(t, targetMsg.Type, receivedMsg.Type)
	receivedEvent := receivedMsg.Message[0].(*commonEvent.DMLEvent)
	receivedEvent.AssembleRows(dml2.TableInfo)
	require.Equal(t, dml2.Rows.ToString(dml2.TableInfo.GetFieldSlice()), receivedEvent.Rows.ToString(dml2.TableInfo.GetFieldSlice()))
	log.Info("Pass test 2: send and receive remote message", zap.Any("receivedMsg", receivedMsg))

	//Case3: Send a message from mc2 to mc3, remote message.
	targetMsg = NewSingleTargetMessage(mc3.id, topic3, dml3)
	timeoutCh = time.After(30 * time.Second)
LOOP4:
	for {
		err := mc2.SendEvent(targetMsg)
		require.NoError(t, err)
		select {
		case receivedMsg = <-ch3:
			break LOOP4
		case <-timeoutCh:
			t.Fatal("Timeout when sending message to remote target")
		default:
			log.Info("waiting for message, retry to send it later")

			time.Sleep(time.Millisecond * 100)
			continue
		}
	}
	require.Equal(t, targetMsg.To, receivedMsg.To)
	require.Equal(t, mc2.id, receivedMsg.From)
	require.Equal(t, targetMsg.Type, receivedMsg.Type)
	receivedEvent = receivedMsg.Message[0].(*commonEvent.DMLEvent)
	receivedEvent.AssembleRows(dml3.TableInfo)
	require.Equal(t, dml3.Rows.ToString(dml3.TableInfo.GetFieldSlice()), receivedEvent.Rows.ToString(dml3.TableInfo.GetFieldSlice()))
	log.Info("Pass test 3: send and receive remote message", zap.Any("receivedMsg", receivedMsg))
}
