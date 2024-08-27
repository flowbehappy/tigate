// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package maintainer

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/logservice/schemastore"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/messaging/proto"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/google/uuid"
	"github.com/pingcap/tiflow/cdc/model"
	config2 "github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type mockSchemaStore struct {
	schemastore.SchemaStore
	tables []common.TableID
}

func (m *mockSchemaStore) GetAllPhysicalTables(snapTs common.Ts, filter filter.Filter) ([]common.TableID, error) {
	return m.tables, nil
}

// scale out/in close
func TestMaintainerSchedulesNodeChanges(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	selfNode := &common.NodeInfo{ID: uuid.New().String(), AdvertiseAddr: "127.0.0.1:8300"}
	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	nodeManager.GetAliveNodes()[selfNode.ID] = selfNode
	store := &mockSchemaStore{
		tables: []common.TableID{1, 2, 3, 4},
	}
	appcontext.SetService(appcontext.SchemaStore, store)
	mc := messaging.NewMessageCenter(ctx,
		messaging.ServerId(selfNode.ID), 0, config.NewDefaultMessageCenterConfig())
	appcontext.SetService(appcontext.MessageCenter, mc)
	startDispatcherNode(ctx, selfNode, mc, nodeManager)
	nodeManager.RegisterNodeChangeHandler(appcontext.MessageCenter, mc.OnNodeChanges)
	//discard maintainer manager messages
	mc.RegisterHandler(messaging.CoordinatorTopic, func(ctx context.Context, msg *messaging.TargetMessage) error {
		return nil
	})
	manager := NewMaintainerManager(selfNode)
	msg := messaging.NewSingleTargetMessage(messaging.ServerId(selfNode.ID),
		messaging.MaintainerManagerTopic,
		&heartbeatpb.CoordinatorBootstrapRequest{Version: 1})
	msg.From = msg.To
	manager.onCoordinatorBootstrapRequest(msg)
	go func() {
		//manager.coordinatorID = messaging.ServerId(selfNode.ID)
		//manager.coordinatorVersion = 1
		_ = manager.Run(ctx)
	}()
	dispManager := MockDispatcherManager(mc)
	go func() {
		_ = dispManager.Run(ctx)
	}()
	cfConfig := &model.ChangeFeedInfo{
		ID:     "test",
		Config: config2.GetDefaultReplicaConfig(),
	}
	data, err := json.Marshal(cfConfig)
	require.NoError(t, err)
	_ = mc.SendCommand(messaging.NewSingleTargetMessage(messaging.ServerId(selfNode.ID),
		messaging.MaintainerManagerTopic, &heartbeatpb.AddMaintainerRequest{
			Id:           "test",
			Config:       data,
			CheckpointTs: 10,
		}))
	time.Sleep(5 * time.Second)
	cfID := model.DefaultChangeFeedID("test")
	value, _ := manager.maintainers.Load(cfID)
	maintainer := value.(*Maintainer)

	require.Equal(t, 4,
		maintainer.scheduler.GetTaskSizeByState(scheduler.SchedulerStatusWorking))
	require.Equal(t, 4,
		maintainer.scheduler.GetTaskSizeByNodeID(selfNode.ID))

	// add 2 new node
	node2 := &common.NodeInfo{ID: uuid.New().String(), AdvertiseAddr: "127.0.0.1:8400"}
	mc2 := messaging.NewMessageCenter(ctx, messaging.ServerId(node2.ID), 0, config.NewDefaultMessageCenterConfig())
	node3 := &common.NodeInfo{ID: uuid.New().String(), AdvertiseAddr: "127.0.0.1:8500"}
	mc3 := messaging.NewMessageCenter(ctx, messaging.ServerId(node3.ID), 0, config.NewDefaultMessageCenterConfig())
	node4 := &common.NodeInfo{ID: uuid.New().String(), AdvertiseAddr: "127.0.0.1:8600"}
	mc4 := messaging.NewMessageCenter(ctx, messaging.ServerId(node4.ID), 0, config.NewDefaultMessageCenterConfig())
	startDispatcherNode(ctx, node2, mc2, nodeManager)
	dn3 := startDispatcherNode(ctx, node3, mc3, nodeManager)
	dn4 := startDispatcherNode(ctx, node4, mc4, nodeManager)

	// use a small check interval
	maintainer.scheduler.checkBalanceInterval = time.Millisecond * 50
	// notify node changes
	_, _ = nodeManager.Tick(ctx, &orchestrator.GlobalReactorState{
		Captures: map[model.CaptureID]*model.CaptureInfo{
			selfNode.ID: {ID: selfNode.ID, AdvertiseAddr: selfNode.AdvertiseAddr},
			node2.ID:    {ID: node2.ID, AdvertiseAddr: node2.AdvertiseAddr},
			node3.ID:    {ID: node3.ID, AdvertiseAddr: node3.AdvertiseAddr},
			node4.ID:    {ID: node4.ID, AdvertiseAddr: node4.AdvertiseAddr},
		}})

	time.Sleep(5 * time.Second)
	require.Equal(t, 4,
		maintainer.scheduler.GetTaskSizeByState(scheduler.SchedulerStatusWorking))
	require.Equal(t, 1,
		maintainer.scheduler.GetTaskSizeByNodeID(selfNode.ID))
	require.Equal(t, 1,
		maintainer.scheduler.GetTaskSizeByNodeID(node2.ID))
	require.Equal(t, 1,
		maintainer.scheduler.GetTaskSizeByNodeID(node3.ID))
	require.Equal(t, 1,
		maintainer.scheduler.GetTaskSizeByNodeID(node4.ID))

	// remove 2 nodes
	dn3.stop()
	dn4.stop()
	_, _ = nodeManager.Tick(ctx, &orchestrator.GlobalReactorState{
		Captures: map[model.CaptureID]*model.CaptureInfo{
			selfNode.ID: {ID: selfNode.ID, AdvertiseAddr: selfNode.AdvertiseAddr},
			node2.ID:    {ID: node2.ID, AdvertiseAddr: node2.AdvertiseAddr},
		}})
	time.Sleep(5 * time.Second)
	require.Equal(t, 4,
		maintainer.scheduler.GetTaskSizeByState(scheduler.SchedulerStatusWorking))
	require.Equal(t, 2,
		maintainer.scheduler.GetTaskSizeByNodeID(selfNode.ID))
	require.Equal(t, 2,
		maintainer.scheduler.GetTaskSizeByNodeID(node2.ID))

	//close maintainer
	err = mc.SendCommand(messaging.NewSingleTargetMessage(messaging.ServerId(selfNode.ID), messaging.MaintainerManagerTopic,
		&heartbeatpb.RemoveMaintainerRequest{Id: cfID.ID, Cascade: true}))
	require.NoError(t, err)
	time.Sleep(2 * time.Second)
	require.Equal(t, heartbeatpb.ComponentState_Stopped, maintainer.state)
	_, ok := manager.maintainers.Load(cfID)
	require.False(t, ok)
	manager.stream.Close()
	cancel()
}

type dispatcherNode struct {
	cancel context.CancelFunc
	mc     messaging.MessageCenter
}

func (d *dispatcherNode) stop() {
	d.mc.Close()
	d.cancel()
}

func startDispatcherNode(ctx context.Context,
	node *common.NodeInfo, mc messaging.MessageCenter, nodeManager *watcher.NodeManager) *dispatcherNode {
	nodeManager.RegisterNodeChangeHandler(node.ID, mc.OnNodeChanges)
	ctx, cancel := context.WithCancel(ctx)
	dispManager := MockDispatcherManager(mc)
	go func() {
		var opts []grpc.ServerOption
		grpcServer := grpc.NewServer(opts...)
		mcs := messaging.NewMessageCenterServer(mc)
		proto.RegisterMessageCenterServer(grpcServer, mcs)
		lis, err := net.Listen("tcp", node.AdvertiseAddr)
		if err != nil {
			panic(err)
		}
		go func() {
			_ = grpcServer.Serve(lis)
		}()
		_ = dispManager.Run(ctx)
		grpcServer.Stop()
	}()
	return &dispatcherNode{
		cancel: cancel,
		mc:     mc,
	}
}
