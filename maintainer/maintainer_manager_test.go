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
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/messaging/proto"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/pingcap/tiflow/cdc/model"
	config2 "github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// scale out/in close, add/remove tables
func TestMaintainerSchedulesNodeChanges(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	selfNode := node.NewInfo("127.0.0.1:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	nodeManager.GetAliveNodes()[selfNode.ID] = selfNode
	store := &mockSchemaStore{
		// 3 tables and a ddl_event_trigger as a table
		tables: []commonEvent.Table{
			{SchemaID: 1, TableID: 1}, {SchemaID: 1, TableID: 2}, {SchemaID: 1, TableID: 3}},
	}
	appcontext.SetService(appcontext.SchemaStore, store)
	mc := messaging.NewMessageCenter(ctx, selfNode.ID, 0, config.NewDefaultMessageCenterConfig())
	appcontext.SetService(appcontext.MessageCenter, mc)
	startDispatcherNode(ctx, selfNode, mc, nodeManager)
	nodeManager.RegisterNodeChangeHandler(appcontext.MessageCenter, mc.OnNodeChanges)
	//discard maintainer manager messages
	mc.RegisterHandler(messaging.CoordinatorTopic, func(ctx context.Context, msg *messaging.TargetMessage) error {
		return nil
	})
	schedulerConf := &config.SchedulerConfig{
		AddTableBatchSize:    1000,
		CheckBalanceInterval: 0,
	}
	manager := NewMaintainerManager(selfNode, schedulerConf, nil, nil)
	msg := messaging.NewSingleTargetMessage(selfNode.ID,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.CoordinatorBootstrapRequest{Version: 1})
	msg.From = msg.To
	manager.onCoordinatorBootstrapRequest(msg)
	go func() {
		_ = manager.Run(ctx)
	}()
	dispManager := MockDispatcherManager(mc, selfNode.ID)
	go func() {
		_ = dispManager.Run(ctx)
	}()
	cfConfig := &model.ChangeFeedInfo{
		ID:     "test",
		Config: config2.GetDefaultReplicaConfig(),
	}
	data, err := json.Marshal(cfConfig)
	require.NoError(t, err)
	_ = mc.SendCommand(messaging.NewSingleTargetMessage(selfNode.ID,
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
		maintainer.controller.replicationDB.GetReplicatingSize())
	require.Equal(t, 4,
		maintainer.controller.GetTaskSizeByNodeID(selfNode.ID))

	// add 2 new node
	node2 := node.NewInfo("127.0.0.1:8400", "")
	mc2 := messaging.NewMessageCenter(ctx, node2.ID, 0, config.NewDefaultMessageCenterConfig())

	node3 := node.NewInfo("127.0.0.1:8500", "")
	mc3 := messaging.NewMessageCenter(ctx, node3.ID, 0, config.NewDefaultMessageCenterConfig())

	node4 := node.NewInfo("127.0.0.1:8600", "")
	mc4 := messaging.NewMessageCenter(ctx, node4.ID, 0, config.NewDefaultMessageCenterConfig())

	startDispatcherNode(ctx, node2, mc2, nodeManager)
	dn3 := startDispatcherNode(ctx, node3, mc3, nodeManager)
	dn4 := startDispatcherNode(ctx, node4, mc4, nodeManager)

	// notify node changes
	_, _ = nodeManager.Tick(ctx, &orchestrator.GlobalReactorState{
		Captures: map[model.CaptureID]*model.CaptureInfo{
			model.CaptureID(selfNode.ID): {ID: model.CaptureID(selfNode.ID), AdvertiseAddr: selfNode.AdvertiseAddr},
			model.CaptureID(node2.ID):    {ID: model.CaptureID(node2.ID), AdvertiseAddr: node2.AdvertiseAddr},
			model.CaptureID(node3.ID):    {ID: model.CaptureID(node3.ID), AdvertiseAddr: node3.AdvertiseAddr},
			model.CaptureID(node4.ID):    {ID: model.CaptureID(node4.ID), AdvertiseAddr: node4.AdvertiseAddr},
		}})

	time.Sleep(5 * time.Second)
	require.Equal(t, 4,
		maintainer.controller.replicationDB.GetReplicatingSize())
	require.Equal(t, 1,
		maintainer.controller.GetTaskSizeByNodeID(selfNode.ID))
	require.Equal(t, 1,
		maintainer.controller.GetTaskSizeByNodeID(node2.ID))
	require.Equal(t, 1,
		maintainer.controller.GetTaskSizeByNodeID(node3.ID))
	require.Equal(t, 1,
		maintainer.controller.GetTaskSizeByNodeID(node4.ID))

	// remove 2 nodes
	dn3.stop()
	dn4.stop()
	_, _ = nodeManager.Tick(ctx, &orchestrator.GlobalReactorState{
		Captures: map[model.CaptureID]*model.CaptureInfo{
			model.CaptureID(selfNode.ID): {ID: model.CaptureID(selfNode.ID), AdvertiseAddr: selfNode.AdvertiseAddr},
			model.CaptureID(node2.ID):    {ID: model.CaptureID(node2.ID), AdvertiseAddr: node2.AdvertiseAddr},
		}})
	time.Sleep(5 * time.Second)
	require.Equal(t, 4,
		maintainer.controller.replicationDB.GetReplicatingSize())
	require.Equal(t, 2,
		maintainer.controller.GetTaskSizeByNodeID(selfNode.ID))
	require.Equal(t, 2,
		maintainer.controller.GetTaskSizeByNodeID(node2.ID))

	// remove 2 tables
	maintainer.controller.RemoveTasksByTableIDs(2, 3)
	time.Sleep(5 * time.Second)
	require.Equal(t, 2,
		maintainer.controller.replicationDB.GetReplicatingSize())
	require.Equal(t, 1,
		maintainer.controller.GetTaskSizeByNodeID(selfNode.ID))
	require.Equal(t, 1,
		maintainer.controller.GetTaskSizeByNodeID(node2.ID))

	// add 2 tables
	maintainer.controller.AddNewTable(commonEvent.Table{
		SchemaID: 1,
		TableID:  4,
	}, 3)
	maintainer.controller.AddNewTable(commonEvent.Table{
		SchemaID: 1,
		TableID:  5,
	}, 3)
	time.Sleep(5 * time.Second)
	require.Equal(t, 4,
		maintainer.controller.replicationDB.GetReplicatingSize())
	require.Equal(t, 2,
		maintainer.controller.GetTaskSizeByNodeID(selfNode.ID))
	require.Equal(t, 2,
		maintainer.controller.GetTaskSizeByNodeID(node2.ID))

	//close maintainer
	err = mc.SendCommand(messaging.NewSingleTargetMessage(selfNode.ID, messaging.MaintainerManagerTopic,
		&heartbeatpb.RemoveMaintainerRequest{Id: cfID.ID, Cascade: true}))
	require.NoError(t, err)
	time.Sleep(2 * time.Second)
	require.Equal(t, heartbeatpb.ComponentState_Stopped, maintainer.state)
	_, ok := manager.maintainers.Load(cfID)
	require.False(t, ok)
	manager.stream.Close()
	cancel()
}

func TestMaintainerBootstrapWithTablesReported(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	selfNode := node.NewInfo("127.0.0.1:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	nodeManager.GetAliveNodes()[selfNode.ID] = selfNode
	store := &mockSchemaStore{
		// 3 tables and a ddl_event_trigger as a table
		tables: []commonEvent.Table{
			{SchemaID: 1, TableID: 1}, {SchemaID: 1, TableID: 2}, {SchemaID: 1, TableID: 3}},
	}
	appcontext.SetService(appcontext.SchemaStore, store)
	mc := messaging.NewMessageCenter(ctx, selfNode.ID, 0, config.NewDefaultMessageCenterConfig())
	appcontext.SetService(appcontext.MessageCenter, mc)
	startDispatcherNode(ctx, selfNode, mc, nodeManager)
	nodeManager.RegisterNodeChangeHandler(appcontext.MessageCenter, mc.OnNodeChanges)
	//discard maintainer manager messages
	mc.RegisterHandler(messaging.CoordinatorTopic, func(ctx context.Context, msg *messaging.TargetMessage) error {
		return nil
	})
	manager := NewMaintainerManager(selfNode, config.GetGlobalServerConfig().Debug.Scheduler, nil, nil)
	msg := messaging.NewSingleTargetMessage(selfNode.ID,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.CoordinatorBootstrapRequest{Version: 1})
	msg.From = msg.To
	manager.onCoordinatorBootstrapRequest(msg)
	go func() {
		_ = manager.Run(ctx)
	}()
	dispManager := MockDispatcherManager(mc, selfNode.ID)
	// table1 and table 2 will be reported by remote
	var remotedIds []common.DispatcherID
	for i := 1; i < 3; i++ {
		span := spanz.TableIDToComparableSpan(int64(i))
		tableSpan := &heartbeatpb.TableSpan{
			TableID:  int64(i),
			StartKey: span.StartKey,
			EndKey:   span.EndKey,
		}
		dispatcherID := common.NewDispatcherID()
		remotedIds = append(remotedIds, dispatcherID)
		dispManager.bootstrapTables = append(dispManager.bootstrapTables, &heartbeatpb.BootstrapTableSpan{
			ID:       dispatcherID.ToPB(),
			SchemaID: 1,
			Span: &heartbeatpb.TableSpan{TableID: tableSpan.TableID,
				StartKey: tableSpan.StartKey,
				EndKey:   tableSpan.EndKey},
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    10,
		})
	}

	go func() {
		_ = dispManager.Run(ctx)
	}()
	cfID := model.DefaultChangeFeedID("test")
	cfConfig := &model.ChangeFeedInfo{
		ID:     cfID.ID,
		Config: config2.GetDefaultReplicaConfig(),
	}
	data, err := json.Marshal(cfConfig)
	require.NoError(t, err)
	_ = mc.SendCommand(messaging.NewSingleTargetMessage(selfNode.ID,
		messaging.MaintainerManagerTopic, &heartbeatpb.AddMaintainerRequest{
			Id:           cfID.ID,
			Config:       data,
			CheckpointTs: 10,
		}))
	time.Sleep(5 * time.Second)

	value, _ := manager.maintainers.Load(cfID)
	maintainer := value.(*Maintainer)

	require.Equal(t, 4,
		maintainer.controller.replicationDB.GetReplicatingSize())
	require.Equal(t, 4,
		maintainer.controller.GetTaskSizeByNodeID(selfNode.ID))
	require.Len(t, remotedIds, 2)
	foundSize := 0
	hasDDLDispatcher := false
	for _, stm := range maintainer.controller.replicationDB.GetReplicating() {
		if stm.Span.Equal(heartbeatpb.DDLSpan) {
			hasDDLDispatcher = true
		}
		for _, remotedId := range remotedIds {
			if stm.ID == remotedId {
				foundSize++
				tblID := stm.Span.TableID
				require.True(t, int64(1) == tblID || int64(2) == tblID)
			}
		}
	}
	require.Equal(t, 2, foundSize)
	require.True(t, hasDDLDispatcher)
	manager.stream.Close()
	cancel()
}

type mockSchemaStore struct {
	schemastore.SchemaStore
	tables []commonEvent.Table
}

func (m *mockSchemaStore) GetAllPhysicalTables(snapTs common.Ts, filter filter.Filter) ([]commonEvent.Table, error) {
	return m.tables, nil
}

type dispatcherNode struct {
	cancel            context.CancelFunc
	mc                messaging.MessageCenter
	dispatcherManager *mockDispatcherManager
}

func (d *dispatcherNode) stop() {
	d.mc.Close()
	d.cancel()
}

func startDispatcherNode(ctx context.Context,
	node *node.Info, mc messaging.MessageCenter, nodeManager *watcher.NodeManager) *dispatcherNode {
	nodeManager.RegisterNodeChangeHandler(node.ID, mc.OnNodeChanges)
	ctx, cancel := context.WithCancel(ctx)
	dispManager := MockDispatcherManager(mc, node.ID)
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
		cancel:            cancel,
		mc:                mc,
		dispatcherManager: dispManager,
	}
}
