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

package coordinator

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/messaging/proto"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type mockPdClient struct {
	pd.Client
}

func (m *mockPdClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	return safePoint, nil
}

type mockMaintainerManager struct {
	mc                 messaging.MessageCenter
	msgCh              chan *messaging.TargetMessage
	coordinatorVersion int64
	coordinatorID      node.ID
	maintainers        []*heartbeatpb.MaintainerStatus
	maintainerMap      map[common.ChangeFeedID]*heartbeatpb.MaintainerStatus
	bootstrapResponse  *heartbeatpb.CoordinatorBootstrapResponse
}

func NewMaintainerManager(mc messaging.MessageCenter) *mockMaintainerManager {
	m := &mockMaintainerManager{
		mc:            mc,
		maintainers:   make([]*heartbeatpb.MaintainerStatus, 0, 1000000),
		maintainerMap: make(map[common.ChangeFeedID]*heartbeatpb.MaintainerStatus, 1000000),
		msgCh:         make(chan *messaging.TargetMessage, 1024),
	}
	mc.RegisterHandler(messaging.MaintainerManagerTopic, m.recvMessages)
	return m
}

func (m *mockMaintainerManager) Run(ctx context.Context) error {
	tick := time.NewTicker(time.Millisecond * 1000)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-m.msgCh:
			m.handleMessage(msg)
		case <-tick.C:
			//1.  try to send heartbeat to coordinator
			m.sendHeartbeat()
		}
	}
}

func (m *mockMaintainerManager) handleMessage(msg *messaging.TargetMessage) {
	switch msg.Type {
	case messaging.TypeCoordinatorBootstrapRequest:
		m.onCoordinatorBootstrapRequest(msg)
	case messaging.TypeAddMaintainerRequest, messaging.TypeRemoveMaintainerRequest:
		absent := m.onDispatchMaintainerRequest(msg)
		if m.coordinatorVersion > 0 {
			response := &heartbeatpb.MaintainerHeartbeat{}
			if absent != nil {
				response.Statuses = append(response.Statuses, &heartbeatpb.MaintainerStatus{
					ChangefeedID: absent,
					State:        heartbeatpb.ComponentState_Stopped,
				})
			}
			if len(response.Statuses) != 0 {
				m.sendMessages(response)
			}
		}
	}
}
func (m *mockMaintainerManager) sendMessages(msg *heartbeatpb.MaintainerHeartbeat) {
	target := messaging.NewSingleTargetMessage(
		m.coordinatorID,
		messaging.CoordinatorTopic,
		msg,
	)
	err := m.mc.SendCommand(target)
	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
}
func (m *mockMaintainerManager) recvMessages(ctx context.Context, msg *messaging.TargetMessage) error {
	switch msg.Type {
	// receive message from coordinator
	case messaging.TypeAddMaintainerRequest, messaging.TypeRemoveMaintainerRequest:
		fallthrough
	case messaging.TypeCoordinatorBootstrapRequest:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m.msgCh <- msg:
		}
		return nil
	default:
		log.Panic("unknown message type", zap.Any("message", msg.Message))
	}
	return nil
}
func (m *mockMaintainerManager) onCoordinatorBootstrapRequest(msg *messaging.TargetMessage) {
	req := msg.Message[0].(*heartbeatpb.CoordinatorBootstrapRequest)
	if m.coordinatorVersion > req.Version {
		log.Warn("ignore invalid coordinator version",
			zap.Int64("version", req.Version))
		return
	}
	m.coordinatorID = msg.From
	m.coordinatorVersion = req.Version

	response := m.bootstrapResponse
	if response == nil {
		response = &heartbeatpb.CoordinatorBootstrapResponse{}
	}
	err := m.mc.SendCommand(messaging.NewSingleTargetMessage(
		m.coordinatorID,
		messaging.CoordinatorTopic,
		response,
	))
	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
	log.Info("New coordinator online",
		zap.Int64("version", m.coordinatorVersion))
}
func (m *mockMaintainerManager) onDispatchMaintainerRequest(
	msg *messaging.TargetMessage,
) *heartbeatpb.ChangefeedID {
	if m.coordinatorID != msg.From {
		log.Warn("ignore invalid coordinator id",
			zap.Any("coordinator", msg.From),
			zap.Any("request", msg))
		return nil
	}
	if msg.Type == messaging.TypeAddMaintainerRequest {
		req := msg.Message[0].(*heartbeatpb.AddMaintainerRequest)
		cfID := common.NewChangefeedIDFromPB(req.GetId())
		cf, ok := m.maintainerMap[cfID]
		if !ok {
			cfConfig := &model.ChangeFeedInfo{}
			err := json.Unmarshal(req.Config, cfConfig)
			if err != nil {
				log.Panic("decode changefeed fail", zap.Error(err))
			}
			cf = &heartbeatpb.MaintainerStatus{
				ChangefeedID: req.GetId(),
				FeedState:    "normal",
				State:        heartbeatpb.ComponentState_Working,
				CheckpointTs: req.CheckpointTs,
			}
			m.maintainerMap[cfID] = cf
			m.maintainers = append(m.maintainers, cf)
		}
	} else {
		req := msg.Message[0].(*heartbeatpb.RemoveMaintainerRequest)
		maintainers := make([]*heartbeatpb.MaintainerStatus, 0, len(m.maintainers))
		delete(m.maintainerMap, common.NewChangefeedIDFromPB(req.GetId()))
		for _, status := range m.maintainerMap {
			maintainers = append(maintainers, status)
		}
		m.maintainers = maintainers
		return req.GetId()
	}
	return nil
}
func (m *mockMaintainerManager) sendHeartbeat() {
	if m.coordinatorVersion > 0 {
		response := &heartbeatpb.MaintainerHeartbeat{}
		response.Statuses = m.maintainers
		if len(response.Statuses) != 0 {
			m.sendMessages(response)
		}
	}
}

func TestCoordinatorScheduling(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	go func() {
		t.Fatal(http.ListenAndServe(":8300", mux))
	}()

	ctx := context.Background()
	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	info := node.NewInfo("127.0.0.1:8300", "")
	nodeManager.GetAliveNodes()[info.ID] = info
	mc := messaging.NewMessageCenter(ctx,
		info.ID, 100, config.NewDefaultMessageCenterConfig())
	appcontext.SetService(appcontext.MessageCenter, mc)
	m := NewMaintainerManager(mc)
	go m.Run(ctx)

	if !flag.Parsed() {
		flag.Parse()
	}

	argList := flag.Args()
	if len(argList) > 1 {
		t.Fatal("unexpected args", argList)
	}
	cfSize := 100
	sleepTime := 5
	if len(argList) == 1 {
		cfSize, _ = strconv.Atoi(argList[0])
	}

	backend := &mockBackend{changefeeds: make(map[common.ChangeFeedID]*changefeed.ChangefeedMetaWrapper)}
	cfs := backend.changefeeds
	for i := 0; i < cfSize; i++ {
		cfID := common.NewChangeFeedIDWithDisplayName(common.ChangeFeedDisplayName{
			Name:      fmt.Sprintf("%d", i),
			Namespace: model.DefaultNamespace,
		})
		cfs[cfID] = &changefeed.ChangefeedMetaWrapper{
			Info: &config.ChangeFeedInfo{
				ChangefeedID: cfID,
				Config:       config.GetDefaultReplicaConfig(),
				State:        model.StateNormal,
			},
			Status: &config.ChangeFeedStatus{CheckpointTs: 10},
		}
	}

	cr := New(info, &mockPdClient{}, pdutil.NewClock4Test(), backend, "default", 100, 10000, time.Minute)
	co := cr.(*coordinator)

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		_ = cr.Run(ctx)
	}()
	time.Sleep(time.Second * time.Duration(sleepTime))

	cancel()
	co.stream.Close()
	require.Equal(t, cfSize,
		co.controller.changefeedDB.GetReplicatingSize())
	require.Equal(t, cfSize,
		len(co.controller.changefeedDB.GetByNodeID(info.ID)))
}

func TestScaleNode(t *testing.T) {
	ctx := context.Background()
	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	info := node.NewInfo("127.0.0.1:8300", "")
	nodeManager.GetAliveNodes()[info.ID] = info
	mc1 := messaging.NewMessageCenter(ctx, info.ID, 0, config.NewDefaultMessageCenterConfig())
	appcontext.SetService(appcontext.MessageCenter, mc1)
	startMaintainerNode(ctx, info, mc1, nodeManager)

	serviceID := "default"
	backend := &mockBackend{changefeeds: make(map[common.ChangeFeedID]*changefeed.ChangefeedMetaWrapper)}
	cfs := backend.changefeeds
	cfSize := 6
	for i := 0; i < cfSize; i++ {
		cfID := common.NewChangeFeedIDWithDisplayName(common.ChangeFeedDisplayName{
			Name:      fmt.Sprintf("%d", i),
			Namespace: model.DefaultNamespace,
		})
		cfs[cfID] = &changefeed.ChangefeedMetaWrapper{
			Info: &config.ChangeFeedInfo{
				ChangefeedID: cfID,
				Config:       config.GetDefaultReplicaConfig(),
				State:        model.StateNormal,
			},
			Status: &config.ChangeFeedStatus{CheckpointTs: 10},
		}
	}

	cr := New(info, &mockPdClient{}, pdutil.NewClock4Test(), backend, serviceID, 100, 10000, time.Millisecond*10)

	// run coordinator
	go func() { cr.Run(ctx) }()

	time.Sleep(time.Second * 5)
	co := cr.(*coordinator)
	require.Equal(t, cfSize, co.controller.changefeedDB.GetReplicatingSize())

	// add two nodes
	info2 := node.NewInfo("127.0.0.1:8400", "")
	mc2 := messaging.NewMessageCenter(ctx, info2.ID, 0, config.NewDefaultMessageCenterConfig())
	startMaintainerNode(ctx, info2, mc2, nodeManager)
	info3 := node.NewInfo("127.0.0.1:8500", "")
	mc3 := messaging.NewMessageCenter(ctx, info3.ID, 0, config.NewDefaultMessageCenterConfig())
	startMaintainerNode(ctx, info3, mc3, nodeManager)
	// notify node changes
	_, _ = nodeManager.Tick(ctx, &orchestrator.GlobalReactorState{
		Captures: map[model.CaptureID]*model.CaptureInfo{
			model.CaptureID(info.ID):  {ID: model.CaptureID(info.ID), AdvertiseAddr: info.AdvertiseAddr},
			model.CaptureID(info2.ID): {ID: model.CaptureID(info2.ID), AdvertiseAddr: info2.AdvertiseAddr},
			model.CaptureID(info3.ID): {ID: model.CaptureID(info3.ID), AdvertiseAddr: info3.AdvertiseAddr},
		}})
	time.Sleep(time.Second * 5)
	require.Equal(t, cfSize, co.controller.changefeedDB.GetReplicatingSize())
	require.Equal(t, 2, len(co.controller.changefeedDB.GetByNodeID(info.ID)))
	require.Equal(t, 2, len(co.controller.changefeedDB.GetByNodeID(info2.ID)))
	require.Equal(t, 2, len(co.controller.changefeedDB.GetByNodeID(info3.ID)))

	// notify node changes
	_, _ = nodeManager.Tick(ctx, &orchestrator.GlobalReactorState{
		Captures: map[model.CaptureID]*model.CaptureInfo{
			model.CaptureID(info.ID):  {ID: model.CaptureID(info.ID), AdvertiseAddr: info.AdvertiseAddr},
			model.CaptureID(info2.ID): {ID: model.CaptureID(info2.ID), AdvertiseAddr: info2.AdvertiseAddr},
		}})
	time.Sleep(time.Second * 5)
	require.Equal(t, cfSize, co.controller.changefeedDB.GetReplicatingSize())
	require.Equal(t, 3, len(co.controller.changefeedDB.GetByNodeID(info.ID)))
	require.Equal(t, 3, len(co.controller.changefeedDB.GetByNodeID(info2.ID)))
}

func TestBootstrapWithUnStoppedChangefeed(t *testing.T) {
	ctx := context.Background()
	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	info := node.NewInfo("127.0.0.1:8300", "")
	nodeManager.GetAliveNodes()[info.ID] = info
	mc1 := messaging.NewMessageCenter(ctx, info.ID, 0, config.NewDefaultMessageCenterConfig())
	appcontext.SetService(appcontext.MessageCenter, mc1)
	mNode := startMaintainerNode(ctx, info, mc1, nodeManager)

	removingCf1 := &changefeed.ChangefeedMetaWrapper{
		Info: &config.ChangeFeedInfo{
			ChangefeedID: common.NewChangeFeedIDWithName("cf1"),
			Config:       config.GetDefaultReplicaConfig(),
			State:        model.StateNormal,
		},
		Status: &config.ChangeFeedStatus{CheckpointTs: 10, Progress: config.ProgressRemoving},
	}
	removingCf2 := &changefeed.ChangefeedMetaWrapper{
		Info: &config.ChangeFeedInfo{
			ChangefeedID: common.NewChangeFeedIDWithName("cf2"),
			Config:       config.GetDefaultReplicaConfig(),
			State:        model.StateNormal,
		},
		Status: &config.ChangeFeedStatus{CheckpointTs: 10, Progress: config.ProgressRemoving},
	}
	stopingCf1 := &changefeed.ChangefeedMetaWrapper{
		Info: &config.ChangeFeedInfo{
			ChangefeedID: common.NewChangeFeedIDWithName("cf1"),
			Config:       config.GetDefaultReplicaConfig(),
			State:        model.StateStopped,
		},
		Status: &config.ChangeFeedStatus{CheckpointTs: 10, Progress: config.ProgressStopping},
	}

	stopingCf2 := &changefeed.ChangefeedMetaWrapper{
		Info: &config.ChangeFeedInfo{
			ChangefeedID: common.NewChangeFeedIDWithName("cf2"),
			Config:       config.GetDefaultReplicaConfig(),
			State:        model.StateStopped,
		},
		Status: &config.ChangeFeedStatus{CheckpointTs: 10, Progress: config.ProgressStopping},
	}

	// two changefeeds are working
	mNode.manager.bootstrapResponse = &heartbeatpb.CoordinatorBootstrapResponse{
		Statuses: []*heartbeatpb.MaintainerStatus{
			{
				ChangefeedID: removingCf1.Info.ChangefeedID.ToPB(),
				State:        heartbeatpb.ComponentState_Working,
			},
			{
				ChangefeedID: stopingCf1.Info.ChangefeedID.ToPB(),
				State:        heartbeatpb.ComponentState_Working,
			},
		},
	}

	serviceID := "default"
	backend := &mockBackend{changefeeds: map[common.ChangeFeedID]*changefeed.ChangefeedMetaWrapper{
		removingCf1.Info.ChangefeedID: removingCf1,
		removingCf2.Info.ChangefeedID: removingCf2,
		stopingCf1.Info.ChangefeedID:  stopingCf1,
		stopingCf2.Info.ChangefeedID:  stopingCf2,
	}}
	cr := New(info, &mockPdClient{}, pdutil.NewClock4Test(), backend, serviceID, 100, 10000, time.Millisecond*10)

	// run coordinator
	go func() { cr.Run(ctx) }()

	time.Sleep(time.Second * 5)
	co := cr.(*coordinator)
	require.Equal(t, 0, co.controller.changefeedDB.GetReplicatingSize())
	require.Equal(t, 2, co.controller.changefeedDB.GetStoppedSize())
	require.Equal(t, 0, co.controller.operatorController.OperatorSize())
}

type maintainNode struct {
	cancel  context.CancelFunc
	mc      messaging.MessageCenter
	manager *mockMaintainerManager
}

func (d *maintainNode) stop() {
	d.mc.Close()
	d.cancel()
}

func startMaintainerNode(ctx context.Context,
	node *node.Info, mc messaging.MessageCenter,
	nodeManager *watcher.NodeManager) *maintainNode {
	nodeManager.RegisterNodeChangeHandler(node.ID, mc.OnNodeChanges)
	ctx, cancel := context.WithCancel(ctx)
	maintainerM := NewMaintainerManager(mc)
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
		_ = maintainerM.Run(ctx)
		grpcServer.Stop()
	}()
	return &maintainNode{
		cancel:  cancel,
		mc:      mc,
		manager: maintainerM,
	}
}

type mockBackend struct {
	changefeed.Backend
	changefeeds map[common.ChangeFeedID]*changefeed.ChangefeedMetaWrapper
}

func (m *mockBackend) GetAllChangefeeds(_ context.Context) (map[common.ChangeFeedID]*changefeed.ChangefeedMetaWrapper, error) {
	return m.changefeeds, nil
}

func (m *mockBackend) UpdateChangefeedCheckpointTs(_ context.Context, _ map[common.ChangeFeedID]uint64) error {
	return nil
}

func (m *mockBackend) SetChangefeedProgress(_ context.Context, _ common.ChangeFeedID, _ config.Progress) error {
	return nil
}

func (m *mockBackend) DeleteChangefeed(_ context.Context, _ common.ChangeFeedID) error {
	return nil
}

func (m *mockBackend) ResumeChangefeed(_ context.Context, _ common.ChangeFeedID, _ uint64) error {
	return nil
}

func (m *mockBackend) PauseChangefeed(_ context.Context, _ common.ChangeFeedID) error {
	return nil
}
