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
	"github.com/flowbehappy/tigate/pkg/node"
	"net/http"
	"net/http/pprof"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	config2 "github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/pdutil"
	pd "github.com/tikv/pd/client"
	"go.uber.org/atomic"
	"go.uber.org/zap"
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
	maintainers        sync.Map
}

func NewMaintainerManager() *mockMaintainerManager {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	m := &mockMaintainerManager{
		mc:          mc,
		maintainers: sync.Map{},
		msgCh:       make(chan *messaging.TargetMessage, 1024),
	}
	mc.RegisterHandler(messaging.MaintainerManagerTopic, m.recvMessages)
	return m
}

func (m *mockMaintainerManager) Run(ctx context.Context) error {
	tick := time.NewTicker(time.Millisecond * 500)
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
			if absent != "" {
				response.Statuses = append(response.Statuses, &heartbeatpb.MaintainerStatus{
					ChangefeedID: absent,
					State:        heartbeatpb.ComponentState_Absent,
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

	response := &heartbeatpb.CoordinatorBootstrapResponse{}
	err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendCommand(messaging.NewSingleTargetMessage(
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
) string {
	if m.coordinatorID != msg.From {
		log.Warn("ignore invalid coordinator id",
			zap.Any("coordinator", msg.From),
			zap.Any("request", msg))
		return ""
	}
	if msg.Type == messaging.TypeAddMaintainerRequest {
		req := msg.Message[0].(*heartbeatpb.AddMaintainerRequest)
		cfID := model.DefaultChangeFeedID(req.GetId())
		cf, ok := m.maintainers.Load(cfID)
		if !ok {
			cfConfig := &model.ChangeFeedInfo{}
			err := json.Unmarshal(req.Config, cfConfig)
			if err != nil {
				log.Panic("decode changefeed fail", zap.Error(err))
			}
			cf = &Maintainer{config: cfConfig}
			m.maintainers.Store(cfID, cf)
		}
	} else {
		req := msg.Message[0].(*heartbeatpb.RemoveMaintainerRequest)
		cfID := model.DefaultChangeFeedID(req.GetId())
		cf, ok := m.maintainers.Load(cfID)
		if !ok {
			log.Warn("ignore remove maintainer request, "+
				"since the maintainer not found",
				zap.String("changefeed", cfID.String()),
				zap.Any("request", req))
			return req.GetId()
		}
		cf.(*Maintainer).removing.Store(true)
		cf.(*Maintainer).cascadeRemoving.Store(req.Cascade)
	}
	return ""
}
func (m *mockMaintainerManager) sendHeartbeat() {
	if m.coordinatorVersion > 0 {
		response := &heartbeatpb.MaintainerHeartbeat{}
		m.maintainers.Range(func(key, value interface{}) bool {
			cfMaintainer := value.(*Maintainer)
			if cfMaintainer.statusChanged.Load() || time.Since(cfMaintainer.lastReportTime) > time.Second*2 {
				response.Statuses = append(response.Statuses, cfMaintainer.GetMaintainerStatus())
				cfMaintainer.statusChanged.Store(false)
				cfMaintainer.lastReportTime = time.Now()
			}
			return true
		})
		if len(response.Statuses) != 0 {
			m.sendMessages(response)
		}
	}
}

type Maintainer struct {
	statusChanged   atomic.Bool
	lastReportTime  time.Time
	removing        atomic.Bool
	cascadeRemoving atomic.Bool

	config *model.ChangeFeedInfo
}

func (m *Maintainer) GetMaintainerStatus() *heartbeatpb.MaintainerStatus {
	return &heartbeatpb.MaintainerStatus{
		ChangefeedID: m.config.ID,
		State:        heartbeatpb.ComponentState_Working,
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
	info := node.NewInfo("", "")
	appcontext.SetService(appcontext.MessageCenter, messaging.NewMessageCenter(ctx,
		info.ID, 100, config.NewDefaultMessageCenterConfig()))
	m := NewMaintainerManager()
	go m.Run(ctx)

	serviceID := "default"
	cr := New(info, &mockPdClient{}, pdutil.NewClock4Test(), serviceID, 100)
	var metadata orchestrator.ReactorState

	cfs := map[model.ChangeFeedID]*orchestrator.ChangefeedReactorState{}

	if !flag.Parsed() {
		flag.Parse()
	}

	argList := flag.Args()
	if len(argList) > 1 {
		t.Fatal("unexpected args", argList)
	}
	cfSize := 100
	if len(argList) == 1 {
		cfSize, _ = strconv.Atoi(argList[0])
	}

	for i := 0; i < cfSize; i++ {
		cfID := model.DefaultChangeFeedID(fmt.Sprintf("%d", i))
		cfs[cfID] = &orchestrator.ChangefeedReactorState{
			ID: cfID,
			Info: &model.ChangeFeedInfo{
				ID:        cfID.ID,
				Namespace: cfID.Namespace,
				Config:    config2.GetDefaultReplicaConfig(),
				State:     model.StateNormal,
			},
			Status: &model.ChangeFeedStatus{CheckpointTs: 10, MinTableBarrierTs: 10},
		}
	}
	metadata = &orchestrator.GlobalReactorState{
		Captures: map[model.CaptureID]*model.CaptureInfo{
			model.CaptureID(info.ID): {
				ID:            model.CaptureID(info.ID),
				AdvertiseAddr: "127.0.0.1:8300",
			},
		},
		Changefeeds: cfs,
	}

	tick := time.NewTicker(time.Millisecond * 50)
	var err error
	for {
		select {
		case <-tick.C:
			metadata, err = cr.Tick(ctx, metadata)
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			return
		}
	}
}
