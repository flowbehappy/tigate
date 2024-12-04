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
	"flag"
	"net/http"
	"net/http/pprof"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/pingcap/ticdc/utils/threadpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mockDispatcherManager struct {
	mc           messaging.MessageCenter
	self         node.ID
	dispatchers  []*heartbeatpb.TableSpanStatus
	msgCh        chan *messaging.TargetMessage
	maintainerID node.ID
	checkpointTs uint64
	changefeedID *heartbeatpb.ChangefeedID

	bootstrapTables []*heartbeatpb.BootstrapTableSpan
	dispatchersMap  map[heartbeatpb.DispatcherID]*heartbeatpb.TableSpanStatus
}

func MockDispatcherManager(mc messaging.MessageCenter, self node.ID) *mockDispatcherManager {
	m := &mockDispatcherManager{
		mc:             mc,
		dispatchers:    make([]*heartbeatpb.TableSpanStatus, 0, 2000001),
		msgCh:          make(chan *messaging.TargetMessage, 1024),
		dispatchersMap: make(map[heartbeatpb.DispatcherID]*heartbeatpb.TableSpanStatus, 2000001),
		self:           self,
	}
	mc.RegisterHandler(messaging.DispatcherManagerManagerTopic, m.recvMessages)
	mc.RegisterHandler(messaging.HeartbeatCollectorTopic, m.recvMessages)
	return m
}

func (m *mockDispatcherManager) Run(ctx context.Context) error {
	tick := time.NewTicker(time.Millisecond * 1000)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-m.msgCh:
			m.handleMessage(msg)
		case <-tick.C:
			m.sendHeartbeat()
		}
	}
}

func (m *mockDispatcherManager) handleMessage(msg *messaging.TargetMessage) {
	switch msg.Type {
	case messaging.TypeMaintainerBootstrapRequest:
		m.onBootstrapRequest(msg)
	case messaging.TypeMaintainerPostBootstrapRequest:

	case messaging.TypeScheduleDispatcherRequest:
		m.onDispatchRequest(msg)
	case messaging.TypeMaintainerCloseRequest:
		m.onMaintainerCloseRequest(msg)
	default:
		log.Panic("unknown msg type", zap.Any("msg", msg))
	}
}
func (m *mockDispatcherManager) sendMessages(msg *heartbeatpb.HeartBeatRequest) {
	target := messaging.NewSingleTargetMessage(
		m.maintainerID,
		messaging.MaintainerManagerTopic,
		msg,
	)
	err := m.mc.SendCommand(target)
	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
}
func (m *mockDispatcherManager) recvMessages(ctx context.Context, msg *messaging.TargetMessage) error {
	switch msg.Type {
	// receive message from maintainer
	case messaging.TypeScheduleDispatcherRequest,
		messaging.TypeMaintainerBootstrapRequest,
		messaging.TypeMaintainerPostBootstrapRequest,
		messaging.TypeMaintainerCloseRequest:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m.msgCh <- msg:
		}
		return nil
	default:
		log.Panic("unknown message type", zap.Any("message", msg.Message), zap.Any("type", msg.Type))
	}
	return nil
}
func (m *mockDispatcherManager) onBootstrapRequest(msg *messaging.TargetMessage) {
	req := msg.Message[0].(*heartbeatpb.MaintainerBootstrapRequest)
	m.maintainerID = msg.From
	response := &heartbeatpb.MaintainerBootstrapResponse{
		ChangefeedID: req.ChangefeedID,
		Spans:        m.bootstrapTables,
		CheckpointTs: req.StartTs,
	}
	m.changefeedID = req.ChangefeedID
	m.checkpointTs = req.StartTs
	err := m.mc.SendCommand(messaging.NewSingleTargetMessage(
		m.maintainerID,
		messaging.MaintainerManagerTopic,
		response,
	))
	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
	log.Info("New maintainer online",
		zap.String("server", m.maintainerID.String()))
}

func (m *mockDispatcherManager) onPostBootstrapRequest(msg *messaging.TargetMessage) {
	req := msg.Message[0].(*heartbeatpb.MaintainerPostBootstrapRequest)
	m.maintainerID = msg.From
	response := &heartbeatpb.MaintainerPostBootstrapResponse{
		ChangefeedID:                  req.ChangefeedID,
		TableTriggerEventDispatcherId: req.TableTriggerEventDispatcherId,
		Err:                           nil,
	}
	err := m.mc.SendCommand(messaging.NewSingleTargetMessage(
		m.maintainerID,
		messaging.MaintainerManagerTopic,
		response,
	))
	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
	log.Info("Post bootstrap finished",
		zap.String("server", m.maintainerID.String()))
}

func (m *mockDispatcherManager) onDispatchRequest(
	msg *messaging.TargetMessage,
) {
	request := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
	if m.maintainerID != msg.From {
		log.Warn("ignore invalid maintainer id",
			zap.Any("request", request),
			zap.Any("maintainer", msg.From))
		return
	}
	if request.ScheduleAction == heartbeatpb.ScheduleAction_Create {
		if m.dispatchersMap[*request.Config.DispatcherID] != nil {
			log.Warn("dispatcher already exists",
				zap.String("from", msg.From.String()),
				zap.String("self", m.self.String()),
				zap.String("dispatcher", common.NewDispatcherIDFromPB(request.Config.DispatcherID).String()))
			return
		}
		status := &heartbeatpb.TableSpanStatus{
			ID:              request.Config.DispatcherID,
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    request.Config.StartTs,
		}
		m.dispatchers = append(m.dispatchers, status)
		m.dispatchersMap[*request.Config.DispatcherID] = status
	} else {
		dispatchers := make([]*heartbeatpb.TableSpanStatus, 0, len(m.dispatchers))
		delete(m.dispatchersMap, *request.Config.DispatcherID)
		for _, status := range m.dispatchers {
			if status.ID.High != request.Config.DispatcherID.High || status.ID.Low != request.Config.DispatcherID.Low {
				dispatchers = append(dispatchers, status)
			} else {
				status.ComponentStatus = heartbeatpb.ComponentState_Stopped
				response := &heartbeatpb.HeartBeatRequest{
					ChangefeedID: m.changefeedID,
					Watermark: &heartbeatpb.Watermark{
						CheckpointTs: m.checkpointTs,
						ResolvedTs:   m.checkpointTs,
					},
					Statuses: []*heartbeatpb.TableSpanStatus{status},
				}
				m.sendMessages(response)
			}
		}
		m.dispatchers = dispatchers
	}
}

func (m *mockDispatcherManager) onMaintainerCloseRequest(msg *messaging.TargetMessage) {
	_ = m.mc.SendCommand(messaging.NewSingleTargetMessage(msg.From,
		messaging.MaintainerTopic, &heartbeatpb.MaintainerCloseResponse{
			ChangefeedID: msg.Message[0].(*heartbeatpb.MaintainerCloseRequest).ChangefeedID,
			Success:      true,
		}))
}

func (m *mockDispatcherManager) sendHeartbeat() {
	if m.maintainerID.String() != "" {
		response := &heartbeatpb.HeartBeatRequest{
			ChangefeedID: m.changefeedID,
			Watermark: &heartbeatpb.Watermark{
				CheckpointTs: m.checkpointTs,
				ResolvedTs:   m.checkpointTs,
			},
			Statuses: m.dispatchers,
		}
		m.checkpointTs++
		m.sendMessages(response)
	}
}

func TestMaintainerSchedule(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	mux := http.NewServeMux()
	registry := prometheus.NewRegistry()
	metrics.InitMetrics(registry)
	prometheus.DefaultGatherer = registry
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/metrics", promhttp.Handler())
	go func() {
		t.Fatal(http.ListenAndServe(":18300", mux))
	}()

	if !flag.Parsed() {
		flag.Parse()
	}

	argList := flag.Args()
	if len(argList) > 1 {
		t.Fatal("unexpected args", argList)
	}
	tableSize := 100
	sleepTime := 5
	if len(argList) == 1 {
		tableSize, _ = strconv.Atoi(argList[0])
	}
	if len(argList) == 2 {
		tableSize, _ = strconv.Atoi(argList[0])
		sleepTime, _ = strconv.Atoi(argList[1])
	}

	var tables = make([]commonEvent.Table, 0, tableSize)
	for id := 1; id <= tableSize; id++ {
		tables = append(tables, commonEvent.Table{
			SchemaID:        1,
			TableID:         int64(id),
			SchemaTableName: &commonEvent.SchemaTableName{},
		})
	}
	schemaStore := &mockSchemaStore{tables: tables}
	appcontext.SetService(appcontext.SchemaStore, schemaStore)

	n := node.NewInfo("", "")
	appcontext.SetService(appcontext.MessageCenter, messaging.NewMessageCenter(ctx,
		n.ID, 100, config.NewDefaultMessageCenterConfig()))
	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	nodeManager.GetAliveNodes()[n.ID] = n
	stream := dynstream.NewDynamicStream(NewStreamHandler())
	stream.Start()
	cfID := common.NewChangeFeedIDWithName("test")
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	mc.RegisterHandler(messaging.MaintainerManagerTopic,
		func(ctx context.Context, msg *messaging.TargetMessage) error {
			stream.In() <- &Event{
				changefeedID: cfID,
				eventType:    EventMessage,
				message:      msg,
			}
			return nil
		})
	dispatcherManager := MockDispatcherManager(mc, n.ID)
	go dispatcherManager.Run(ctx)

	taskScheduler := threadpool.NewThreadPoolDefault()
	tsoClient := &mockTsoClient{}
	maintainer := NewMaintainer(cfID,
		&config.SchedulerConfig{
			CheckBalanceInterval: config.TomlDuration(time.Minute),
			AddTableBatchSize:    10000,
		},
		&config.ChangeFeedInfo{
			Config: config.GetDefaultReplicaConfig(),
		}, n, stream, taskScheduler, nil, tsoClient, nil, 10)
	_ = stream.AddPath(cfID.Id, maintainer)

	// send bootstrap message
	maintainer.sendMessages(maintainer.bootstrapper.HandleNewNodes(
		[]*node.Info{n},
	))
	// setup period event
	SubmitScheduledEvent(maintainer.taskScheduler, maintainer.stream, &Event{
		changefeedID: maintainer.id,
		eventType:    EventPeriod,
	}, time.Now().Add(time.Millisecond*500))
	time.Sleep(time.Second * time.Duration(sleepTime))

	cancel()
	// stream.Close()
	require.Equal(t, tableSize,
		maintainer.controller.replicationDB.GetReplicatingSize())
	require.Equal(t, tableSize,
		maintainer.controller.GetTaskSizeByNodeID(n.ID))
}
