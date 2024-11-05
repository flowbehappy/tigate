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

package dispatcherorchestrator

import (
	"context"
	"encoding/json"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/dispatchermanager"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

// DispatcherOrchestrator coordinates the creation, deletion, and management of event dispatcher managers
// for different change feeds based on maintainer bootstrap messages.
type DispatcherOrchestrator struct {
	mc                 messaging.MessageCenter
	dispatcherManagers map[common.ChangeFeedID]*dispatchermanager.EventDispatcherManager
}

func New() *DispatcherOrchestrator {
	m := &DispatcherOrchestrator{
		mc:                 appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
		dispatcherManagers: make(map[common.ChangeFeedID]*dispatchermanager.EventDispatcherManager),
	}
	m.mc.RegisterHandler(messaging.DispatcherManagerManagerTopic, m.RecvMaintainerRequest)
	return m
}

func (m *DispatcherOrchestrator) RecvMaintainerRequest(_ context.Context, msg *messaging.TargetMessage) error {
	switch req := msg.Message[0].(type) {
	case *heartbeatpb.MaintainerBootstrapRequest:
		return m.handleAddDispatcherManager(msg.From, req)
	case *heartbeatpb.MaintainerCloseRequest:
		return m.handleRemoveDispatcherManager(msg.From, req)
	default:
		log.Panic("unknown message type", zap.Any("message", msg.Message))
	}
	return nil
}

func (m *DispatcherOrchestrator) handleAddDispatcherManager(from node.ID, req *heartbeatpb.MaintainerBootstrapRequest) error {
	cfId := common.NewChangefeedIDFromPB(req.ChangefeedID)
	manager, exists := m.dispatcherManagers[cfId]
	var err error
	var startTs uint64
	if !exists {
		cfConfig := &config.ChangefeedConfig{}
		if err := json.Unmarshal(req.Config, cfConfig); err != nil {
			log.Panic("failed to unmarshal changefeed config", zap.String("changefeed id", cfId.Name()), zap.Error(err))
			return err
		}
		manager, startTs, err = dispatchermanager.NewEventDispatcherManager(cfId, cfConfig, req.TableTriggerEventDispatcherId, req.StartTs, from)
		// Fast return the error to maintainer.
		if err != nil {
			log.Error("failed to create new dispatcher manager", zap.Error(err), zap.Any("ChangefeedID", cfId.Name()))
			// TODO: deal with the repsonse in maintainer, and turn to changefeed error state
			response := &heartbeatpb.MaintainerBootstrapResponse{
				ChangefeedID: req.ChangefeedID,
				Err: &heartbeatpb.RunningError{
					Message: err.Error(),
				},
			}
			return m.sendResponse(from, messaging.MaintainerManagerTopic, response)
		}
		m.dispatcherManagers[cfId] = manager
		metrics.EventDispatcherManagerGauge.WithLabelValues(cfId.Namespace(), cfId.Name()).Inc()
	}

	if manager.GetMaintainerID() != from {
		manager.SetMaintainerID(from)
	}

	response := createBootstrapResponse(req.ChangefeedID, manager, startTs)
	return m.sendResponse(from, messaging.MaintainerManagerTopic, response)
}

func (m *DispatcherOrchestrator) handleRemoveDispatcherManager(from node.ID, req *heartbeatpb.MaintainerCloseRequest) error {
	cfId := common.NewChangefeedIDFromPB(req.ChangefeedID)
	response := &heartbeatpb.MaintainerCloseResponse{
		ChangefeedID: req.ChangefeedID,
	}

	if manager, ok := m.dispatcherManagers[cfId]; ok {
		if closed := manager.TryClose(req.Removed); closed {
			delete(m.dispatcherManagers, cfId)
			metrics.EventDispatcherManagerGauge.WithLabelValues(cfId.Namespace(), cfId.Name()).Dec()
			response.Success = closed
		}
	}

	return m.sendResponse(from, messaging.MaintainerTopic, response)
}

func createBootstrapResponse(changefeedID *heartbeatpb.ChangefeedID, manager *dispatchermanager.EventDispatcherManager, startTs uint64) *heartbeatpb.MaintainerBootstrapResponse {
	response := &heartbeatpb.MaintainerBootstrapResponse{
		ChangefeedID: changefeedID,
		Spans:        make([]*heartbeatpb.BootstrapTableSpan, 0, manager.GetDispatcherMap().Len()),
	}

	if startTs != 0 {
		response.CheckpointTs = startTs
	}

	manager.GetDispatcherMap().ForEach(func(id common.DispatcherID, d *dispatcher.Dispatcher) {
		response.Spans = append(response.Spans, &heartbeatpb.BootstrapTableSpan{
			ID:              id.ToPB(),
			SchemaID:        d.GetSchemaID(),
			Span:            d.GetTableSpan(),
			ComponentStatus: d.GetComponentStatus(),
			CheckpointTs:    d.GetCheckpointTs(),
			BlockState:      d.GetBlockStatus(),
		})
	})

	return response
}

func (m *DispatcherOrchestrator) sendResponse(to node.ID, topic string, msg messaging.IOTypeT) error {
	message := messaging.NewSingleTargetMessage(to, topic, msg)
	if err := m.mc.SendCommand(message); err != nil {
		log.Error("failed to send response", zap.Error(err))
		return err
	}
	return nil
}
