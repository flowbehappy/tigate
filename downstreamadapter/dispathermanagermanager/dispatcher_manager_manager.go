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

package dispatchermanagermanager

import (
	"context"
	"encoding/json"

	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/node"

	"github.com/flowbehappy/tigate/downstreamadapter/dispatcher"
	"github.com/flowbehappy/tigate/downstreamadapter/dispatchermanager"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// DispatcherManagerManager deal with the maintainer bootstrap message, to create or delete the event dispatcher manager
type DispatcherManagerManager struct {
	mc                 messaging.MessageCenter
	dispatcherManagers map[model.ChangeFeedID]*dispatchermanager.EventDispatcherManager
}

func New() *DispatcherManagerManager {
	m := &DispatcherManagerManager{
		mc:                 appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
		dispatcherManagers: make(map[model.ChangeFeedID]*dispatchermanager.EventDispatcherManager),
	}
	m.mc.RegisterHandler(messaging.DispatcherManagerManagerTopic, m.RecvMaintainerRequest)
	return m
}

func (m *DispatcherManagerManager) RecvMaintainerRequest(_ context.Context, msg *messaging.TargetMessage) error {
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

func newMaintainerManagerMessage(to node.ID, msg messaging.IOTypeT) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(
		to,
		messaging.MaintainerManagerTopic,
		msg,
	)
}

func (m *DispatcherManagerManager) handleAddDispatcherManager(from node.ID, maintainerBootstrapRequest *heartbeatpb.MaintainerBootstrapRequest) error {
	cfId := model.DefaultChangeFeedID(maintainerBootstrapRequest.ChangefeedID)
	manager, ok := m.dispatcherManagers[cfId]
	if !ok {
		// TODO: decode config
		cfConfig := &config.ChangefeedConfig{}
		err := json.Unmarshal(maintainerBootstrapRequest.Config, cfConfig)
		if err != nil {
			log.Error("failed to unmarshal changefeed config",
				zap.String("changefeed id", maintainerBootstrapRequest.ChangefeedID),
				zap.Error(err))
			return err
		}

		log.Info("create new dispatcher manager", zap.Any("config", cfConfig))
		manager, err := dispatchermanager.NewEventDispatcherManager(cfId, cfConfig, from)
		var response *heartbeatpb.MaintainerBootstrapResponse
		if err != nil {
			log.Error("failed to create new dispatcher manager", zap.Error(err), zap.Any("ChangefeedID", cfId))
			// TODO: deal with the repsonse in maintainer, and turn to changefeed error state
			response = &heartbeatpb.MaintainerBootstrapResponse{
				ChangefeedID: maintainerBootstrapRequest.ChangefeedID,
				Err: &heartbeatpb.RunningError{
					Message: err.Error(),
				},
			}
		} else {
			m.dispatcherManagers[cfId] = manager
			metrics.EventDispatcherManagerGauge.WithLabelValues(cfId.Namespace, cfId.ID).Inc()

			response = &heartbeatpb.MaintainerBootstrapResponse{
				ChangefeedID: maintainerBootstrapRequest.ChangefeedID,
				Spans:        make([]*heartbeatpb.BootstrapTableSpan, 0),
			}
		}

		message := newMaintainerManagerMessage(from, response)
		err = m.mc.SendCommand(message)
		if err != nil {
			log.Error("failed to send maintainer bootstrap response", zap.Error(err))
			return err
		}
		return nil
	}

	// We also will receive maintainerBootstrapRequest when the maintainer is restarted,
	// so we need to update the maintainer id if it's changed
	// and return all table span info to the new maintainer
	if manager.GetMaintainerID() != from {
		manager.SetMaintainerID(from)
	}
	response := &heartbeatpb.MaintainerBootstrapResponse{
		ChangefeedID: maintainerBootstrapRequest.ChangefeedID,
		Spans:        make([]*heartbeatpb.BootstrapTableSpan, 0, manager.GetDispatcherMap().Len()),
	}
	manager.GetDispatcherMap().ForEach(func(id common.DispatcherID, tableEventDispatcher *dispatcher.Dispatcher) {
		response.Spans = append(response.Spans, &heartbeatpb.BootstrapTableSpan{
			ID:              id.ToPB(),
			SchemaID:        tableEventDispatcher.GetSchemaID(),
			Span:            tableEventDispatcher.GetTableSpan(),
			ComponentStatus: tableEventDispatcher.GetComponentStatus(),
			CheckpointTs:    tableEventDispatcher.GetCheckpointTs(),
		})
	})

	message := newMaintainerManagerMessage(from, response)
	err := m.mc.SendCommand(message)
	if err != nil {
		log.Error("failed to send maintainer bootstrap response", zap.Error(err))
		return err
	}
	return nil
}

func (m *DispatcherManagerManager) handleRemoveDispatcherManager(from node.ID, req *heartbeatpb.MaintainerCloseRequest) error {
	cfId := model.DefaultChangeFeedID(req.ChangefeedID)
	response := &heartbeatpb.MaintainerCloseResponse{
		ChangefeedID: req.ChangefeedID,
		Success:      true,
	}

	manager, ok := m.dispatcherManagers[cfId]
	if ok {
		closed := manager.TryClose()
		if closed {
			delete(m.dispatcherManagers, cfId)
			metrics.EventDispatcherManagerGauge.WithLabelValues(cfId.Namespace, cfId.ID).Dec()
		}
		response.Success = closed
	}

	message := messaging.NewSingleTargetMessage(from, messaging.MaintainerTopic, response)
	err := m.mc.SendCommand(message)
	if err != nil {
		log.Error("failed to send maintainer bootstrap response", zap.Error(err))
		return err
	}
	return nil
}
