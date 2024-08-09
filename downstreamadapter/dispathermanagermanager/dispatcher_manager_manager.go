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
	dispatcherManagers map[model.ChangeFeedID]*dispatchermanager.EventDispatcherManager
}

func NewDispatcherManagerManager() *DispatcherManagerManager {
	m := &DispatcherManagerManager{
		dispatcherManagers: make(map[model.ChangeFeedID]*dispatchermanager.EventDispatcherManager),
	}
	appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).
		RegisterHandler(messaging.DispatcherManagerManagerTopic, m.RecvMaintainerRequest)
	return m
}

func (m *DispatcherManagerManager) RecvMaintainerRequest(ctx context.Context, msg *messaging.TargetMessage) error {
	switch req := msg.Message.(type) {
	case *heartbeatpb.MaintainerBootstrapRequest:
		return m.handleAddDispatcherManager(msg.From, req)
	case *heartbeatpb.MaintainerCloseRequest:
		return m.handleRemoveDispatcherManager(msg.From, req)
	default:
		log.Panic("unknown message type", zap.Any("message", msg.Message))
	}
	return nil
}

func (m *DispatcherManagerManager) handleAddDispatcherManager(from messaging.ServerId, maintainerBootstrapRequest *heartbeatpb.MaintainerBootstrapRequest) error {
	changefeedID := model.DefaultChangeFeedID(maintainerBootstrapRequest.ChangefeedID)

	eventDispatcherManager, ok := m.dispatcherManagers[changefeedID]
	if !ok {
		// TODO: decode config
		cfConfig := &model.ChangefeedConfig{}
		err := json.Unmarshal(maintainerBootstrapRequest.Config, cfConfig)
		if err != nil {
			log.Error("failed to unmarshal changefeed config",
				zap.String("changefeed id", maintainerBootstrapRequest.ChangefeedID),
				zap.Error(err))
			return err
		}
		eventDispatcherManager := dispatchermanager.NewEventDispatcherManager(changefeedID, cfConfig, from)
		m.dispatcherManagers[changefeedID] = eventDispatcherManager
		metrics.EventDispatcherManagerGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID).Inc()

		response := &heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: maintainerBootstrapRequest.ChangefeedID,
			Statuses:     make([]*heartbeatpb.TableSpanStatus, 0),
		}

		err = appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendCommand(messaging.NewTargetMessage(
			from,
			messaging.MaintainerManagerTopic,
			response,
		))
		if err != nil {
			log.Error("failed to send maintainer bootstrap response", zap.Error(err))
			return err
		}
		return nil
	}

	// We also will receive maintainerBootstrapRequest when the maintainer is restarted,
	// so we need to update the maintainer id if it's changed
	// and return all table span info to the new maintainer
	if eventDispatcherManager.GetMaintainerID() != from {
		eventDispatcherManager.SetMaintainerID(from)
	}
	response := &heartbeatpb.MaintainerBootstrapResponse{
		Statuses: make([]*heartbeatpb.TableSpanStatus, 0, eventDispatcherManager.GetDispatcherMap().Len()),
	}
	eventDispatcherManager.GetDispatcherMap().ForEach(func(tableSpan *common.TableSpan, tableEventDispatcher *dispatcher.TableEventDispatcher) {
		response.Statuses = append(response.Statuses, &heartbeatpb.TableSpanStatus{
			Span:            tableEventDispatcher.GetTableSpan().TableSpan,
			ComponentStatus: tableEventDispatcher.GetComponentStatus(),
			CheckpointTs:    0,
		})
	})
	err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendCommand(messaging.NewTargetMessage(
		from,
		messaging.MaintainerManagerTopic,
		response,
	))
	if err != nil {
		log.Error("failed to send maintainer bootstrap response", zap.Error(err))
		return err
	}
	return nil
}

func (m *DispatcherManagerManager) handleRemoveDispatcherManager(from messaging.ServerId, req *heartbeatpb.MaintainerCloseRequest) error {
	changefeedID := model.DefaultChangeFeedID(req.ChangefeedID)
	response := &heartbeatpb.MaintainerCloseResponse{
		ChangefeedID: req.ChangefeedID,
		Success:      true,
	}

	eventDispatcherManager, ok := m.dispatcherManagers[changefeedID]
	if ok {
		closed := eventDispatcherManager.TryClose()
		if closed {
			delete(m.dispatcherManagers, changefeedID)
			metrics.EventDispatcherManagerGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID).Dec()
		}
		response.Success = closed
	}
	err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendCommand(messaging.NewTargetMessage(
		from,
		messaging.MaintainerTopic,
		response,
	))

	if err != nil {
		log.Error("failed to send maintainer bootstrap response", zap.Error(err))
		return err
	}
	return nil
}
