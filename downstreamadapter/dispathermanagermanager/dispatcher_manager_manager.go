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
		RegisterHandler(messaging.MaintainerBoostrapRequestTopic, m.RecvMaintainerBootstrapRequest)
	return m
}

func (m *DispatcherManagerManager) RecvMaintainerBootstrapRequest(ctx context.Context, msg *messaging.TargetMessage) error {
	maintainerBootstrapRequest := msg.Message.(*heartbeatpb.MaintainerBootstrapRequest)
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
		eventDispatcherManager := dispatchermanager.NewEventDispatcherManager(changefeedID, cfConfig, msg.To, msg.From)
		m.dispatcherManagers[changefeedID] = eventDispatcherManager
		EventDispatcherManagerCount.Inc()

		response := &heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: maintainerBootstrapRequest.ChangefeedID,
			Statuses:     make([]*heartbeatpb.TableSpanStatus, 0),
		}

		err = appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendCommand(messaging.NewTargetMessage(
			msg.From,
			messaging.MaintainerBootstrapResponseTopic,
			response,
		))
		if err != nil {
			log.Error("failed to send maintainer bootstrap response", zap.Error(err))
			return err
		}
		return nil
	}

	// set new maintainer id if it's changed
	if eventDispatcherManager.GetMaintainerID() == msg.From {
		eventDispatcherManager.SetMaintainerID(msg.From)
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
		msg.From,
		messaging.MaintainerBootstrapResponseTopic,
		response,
	))
	if err != nil {
		log.Error("failed to send maintainer bootstrap response", zap.Error(err))
		return err
	}
	return nil
}
