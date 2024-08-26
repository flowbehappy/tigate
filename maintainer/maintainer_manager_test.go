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
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/logservice/schemastore"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/google/uuid"
	"github.com/pingcap/tiflow/cdc/model"
	config2 "github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type mockSchemaStore struct {
	schemastore.SchemaStore
	tables []common.TableID
}

func (m *mockSchemaStore) GetAllPhysicalTables(snapTs common.Ts, filter filter.Filter) ([]common.TableID, error) {
	return m.tables, nil
}

func TestAddMaintainer(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	selfNode := &common.NodeInfo{ID: uuid.New().String()}
	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	nodes := nodeManager.GetAliveNodes()
	nodes[selfNode.ID] = selfNode
	store := &mockSchemaStore{
		tables: []common.TableID{1, 2, 3},
	}
	appcontext.SetService(appcontext.SchemaStore, store)
	mc := messaging.NewMessageCenter(ctx,
		messaging.ServerId(selfNode.ID), 100, config.NewDefaultMessageCenterConfig())
	appcontext.SetService(appcontext.MessageCenter, mc)
	//discard maintainer manager messages
	mc.RegisterHandler(messaging.CoordinatorTopic, func(ctx context.Context, msg *messaging.TargetMessage) error {
		return nil
	})
	manager := NewMaintainerManager(selfNode)
	go func() {
		manager.coordinatorID = messaging.ServerId(selfNode.ID)
		manager.coordinatorVersion = 1
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
	time.Sleep(20 * time.Second)
	manager.stream.Close()
	cancel()
}
