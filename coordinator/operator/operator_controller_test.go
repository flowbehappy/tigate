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

package operator

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	mock_changefeed "github.com/pingcap/ticdc/coordinator/changefeed/mock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

func TestController_StopChangefeed(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB()
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	self := node.NewInfo("localhost:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self
	oc := NewOperatorController(nil, node.NewInfo("localhost:8300", ""), changefeedDB,
		backend, nodeManager, 10)
	cfID := common.NewChangeFeedIDWithName("test")
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{ChangefeedID: cfID,
		Config:  config.GetDefaultReplicaConfig(),
		SinkURI: "mysql://127.0.0.1:3306"},
		1)
	changefeedDB.AddReplicatingMaintainer(cf, "n1")

	oc.StopChangefeed(context.Background(), cfID, false)
	require.Len(t, oc.operators, 1)
	// the old  PostFinish will be called
	backend.EXPECT().SetChangefeedProgress(gomock.Any(), gomock.Any(), config.ProgressNone).Return(nil).Times(1)
	oc.StopChangefeed(context.Background(), cfID, true)
	require.Len(t, oc.operators, 1)
	oc.StopChangefeed(context.Background(), cfID, true)
	require.Len(t, oc.operators, 1)
}

func TestController_AddOperator(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB()
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	self := node.NewInfo("localhost:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self
	oc := NewOperatorController(nil, node.NewInfo("localhost:8300", ""), changefeedDB,
		backend, nodeManager, 10)
	cfID := common.NewChangeFeedIDWithName("test")
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{ChangefeedID: cfID,
		Config:  config.GetDefaultReplicaConfig(),
		SinkURI: "mysql://127.0.0.1:3306"},
		1)
	changefeedDB.AddReplicatingMaintainer(cf, "n1")

	require.True(t, oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf, "n2")))
	require.False(t, oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf, "n2")))
	cf2ID := common.NewChangeFeedIDWithName("test2")
	cf2 := changefeed.NewChangefeed(cf2ID, &config.ChangeFeedInfo{ChangefeedID: cf2ID,
		Config:  config.GetDefaultReplicaConfig(),
		SinkURI: "mysql://127.0.0.1:3306"},
		1)
	require.False(t, oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf2, "n2")))

	require.NotNil(t, oc.GetOperator(cfID))
	require.Nil(t, oc.GetOperator(cf2ID))

	require.True(t, oc.HasOperator(cfID.DisplayName))
	require.False(t, oc.HasOperator(cf2ID.DisplayName))
}
