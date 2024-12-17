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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	mock_changefeed "github.com/pingcap/ticdc/coordinator/changefeed/mock"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestResumeChangefeed(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
	}
	cfID := common.NewChangeFeedIDWithName("test")
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{ChangefeedID: cfID,
		Config:  config.GetDefaultReplicaConfig(),
		State:   model.StateFailed,
		SinkURI: "mysql://127.0.0.1:3306"},
		1)
	changefeedDB.AddStoppedChangefeed(cf)

	// no changefeed
	require.NotNil(t, controller.ResumeChangefeed(context.Background(), common.NewChangeFeedIDWithName("test2"), 12))

	backend.EXPECT().ResumeChangefeed(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("failed")).Times(1)
	require.NotNil(t, controller.ResumeChangefeed(context.Background(), cfID, 12))
	require.Equal(t, model.StateFailed, changefeedDB.GetByID(cfID).GetInfo().State)

	backend.EXPECT().ResumeChangefeed(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
	require.Nil(t, controller.ResumeChangefeed(context.Background(), cfID, 12))
	require.Equal(t, model.StateNormal, changefeedDB.GetByID(cfID).GetInfo().State)
}

func TestPauseChangefeed(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)

	self := node.NewInfo("localhost:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
		operatorController: operator.NewOperatorController(nil, node.NewInfo("node1", ""),
			changefeedDB, backend, nodeManager, 10),
	}
	cfID := common.NewChangeFeedIDWithName("test")
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{ChangefeedID: cfID,
		Config:  config.GetDefaultReplicaConfig(),
		State:   model.StateNormal,
		SinkURI: "mysql://127.0.0.1:3306"},
		1)
	changefeedDB.AddReplicatingMaintainer(cf, "node1")

	// no changefeed
	require.NotNil(t, controller.PauseChangefeed(context.Background(), common.NewChangeFeedIDWithName("test2")))

	backend.EXPECT().PauseChangefeed(gomock.Any(), gomock.Any()).Return(errors.New("failed")).Times(1)
	require.NotNil(t, controller.PauseChangefeed(context.Background(), cfID))
	require.Equal(t, model.StateNormal, changefeedDB.GetByID(cfID).GetInfo().State)

	backend.EXPECT().PauseChangefeed(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	require.Nil(t, controller.PauseChangefeed(context.Background(), cfID))
	require.Equal(t, model.StateStopped, changefeedDB.GetByID(cfID).GetInfo().State)
	require.Equal(t, 1, changefeedDB.GetStoppedSize())
}

func TestUpdateChangefeed(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
	}
	cfID := common.NewChangeFeedIDWithName("test")
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{ChangefeedID: cfID,
		Config:  config.GetDefaultReplicaConfig(),
		State:   model.StateStopped,
		SinkURI: "mysql://127.0.0.1:3306"},
		1)
	changefeedDB.AddStoppedChangefeed(cf)

	newConfig := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "kafka://127.0.0.1:9092",
	}
	// no changefeed
	require.NotNil(t, controller.UpdateChangefeed(context.Background(), &config.ChangeFeedInfo{
		ChangefeedID: common.NewChangeFeedIDWithName("test1")}))

	backend.EXPECT().UpdateChangefeed(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("failed")).Times(1)
	require.NotNil(t, controller.UpdateChangefeed(context.Background(), newConfig))
	require.Equal(t, false, changefeedDB.GetByID(cfID).IsMQSink())

	backend.EXPECT().UpdateChangefeed(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
	require.Nil(t, controller.UpdateChangefeed(context.Background(), newConfig))
	require.Equal(t, true, changefeedDB.GetByID(cfID).IsMQSink())
	require.Equal(t, 1, changefeedDB.GetStoppedSize())
}

func TestGetChangefeed(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
	}
	cfID := common.NewChangeFeedIDWithName("test")
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{ChangefeedID: cfID,
		Config:  config.GetDefaultReplicaConfig(),
		State:   model.StateStopped,
		SinkURI: "mysql://127.0.0.1:3306"},
		1)
	changefeedDB.AddStoppedChangefeed(cf)

	ret, status, err := controller.GetChangefeed(context.Background(), cfID.DisplayName)
	require.Nil(t, err)
	require.Equal(t, ret.State, model.StateStopped)
	require.Equal(t, uint64(1), status.CheckpointTs)

	_, _, err = controller.GetChangefeed(context.Background(), common.NewChangeFeedDisplayName("test1", "default"))
	require.True(t, cerror.ErrChangeFeedNotExists.Equal(err))
}

func TestRemoveChangefeed(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	self := node.NewInfo("localhost:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
		operatorController: operator.NewOperatorController(nil, node.NewInfo("node1", ""),
			changefeedDB, backend, nodeManager, 10),
	}
	cfID := common.NewChangeFeedIDWithName("test")
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{ChangefeedID: cfID,
		Config:  config.GetDefaultReplicaConfig(),
		State:   model.StateNormal,
		SinkURI: "mysql://127.0.0.1:3306"},
		1)
	changefeedDB.AddReplicatingMaintainer(cf, "node1")
	// no changefeed
	_, err := controller.RemoveChangefeed(context.Background(), common.NewChangeFeedIDWithName("test2"))
	require.NotNil(t, err)

	backend.EXPECT().SetChangefeedProgress(gomock.Any(), cfID, config.ProgressRemoving).Return(errors.New("failed")).Times(1)
	_, err = controller.RemoveChangefeed(context.Background(), cfID)
	require.NotNil(t, err)

	backend.EXPECT().SetChangefeedProgress(gomock.Any(), cfID, config.ProgressRemoving).Return(nil).Times(1)
	cp, err := controller.RemoveChangefeed(context.Background(), cfID)
	require.Nil(t, err)
	require.Equal(t, uint64(1), cp)
}

func TestListChangefeed(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	self := node.NewInfo("localhost:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
		operatorController: operator.NewOperatorController(nil, node.NewInfo("node1", ""),
			changefeedDB, backend, nodeManager, 10),
	}
	cfID := common.NewChangeFeedIDWithName("test")
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{ChangefeedID: cfID,
		Config:  config.GetDefaultReplicaConfig(),
		State:   model.StateNormal,
		SinkURI: "mysql://127.0.0.1:3306"},
		1)
	changefeedDB.AddReplicatingMaintainer(cf, "node1")
	cf2ID := common.NewChangeFeedIDWithName("test")
	cf2 := changefeed.NewChangefeed(cf2ID, &config.ChangeFeedInfo{ChangefeedID: cfID,
		Config:  config.GetDefaultReplicaConfig(),
		State:   model.StateNormal,
		SinkURI: "mysql://127.0.0.1:3306"},
		2)
	changefeedDB.AddAbsentChangefeed(cf2)

	cf3ID := common.NewChangeFeedIDWithName("test")
	cf3 := changefeed.NewChangefeed(cf3ID, &config.ChangeFeedInfo{ChangefeedID: cfID,
		Config:  config.GetDefaultReplicaConfig(),
		State:   model.StateNormal,
		SinkURI: "mysql://127.0.0.1:3306"},
		2)
	changefeedDB.AddStoppedChangefeed(cf3)
	cfs, status, err := controller.ListChangefeeds(context.Background())
	require.Nil(t, err)
	require.Len(t, cfs, 3)
	require.Len(t, status, 3)
}

func TestCreateChangefeed(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	self := node.NewInfo("localhost:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
		operatorController: operator.NewOperatorController(nil, node.NewInfo("node1", ""),
			changefeedDB, backend, nodeManager, 10),
		bootstrapped: atomic.NewBool(false),
	}
	cfID := common.NewChangeFeedIDWithName("test")
	cfConfig := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		State:        model.StateNormal,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "kafka://127.0.0.1:9092",
	}
	require.NotNil(t, controller.CreateChangefeed(context.Background(), cfConfig))
	require.Equal(t, 0, changefeedDB.GetSize())

	controller.bootstrapped.Store(true)
	backend.EXPECT().CreateChangefeed(gomock.Any(), gomock.Any()).Return(errors.New("failed")).Times(1)
	require.NotNil(t, controller.CreateChangefeed(context.Background(), cfConfig))
	require.Equal(t, 0, changefeedDB.GetSize())

	backend.EXPECT().CreateChangefeed(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	require.Nil(t, controller.CreateChangefeed(context.Background(), cfConfig))

	// add it again
	require.Equal(t, 1, changefeedDB.GetAbsentSize())
	require.NotNil(t, controller.CreateChangefeed(context.Background(), cfConfig))

	// changefeed is in stopping
	require.Equal(t, 1, changefeedDB.GetAbsentSize())
	controller.operatorController.AddOperator(operator.NewAddMaintainerOperator(changefeedDB, changefeedDB.GetByID(cfID), "node1"))

	cf2ID := common.NewChangeFeedIDWithName("test")
	cf2Config := &config.ChangeFeedInfo{
		ChangefeedID: cf2ID,
		State:        model.StateNormal,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "kafka://127.0.0.1:9092",
	}
	require.NotNil(t, controller.CreateChangefeed(context.Background(), cf2Config))
}
