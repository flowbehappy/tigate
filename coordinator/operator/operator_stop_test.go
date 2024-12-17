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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	mock_changefeed "github.com/pingcap/ticdc/coordinator/changefeed/mock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestStopChangefeedOperator_OnNodeRemove(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	cfID := common.NewChangeFeedIDWithName("test")
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{ChangefeedID: cfID,
		Config:  config.GetDefaultReplicaConfig(),
		SinkURI: "mysql://127.0.0.1:3306"},
		1)
	changefeedDB.AddReplicatingMaintainer(cf, "n1")

	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	op := NewStopChangefeedOperator(cfID, "n1", "n2", backend, true)
	op.OnNodeRemove("n1")
	require.Equal(t, "n2", op.nodeID.String())
	require.False(t, op.finished.Load())
}

func TestStopChangefeedOperator_OnTaskRemoved(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	cfID := common.NewChangeFeedIDWithName("test")
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{ChangefeedID: cfID,
		Config:  config.GetDefaultReplicaConfig(),
		SinkURI: "mysql://127.0.0.1:3306"},
		1)
	changefeedDB.AddReplicatingMaintainer(cf, "n1")
	op := NewStopChangefeedOperator(cfID, "n1", "n2", nil, true)
	op.OnTaskRemoved()
	require.True(t, op.finished.Load())
}

func TestStopChangefeedOperator_PostFinish(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	cfID := common.NewChangeFeedIDWithName("test")
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{ChangefeedID: cfID,
		Config:  config.GetDefaultReplicaConfig(),
		SinkURI: "mysql://127.0.0.1:3306"},
		1)
	changefeedDB.AddReplicatingMaintainer(cf, "n1")

	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	op := NewStopChangefeedOperator(cfID, "n1", "n2", backend, true)
	backend.EXPECT().DeleteChangefeed(gomock.Any(), cfID).Return(errors.New("err"))
	op.PostFinish()

	op2 := NewStopChangefeedOperator(cfID, "n1", "n2", backend, false)
	backend.EXPECT().SetChangefeedProgress(gomock.Any(), cfID, config.ProgressNone).Return(errors.New("err"))
	op2.PostFinish()
}
