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

package scheduler

import (
	"testing"

	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestExecute(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB()
	for i := 0; i < 9; i++ {
		cfID := common.NewChangeFeedIDWithName("test")
		cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{ChangefeedID: cfID,
			Config:  config.GetDefaultReplicaConfig(),
			State:   model.StateNormal,
			SinkURI: "mysql://127.0.0.1:3306"},
			1)
		changefeedDB.AddAbsentChangefeed(cf)
	}

	self := node.NewInfo("node1", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self
	operatorController := operator.NewOperatorController(nil, self,
		changefeedDB, nil, nodeManager, 10)
	s := NewScheduler(4, operatorController, changefeedDB, nodeManager, 0)
	s.batchSize = 4
	s.Execute()
	require.Equal(t, 4, operatorController.OperatorSize())
	s.Execute()
	require.Equal(t, 4, operatorController.OperatorSize())
	s.batchSize = 6
	s.Execute()
	require.Equal(t, 4, operatorController.OperatorSize())
}
