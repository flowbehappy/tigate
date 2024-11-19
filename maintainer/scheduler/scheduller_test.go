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

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

func TestExecute(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test")
	db := replica.NewReplicaSetDB(cfID, replica.NewReplicaSet(cfID, common.NewDispatcherID(), nil, heartbeatpb.DDLSpanSchemaID, heartbeatpb.DDLSpan, 1))
	for i := 0; i < 9; i++ {
		absent := replica.NewReplicaSet(cfID, common.NewDispatcherID(), nil, 1, &heartbeatpb.TableSpan{TableID: int64(i + 1)}, 1)
		db.AddAbsentReplicaSet(absent)
	}
	self := node.NewInfo("node1", "")
	operatorController := operator.NewOperatorController(cfID, nil, db, 10)
	nm := watcher.NewNodeManager(nil, nil)
	nm.GetAliveNodes()[self.ID] = self
	s := NewScheduler(cfID, 4, operatorController, db, nm, 0)
	s.batchSize = 4
	s.Execute()
	require.Equal(t, 4, operatorController.OperatorSize())
	s.Execute()
	require.Equal(t, 4, operatorController.OperatorSize())
	s.batchSize = 6
	s.Execute()
	require.Equal(t, 4, operatorController.OperatorSize())
}
