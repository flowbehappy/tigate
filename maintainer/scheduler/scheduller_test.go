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
	"math/rand"
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
)

func TestBasicScheduler(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test")
	db := replica.NewReplicaSetDB(cfID, replica.NewReplicaSet(cfID, common.NewDispatcherID(), nil, heartbeatpb.DDLSpanSchemaID, heartbeatpb.DDLSpan, 1))
	totalTasks, firstRound := 10, 10-rand.Intn(5)
	secondRound := totalTasks - firstRound
	dispatchers := []common.DispatcherID{}
	for i := 0; i < totalTasks; i++ {
		id := int64(i + 1)
		totalSpan := spanz.TableIDToComparableSpan(id)
		absent := replica.NewReplicaSet(cfID, common.NewDispatcherID(), nil, 1, &heartbeatpb.TableSpan{
			TableID:  id,
			StartKey: totalSpan.StartKey,
			EndKey:   totalSpan.EndKey,
		}, 1)
		db.AddAbsentReplicaSet(absent)
		dispatchers = append(dispatchers, absent.ID)
	}
	self := node.NewInfo("node1", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self
	operatorController := operator.NewOperatorController(cfID, nil, db, nodeManager, 10)
	s := newBasicScheduler(cfID, 4, operatorController, db, nodeManager)

	// firstRound
	s.batchSize = firstRound
	s.Execute()
	require.Equal(t, firstRound, operatorController.OperatorSize())
	s.Execute()
	require.Equal(t, firstRound, operatorController.OperatorSize())

	// secondRound
	s.batchSize = secondRound
	s.Execute() // skip scheduling since too many operators
	require.Equal(t, firstRound, operatorController.OperatorSize())
	for _, dispatcherID := range dispatchers {
		if operatorController.GetOperator(dispatcherID) == nil {
			continue
		}
		operatorController.UpdateOperatorStatus(dispatcherID, self.ID, &heartbeatpb.TableSpanStatus{
			ID:              dispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
		})
	}
	operatorController.Execute()
	require.Equal(t, 0, operatorController.OperatorSize())
	s.Execute()
	require.Equal(t, secondRound, operatorController.OperatorSize())
}

func TestScheduleToRemovedNode(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test")
	db := replica.NewReplicaSetDB(cfID, replica.NewReplicaSet(cfID, common.NewDispatcherID(), nil, heartbeatpb.DDLSpanSchemaID, heartbeatpb.DDLSpan, 1))
	totalTasks, batch := 10, 3
	dispatchers := []common.DispatcherID{}
	for i := 0; i < totalTasks; i++ {
		id := int64(i + 1)
		totalSpan := spanz.TableIDToComparableSpan(id)
		absent := replica.NewReplicaSet(cfID, common.NewDispatcherID(), nil, 1, &heartbeatpb.TableSpan{
			TableID:  id,
			StartKey: totalSpan.StartKey,
			EndKey:   totalSpan.EndKey,
		}, 1)
		db.AddAbsentReplicaSet(absent)
		dispatchers = append(dispatchers, absent.ID)
	}
	self := node.NewInfo("node1", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self
	operatorController := operator.NewOperatorController(cfID, nil, db, nodeManager, 10)
	s := newBasicScheduler(cfID, 4, operatorController, db, nodeManager)

	s.batchSize = batch
	s.Execute()
	require.Equal(t, batch, operatorController.OperatorSize())
	for _, dispatcherID := range dispatchers {
		if operatorController.GetOperator(dispatcherID) == nil {
			continue
		}
		operatorController.UpdateOperatorStatus(dispatcherID, self.ID, &heartbeatpb.TableSpanStatus{
			ID:              dispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
		})
	}
	operatorController.Execute()
	require.Equal(t, 0, operatorController.OperatorSize())
	require.Equal(t, batch, db.GetReplicatingSize())
	require.Equal(t, totalTasks-batch, db.GetAbsentSize())
	require.Equal(t, 0, db.GetSchedulingSize())

	// this could happen since scheduler tasks are generated asynchronously
	outdatedNodeManager := watcher.NewNodeManager(nil, nil)
	outdatedNodeManager.GetAliveNodes()[self.ID] = self
	s.nodeManager = outdatedNodeManager
	delete(nodeManager.GetAliveNodes(), self.ID)
	s.Execute()
	require.Equal(t, batch, operatorController.OperatorSize())
	require.Equal(t, batch, db.GetSchedulingSize())
	operatorController.Execute()
	require.Equal(t, 0, operatorController.OperatorSize())
	require.Equal(t, totalTasks-batch, db.GetAbsentSize())
}
