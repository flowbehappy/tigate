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

package replica

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/heartbeatpb"
	replica_mock "github.com/pingcap/ticdc/maintainer/replica/mock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestBasicFunction(t *testing.T) {
	db := newDBForTest(t)
	absent := NewReplicaSet(db.changefeedID, common.NewDispatcherID(), db.ddlSpan.tsoClient, 1, &heartbeatpb.TableSpan{TableID: 4}, 1)
	db.AddAbsentReplicaSet(absent)
	// replicating and scheduling will be returned
	replicaSpanID := common.NewDispatcherID()
	replicaSpan := NewWorkingReplicaSet(db.changefeedID, replicaSpanID,
		db.ddlSpan.tsoClient, 1,
		&heartbeatpb.TableSpan{TableID: 3}, &heartbeatpb.TableSpanStatus{
			ID:              replicaSpanID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	db.AddReplicatingSpan(replicaSpan)
	require.Equal(t, 3, db.TaskSize())
	require.Len(t, db.GetAllTasks(), 3)
	require.True(t, db.IsTableExists(3))
	require.False(t, db.IsTableExists(5))
	require.True(t, db.IsTableExists(4))
	require.Len(t, db.GetTasksBySchemaID(1), 2)
	require.Len(t, db.GetTasksBySchemaID(2), 0)
	require.Equal(t, 2, db.GetTaskSizeBySchemaID(1))
	require.Equal(t, 0, db.GetTaskSizeBySchemaID(2))
	require.Len(t, db.GetTasksByTableIDs(3), 1)
	require.Len(t, db.GetTasksByTableIDs(3, 4), 2)
	require.Len(t, db.GetTaskByNodeID("node1"), 1)
	require.Len(t, db.GetTaskByNodeID("node2"), 0)
	require.Equal(t, 0, db.GetTaskSizeByNodeID("node2"))
	require.Equal(t, 1, db.GetTaskSizeByNodeID("node1"))

	require.Len(t, db.GetReplicating(), 1)
	require.NotNil(t, db.GetTaskByID(replicaSpan.ID))
	require.NotNil(t, db.GetTaskByID(absent.ID))
	require.Nil(t, db.GetTaskByID(common.NewDispatcherID()))
	require.Equal(t, 0, db.GetSchedulingSize())
	require.Equal(t, 1, db.GetTaskSizePerNode()["node1"])

	db.MarkSpanScheduling(absent)
	require.Equal(t, 1, db.GetSchedulingSize())
	db.BindSpanToNode("", "node2", absent)
	require.Len(t, db.GetTaskByNodeID("node2"), 1)
	db.MarkSpanReplicating(absent)
	require.Len(t, db.GetReplicating(), 2)
	require.Equal(t, "node2", absent.GetNodeID().String())

	db.UpdateSchemaID(3, 2)
	require.Len(t, db.GetTasksBySchemaID(1), 1)
	require.Len(t, db.GetTasksBySchemaID(2), 1)

	require.Len(t, db.TryRemoveByTableIDs(3), 1)
	require.Len(t, db.GetTasksBySchemaID(1), 1)
	require.Len(t, db.GetTasksBySchemaID(2), 0)
	require.Len(t, db.GetReplicating(), 1)
	require.Equal(t, 1, db.GetReplicatingSize())
	require.Equal(t, 2, db.TaskSize())

	db.UpdateSchemaID(4, 5)
	require.Equal(t, 1, db.GetTaskSizeBySchemaID(5))
	require.Len(t, db.TryRemoveBySchemaID(5), 1)

	require.Len(t, db.GetReplicating(), 0)
	require.Equal(t, 1, db.TaskSize())
	require.Len(t, db.absent, 0)
	require.Len(t, db.scheduling, 0)
	require.Len(t, db.replicating, 0)
	require.Len(t, db.tableTasks, 0)
	require.Len(t, db.schemaTasks, 0)
	require.Len(t, db.nodeTasks, 0)
}

func TestReplaceReplicaSet(t *testing.T) {
	db := newDBForTest(t)
	// replicating and scheduling will be returned
	replicaSpanID := common.NewDispatcherID()
	replicaSpan := NewWorkingReplicaSet(db.changefeedID, replicaSpanID,
		db.ddlSpan.tsoClient, 1,
		&heartbeatpb.TableSpan{TableID: 3}, &heartbeatpb.TableSpanStatus{
			ID:              replicaSpanID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	db.AddReplicatingSpan(replicaSpan)

	notExists := &SpanReplication{ID: common.NewDispatcherID()}
	db.ReplaceReplicaSet(notExists, []*heartbeatpb.TableSpan{{}, {}}, 1)
	require.Len(t, db.GetAllTasks(), 2)

	db.ReplaceReplicaSet(replicaSpan, []*heartbeatpb.TableSpan{{TableID: 3}, {TableID: 4}}, 5)
	require.Len(t, db.GetAllTasks(), 3)
	require.Equal(t, 2, db.GetAbsentSize())
	require.Equal(t, 2, db.GetTaskSizeBySchemaID(1))
}

func TestMarkSpanAbsent(t *testing.T) {
	db := newDBForTest(t)
	// replicating and scheduling will be returned
	replicaSpanID := common.NewDispatcherID()
	replicaSpan := NewWorkingReplicaSet(db.changefeedID, replicaSpanID,
		db.ddlSpan.tsoClient, 1,
		&heartbeatpb.TableSpan{TableID: 3}, &heartbeatpb.TableSpanStatus{
			ID:              replicaSpanID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	db.AddReplicatingSpan(replicaSpan)
	db.MarkSpanAbsent(replicaSpan)
	require.Equal(t, 1, db.GetAbsentSize())
	require.Equal(t, "", replicaSpan.GetNodeID().String())
}

func TestForceRemove(t *testing.T) {
	db := newDBForTest(t)
	// replicating and scheduling will be returned
	replicaSpanID := common.NewDispatcherID()
	replicaSpan := NewWorkingReplicaSet(db.changefeedID, replicaSpanID,
		db.ddlSpan.tsoClient, 1,
		&heartbeatpb.TableSpan{TableID: 3}, &heartbeatpb.TableSpanStatus{
			ID:              replicaSpanID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	db.AddReplicatingSpan(replicaSpan)
	db.ForceRemove(common.NewDispatcherID())
	require.Len(t, db.GetAllTasks(), 2)
	db.ForceRemove(replicaSpan.ID)
	require.Len(t, db.GetAllTasks(), 1)
}

func TestGetAbsents(t *testing.T) {
	db := newDBForTest(t)
	for i := 0; i < 10; i++ {
		absent := NewReplicaSet(db.changefeedID, common.NewDispatcherID(), db.ddlSpan.tsoClient, 1, &heartbeatpb.TableSpan{TableID: int64(i + 1)}, 1)
		db.AddAbsentReplicaSet(absent)
	}
	require.Len(t, db.GetAbsent(nil, 5), 5)
	require.Len(t, db.GetAbsent(nil, 15), 10)
}

func TestRemoveAllTables(t *testing.T) {
	db := newDBForTest(t)
	// ddl span will not be removed
	removed := db.TryRemoveAll()
	require.Len(t, removed, 0)
	require.Len(t, db.GetAllTasks(), 1)
	// replicating and scheduling will be returned
	replicaSpanID := common.NewDispatcherID()
	replicaSpan := NewWorkingReplicaSet(db.changefeedID, replicaSpanID,
		db.ddlSpan.tsoClient, 1,
		&heartbeatpb.TableSpan{TableID: 3}, &heartbeatpb.TableSpanStatus{
			ID:              replicaSpanID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	db.AddReplicatingSpan(replicaSpan)

	absent := NewReplicaSet(db.changefeedID, common.NewDispatcherID(), db.ddlSpan.tsoClient, 1, &heartbeatpb.TableSpan{TableID: 4}, 1)
	db.AddAbsentReplicaSet(absent)

	scheduling := NewReplicaSet(db.changefeedID, common.NewDispatcherID(), db.ddlSpan.tsoClient, 1, &heartbeatpb.TableSpan{TableID: 4}, 1)
	db.AddAbsentReplicaSet(scheduling)
	db.MarkSpanScheduling(scheduling)

	removed = db.TryRemoveAll()
	require.Len(t, removed, 2)
	require.Len(t, db.GetAllTasks(), 1)
}

func newDBForTest(t *testing.T) *ReplicationDB {
	cfID := common.NewChangeFeedIDWithName("test")
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	ctrl := gomock.NewController(t)
	tsoClient := replica_mock.NewMockTSOClient(ctrl)
	ddlSpan := NewWorkingReplicaSet(cfID, tableTriggerEventDispatcherID,
		tsoClient, heartbeatpb.DDLSpanSchemaID,
		heartbeatpb.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	return NewReplicaSetDB(cfID, ddlSpan)
}
