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
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/maintainer/operator"
	"github.com/flowbehappy/tigate/maintainer/replica"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/flowbehappy/tigate/utils"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	config2 "github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
)

func TestSchedule(t *testing.T) {
	setNodeManagerAndMessageCenter()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	nodeManager.GetAliveNodes()["node3"] = &node.Info{ID: "node3"}

	controller := NewController("test", 1, nil, nil, nil, nil, 9, time.Minute)
	for i := 0; i < 1000; i++ {
		controller.AddNewTable(commonEvent.Table{
			SchemaID: 1,
			TableID:  int64(i),
		}, 1)
	}
	controller.spanScheduler.Execute()
	require.Equal(t, 9, controller.operatorController.OperatorSize())
	for _, span := range controller.replicationDB.GetTasksBySchemaID(1) {
		if op := controller.operatorController.GetOperator(span.ID); op != nil {
			op.Start()
		}
	}
	require.Equal(t, 991, controller.replicationDB.GetAbsentSize())
	require.Equal(t, 3, controller.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 3, controller.GetTaskSizeByNodeID("node2"))
	require.Equal(t, 3, controller.GetTaskSizeByNodeID("node3"))
}

func TestRemoveAbsentTask(t *testing.T) {
	setNodeManagerAndMessageCenter()
	controller := NewController("test", 1, nil, nil, nil, nil, 9, time.Minute)
	controller.AddNewTable(commonEvent.Table{
		SchemaID: 1,
		TableID:  int64(1),
	}, 1)
	require.Equal(t, 1, controller.replicationDB.GetAbsentSize())
	controller.RemoveAllTasks()
	require.Equal(t, 0, controller.replicationDB.GetAbsentSize())
}

func TestBalance(t *testing.T) {
	nodeManager := setNodeManagerAndMessageCenter()
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}

	s := NewController("test", 1, nil, nil, nil, nil, 1000, 0)
	for i := 0; i < 100; i++ {
		span := &heartbeatpb.TableSpan{TableID: int64(i)}
		dispatcherID := common.NewDispatcherID()
		spanReplica := replica.NewReplicaSet(model.ChangeFeedID{}, dispatcherID, 1, span, 1)
		spanReplica.SetNodeID("node1")
		s.replicationDB.AddReplicatingSpan(spanReplica)
	}
	s.spanScheduler.Execute()
	require.Equal(t, 0, s.operatorController.OperatorSize())
	require.Equal(t, 100, s.replicationDB.GetReplicatingSize())
	require.Equal(t, 100, s.replicationDB.GetTaskSizeByNodeID("node1"))

	// add new node
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	s.spanScheduler.Execute()
	require.Equal(t, 50, s.operatorController.OperatorSize())
	require.Equal(t, 50, s.replicationDB.GetSchedulingSize())
	require.Equal(t, 50, s.replicationDB.GetReplicatingSize())
	for _, span := range s.replicationDB.GetTasksBySchemaID(1) {
		if op := s.operatorController.GetOperator(span.ID); op != nil {
			_, ok := op.(*operator.MoveDispatcherOperator)
			require.True(t, ok)
		}
	}
	//still on the primary node
	require.Equal(t, 100, s.replicationDB.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 0, s.replicationDB.GetTaskSizeByNodeID("node2"))

	// remove the node2
	delete(nodeManager.GetAliveNodes(), "node2")
	s.RemoveNode("node2")
	for _, span := range s.replicationDB.GetTasksBySchemaID(1) {
		if op := s.operatorController.GetOperator(span.ID); op != nil {
			msg := op.Schedule()
			require.NotNil(t, msg)
			require.Equal(t, "node1", msg.To.String())
			require.True(t, msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest).ScheduleAction ==
				heartbeatpb.ScheduleAction_Create)
			op.Check("node1", &heartbeatpb.TableSpanStatus{
				ID:              span.ID.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Working,
			})
			require.True(t, op.IsFinished())
			op.PostFinish()
		}
	}

	require.Equal(t, 0, s.GetSchedulingSize())
	// changed to working status
	require.Equal(t, 100, s.replicationDB.GetReplicatingSize())
	require.Equal(t, 100, s.replicationDB.GetTaskSizeByNodeID("node1"))
}

func TestStoppedWhenMoving(t *testing.T) {
	nodeManager := setNodeManagerAndMessageCenter()
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	s := NewController("test", 1, nil, nil, nil, nil, 1000, 0)
	for i := 0; i < 2; i++ {
		span := &heartbeatpb.TableSpan{TableID: int64(i)}
		dispatcherID := common.NewDispatcherID()
		spanReplica := replica.NewReplicaSet(model.ChangeFeedID{}, dispatcherID, 1, span, 1)
		spanReplica.SetNodeID("node1")
		s.replicationDB.AddReplicatingSpan(spanReplica)
	}
	require.Equal(t, 2, s.replicationDB.GetReplicatingSize())
	require.Equal(t, 2, s.replicationDB.GetTaskSizeByNodeID("node1"))
	// add new node
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	s.spanScheduler.Execute()
	require.Equal(t, 1, s.operatorController.OperatorSize())
	require.Equal(t, 1, s.replicationDB.GetSchedulingSize())
	require.Equal(t, 1, s.replicationDB.GetReplicatingSize())
	require.Equal(t, 2, s.replicationDB.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 0, s.replicationDB.GetTaskSizeByNodeID("node2"))

	s.RemoveNode("node2")
	s.RemoveNode("node1")
	require.Equal(t, 0, s.GetSchedulingSize())
	// changed to absent status
	require.Equal(t, 2, s.replicationDB.GetAbsentSize())
	require.Equal(t, 0, s.replicationDB.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 0, s.replicationDB.GetTaskSizeByNodeID("node2"))
}

func TestFinishBootstrap(t *testing.T) {
	nodeManager := setNodeManagerAndMessageCenter()
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	s := NewController("test", 1, nil, nil, &mockThreadPool{}, nil, 1000, 0)
	span := &heartbeatpb.TableSpan{TableID: int64(1)}
	s.SetInitialTables([]commonEvent.Table{{TableID: 1, SchemaID: 1}})

	dispatcherID2 := common.NewDispatcherID()
	stm2 := replica.NewWorkingReplicaSet(model.ChangeFeedID{}, dispatcherID2, 1, span, &heartbeatpb.TableSpanStatus{
		ID:              dispatcherID2.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    10,
	}, "node1")
	cached := utils.NewBtreeMap[*heartbeatpb.TableSpan, *replica.SpanReplication](heartbeatpb.LessTableSpan)
	cached.ReplaceOrInsert(span, stm2)
	require.False(t, s.bootstrapped)
	s.FinishBootstrap(map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication]{
		1: cached,
	})
	require.True(t, s.bootstrapped)
	require.Equal(t, 1, s.replicationDB.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 1, s.replicationDB.GetReplicatingSize())
	// ddl dispatcher
	absents, _ := s.replicationDB.GetScheduleSate(nil, 100)
	found := false
	for _, absent := range absents {
		if absent.ID == s.ddlDispatcherID {
			found = true
		}
	}
	require.True(t, found)
	require.Equal(t, 0, s.replicationDB.GetSchedulingSize())
	replicating := s.replicationDB.GetReplicating()
	found = false
	for _, r := range replicating {
		if r.ID == dispatcherID2 {
			found = true
		}
	}
	require.True(t, found)
	require.Nil(t, s.initialTables)
	require.Panics(t, func() {
		s.FinishBootstrap(map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication]{})
	})
}

// 4 tasks and 2 servers, then add one server, no re-balance will be triggered
func TestBalanceUnEvenTask(t *testing.T) {
	nodeManager := setNodeManagerAndMessageCenter()
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	s := NewController("test", 1, nil, nil, nil, nil, 1000, 0)

	for i := 0; i < 4; i++ {
		span := &heartbeatpb.TableSpan{TableID: int64(i)}
		dispatcherID := common.NewDispatcherID()
		spanReplica := replica.NewReplicaSet(model.ChangeFeedID{}, dispatcherID, 1, span, 1)
		s.replicationDB.AddAbsentReplicaSet(spanReplica)
	}
	s.spanScheduler.Execute()

	for _, span := range s.replicationDB.GetTasksBySchemaID(1) {
		if op := s.operatorController.GetOperator(span.ID); op != nil {
			msg := op.Schedule()
			require.NotNil(t, msg)
			req := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
			require.True(t, req.ScheduleAction == heartbeatpb.ScheduleAction_Create)
			op.Check(msg.To, &heartbeatpb.TableSpanStatus{
				ID:              span.ID.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Working,
			})
			require.True(t, op.IsFinished())
			op.PostFinish()
		}
	}
	require.Equal(t, 4, s.replicationDB.GetReplicatingSize())
	require.Equal(t, 0, s.replicationDB.GetSchedulingSize())
	require.Equal(t, 4, s.operatorController.OperatorSize())
	s.operatorController.Execute()
	require.Equal(t, 0, s.operatorController.OperatorSize())

	// add new node
	nodeManager.GetAliveNodes()["node3"] = &node.Info{ID: "node3"}
	s.spanScheduler.Execute()
	require.Equal(t, 4, s.replicationDB.GetReplicatingSize())
	require.Equal(t, 0, s.operatorController.OperatorSize())
	//still on the primary node
	require.Equal(t, 2, s.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 2, s.GetTaskSizeByNodeID("node2"))
	require.Equal(t, 0, s.GetTaskSizeByNodeID("node3"))
}

func TestSplitTableWhenBootstrapFinished(t *testing.T) {
	pdAPI := &mockPdAPI{
		regions: make(map[int64][]pdutil.RegionInfo),
	}
	nodeManager := setNodeManagerAndMessageCenter()
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	s := NewController("test", 1,
		pdAPI,
		nil, nil, &config2.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: true,
			RegionThreshold:        0,
			WriteKeyThreshold:      1,
		}, 1000, 0)
	s.taskScheduler = &mockThreadPool{}

	// 1 is already split, and 2 will be split
	s.SetInitialTables([]commonEvent.Table{
		{TableID: 1, SchemaID: 1}, {TableID: 2, SchemaID: 2},
	})

	totalSpan := spanz.TableIDToComparableSpan(1)
	pdAPI.regions[1] = []pdutil.RegionInfo{
		pdutil.NewTestRegionInfo(1, totalSpan.StartKey, appendNew(totalSpan.StartKey, 'a'), uint64(1)),
		pdutil.NewTestRegionInfo(2, appendNew(totalSpan.StartKey, 'a'), appendNew(totalSpan.StartKey, 'b'), uint64(1)),
		pdutil.NewTestRegionInfo(3, appendNew(totalSpan.StartKey, 'b'), appendNew(totalSpan.StartKey, 'c'), uint64(1)),
		pdutil.NewTestRegionInfo(4, appendNew(totalSpan.StartKey, 'c'), totalSpan.EndKey, uint64(1)),
	}
	pdAPI.regions[2] = []pdutil.RegionInfo{
		pdutil.NewTestRegionInfo(2, []byte("a"), []byte("b"), uint64(1)),
		pdutil.NewTestRegionInfo(3, []byte("b"), []byte("c"), uint64(1)),
		pdutil.NewTestRegionInfo(4, []byte("c"), []byte("d"), uint64(1)),
		pdutil.NewTestRegionInfo(5, []byte("e"), []byte("f"), uint64(1)),
	}
	reportedSpans := []*heartbeatpb.TableSpan{
		{TableID: 1, StartKey: appendNew(totalSpan.StartKey, 'a'), EndKey: appendNew(totalSpan.StartKey, 'b')}, // 1 region // 1 region
		{TableID: 1, StartKey: appendNew(totalSpan.StartKey, 'b'), EndKey: appendNew(totalSpan.StartKey, 'c')},
	}
	cached := utils.NewBtreeMap[*heartbeatpb.TableSpan, *replica.SpanReplication](heartbeatpb.LessTableSpan)
	for _, span := range reportedSpans {
		dispatcherID1 := common.NewDispatcherID()
		stm1 := replica.NewWorkingReplicaSet(model.DefaultChangeFeedID("test"),
			dispatcherID1, 1, span,
			&heartbeatpb.TableSpanStatus{
				ID:              dispatcherID1.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Working,
				CheckpointTs:    10,
			}, "node1")
		cached.ReplaceOrInsert(span, stm1)
	}

	ddlDispatcherID := common.NewDispatcherID()
	ddlStm := replica.NewWorkingReplicaSet(model.DefaultChangeFeedID("test"), ddlDispatcherID,
		heartbeatpb.DDLSpanSchemaID, heartbeatpb.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              ddlDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    10,
		}, "node1")
	ddlCache := utils.NewBtreeMap[*heartbeatpb.TableSpan, *replica.SpanReplication](heartbeatpb.LessTableSpan)
	ddlCache.ReplaceOrInsert(heartbeatpb.DDLSpan, ddlStm)

	require.False(t, s.bootstrapped)
	s.FinishBootstrap(map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication]{
		0: ddlCache,
		1: cached,
	})
	require.Equal(t, ddlDispatcherID, s.ddlDispatcherID)
	//total 9 regions,
	// table 1: 2 holes will be inserted to absent
	// table 2: split to 4 spans, will be inserted to absent
	require.Equal(t, 6, s.replicationDB.GetAbsentSize())
	// table 1 has two working span,  plus a working ddl span
	require.Equal(t, 3, s.replicationDB.GetReplicatingSize())
	require.True(t, s.bootstrapped)
}

func TestDynamicSplitTableBasic(t *testing.T) {
	pdAPI := &mockPdAPI{
		regions: make(map[int64][]pdutil.RegionInfo),
	}
	nodeManager := setNodeManagerAndMessageCenter()
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	s := NewController("test", 1,
		pdAPI,
		nil, nil, &config2.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: true,
			RegionThreshold:        0,
			WriteKeyThreshold:      1,
		}, 1000, 0)
	s.taskScheduler = &mockThreadPool{}

	totalSpan := spanz.TableIDToComparableSpan(1)
	for i := 1; i <= 2; i++ {
		span := &heartbeatpb.TableSpan{TableID: int64(i), StartKey: totalSpan.StartKey, EndKey: totalSpan.EndKey}
		dispatcherID := common.NewDispatcherID()
		spanReplica := replica.NewReplicaSet(model.ChangeFeedID{}, dispatcherID, 1, span, 1)
		spanReplica.SetNodeID(node.ID(fmt.Sprintf("node%d", i)))
		s.replicationDB.AddReplicatingSpan(spanReplica)
	}
	pdAPI.regions[1] = []pdutil.RegionInfo{
		pdutil.NewTestRegionInfo(1, totalSpan.StartKey, appendNew(totalSpan.StartKey, 'a'), uint64(1)),
		pdutil.NewTestRegionInfo(2, appendNew(totalSpan.StartKey, 'a'), appendNew(totalSpan.StartKey, 'b'), uint64(1)),
		pdutil.NewTestRegionInfo(3, appendNew(totalSpan.StartKey, 'b'), appendNew(totalSpan.StartKey, 'c'), uint64(1)),
		pdutil.NewTestRegionInfo(4, appendNew(totalSpan.StartKey, 'c'), totalSpan.EndKey, uint64(1)),
	}
	pdAPI.regions[2] = []pdutil.RegionInfo{
		pdutil.NewTestRegionInfo(5, totalSpan.StartKey, appendNew(totalSpan.StartKey, 'a'), uint64(1)),
		pdutil.NewTestRegionInfo(6, appendNew(totalSpan.StartKey, 'a'), appendNew(totalSpan.StartKey, 'b'), uint64(1)),
		pdutil.NewTestRegionInfo(7, appendNew(totalSpan.StartKey, 'b'), totalSpan.EndKey, uint64(1)),
	}
	replicas := s.replicationDB.GetReplicating()
	require.Equal(t, 2, s.replicationDB.GetReplicatingSize())
	s.checkController.Execute()
	require.Equal(t, 2, s.replicationDB.GetSchedulingSize())
	require.Equal(t, 2, s.operatorController.OperatorSize())
	for _, task := range replicas {
		op := s.operatorController.GetOperator(task.ID)
		op.Schedule()
		op.Check("node1", &heartbeatpb.TableSpanStatus{
			ID:              op.ID().ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Stopped,
			CheckpointTs:    10,
		})
		op.PostFinish()
	}

	//total 7 regions,
	// table 1: split to 4 spans, will be inserted to absent
	// table 2: split to 3 spans, will be inserted to absent
	require.Equal(t, 7, s.replicationDB.GetAbsentSize())
}

func appendNew(origin []byte, c byte) []byte {
	nb := bytes.Clone(origin)
	return append(nb, c)
}

type mockPdAPI struct {
	pdutil.PDAPIClient
	regions map[int64][]pdutil.RegionInfo
}

func (m *mockPdAPI) ScanRegions(_ context.Context, span tablepb.Span) ([]pdutil.RegionInfo, error) {
	return m.regions[span.TableID], nil
}

type mockThreadPool struct {
	threadpool.ThreadPool
}

func (m *mockThreadPool) Submit(_ threadpool.Task, _ time.Time) *threadpool.TaskHandle {
	return nil
}
