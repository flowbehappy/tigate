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
	"testing"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/utils"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	config2 "github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
)

func TestSchedule(t *testing.T) {
	s := NewController("test", 1, nil, nil, nil, 9, time.Minute)
	s.nodeTasks["node1"] = map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]{}
	s.nodeTasks["node2"] = map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]{}
	s.nodeTasks["node3"] = map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]{}

	for i := 0; i < 1000; i++ {
		s.AddNewTable(commonEvent.Table{
			SchemaID: 1,
			TableID:  int64(i),
		}, 1)
	}
	s.Schedule()
	require.Equal(t, len(s.Commiting()), 9)
	require.Equal(t, len(s.Absent()), 991)
	require.Equal(t, len(s.nodeTasks["node1"]), 3)
	require.Equal(t, len(s.nodeTasks["node2"]), 3)
	require.Equal(t, len(s.nodeTasks["node3"]), 3)
	msgs := s.GetSchedulingMessages()
	require.Len(t, msgs, 9)
}

func TestMoveTask(t *testing.T) {
	s := NewController("test", 1, nil, nil, nil, 9, time.Minute)
	s.AddNewTable(commonEvent.Table{
		SchemaID: 1,
		TableID:  int64(1),
	}, 1)
	s.nodeTasks["a"] = map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]{}
	s.nodeTasks["b"] = map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]{}
	var (
		dispatcherID common.DispatcherID
		stm          *scheduler.StateMachine[common.DispatcherID]
	)
	for id, m := range s.Absent() {
		dispatcherID = id
		stm = m
		break
	}

	//absent->committing
	stm.State = scheduler.SchedulerStatusCommiting
	stm.Primary = "a"
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusAbsent, "", true)
	require.Equal(t, 0, len(s.Absent()))
	require.Equal(t, 1, len(s.nodeTasks["a"]))
	require.Equal(t, 1, len(s.Commiting()))

	//committing -> working
	stm.State = scheduler.SchedulerStatusWorking
	stm.Primary = "a"
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusCommiting, "a", true)
	require.Equal(t, 0, len(s.Absent()))
	require.Equal(t, 1, len(s.nodeTasks["a"]))
	require.Equal(t, 0, len(s.Commiting()))
	require.Equal(t, 1, len(s.Working()))

	//working -> removing
	stm.HandleMoveInferior("b")
	stm.Primary = "a"
	stm.Secondary = "b"
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusWorking, "a", true)
	require.Equal(t, 0, len(s.Absent()))
	require.Equal(t, 1, len(s.nodeTasks["a"]))
	require.Equal(t, 1, len(s.Removing()))
	require.Equal(t, 0, len(s.Working()))

	// removing -> committing
	stm.HandleInferiorStatus(heartbeatpb.ComponentState_Stopped,
		&heartbeatpb.TableSpanStatus{
			ID:              dispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Stopped,
			CheckpointTs:    10,
		}, "a")
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusRemoving, "a", true)
	require.Equal(t, 0, len(s.Absent()))
	require.Equal(t, 0, len(s.nodeTasks["a"]))
	require.Equal(t, 1, len(s.nodeTasks["b"]))
	require.Equal(t, 1, len(s.Commiting()))
	require.Equal(t, 0, len(s.Working()))

	//committing -> working
	stm.HandleInferiorStatus(heartbeatpb.ComponentState_Working,
		&heartbeatpb.TableSpanStatus{
			ID:              dispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    10,
		}, "b")
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusCommiting, "a", true)
	require.Equal(t, 0, len(s.Absent()))
	require.Equal(t, 0, len(s.nodeTasks["a"]))
	require.Equal(t, 1, len(s.nodeTasks["b"]))
	require.Equal(t, 0, len(s.Commiting()))
	require.Equal(t, 1, len(s.Working()))

	//working -> removing
	stm.HandleRemoveInferior()
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusWorking, "b", true)

	// removing -> removed
	stm.HandleInferiorStatus(heartbeatpb.ComponentState_Stopped,
		&heartbeatpb.TableSpanStatus{
			ID:              dispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Stopped,
			CheckpointTs:    10,
		}, "b")
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusWorking, "b", true)
	require.Equal(t, 0, len(s.Absent()))
	require.Equal(t, 0, len(s.nodeTasks["a"]))
	require.Equal(t, 0, len(s.nodeTasks["b"]))
	require.Equal(t, 0, len(s.Commiting()))
	require.Equal(t, 0, len(s.Working()))
}

func TestRemoveAbsentTask(t *testing.T) {
	s := NewController("test", 1, nil, nil, nil, 9, time.Minute)
	s.AddNewTable(commonEvent.Table{
		SchemaID: 1,
		TableID:  int64(1),
	}, 1)
	require.Equal(t, 1, len(s.Absent()))
	s.RemoveAllTasks()
	require.Equal(t, 0, len(s.Absent()))
}

func TestBalance(t *testing.T) {
	s := NewController("test", 1, nil, nil, nil, 1000, 0)
	s.nodeTasks["node1"] = map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]{}
	for i := 0; i < 100; i++ {
		span := &heartbeatpb.TableSpan{TableID: int64(i)}
		dispatcherID := common.NewDispatcherID()
		stm := scheduler.NewStateMachine(dispatcherID, nil,
			NewReplicaSet(model.ChangeFeedID{}, dispatcherID, 1, span, 1))
		stm.State = scheduler.SchedulerStatusWorking
		stm.Primary = "node1"
		s.Working()[dispatcherID] = stm
		s.nodeTasks["node1"][dispatcherID] = stm
	}
	s.Schedule()
	require.Equal(t, len(s.Working()), 100)
	require.Equal(t, len(s.nodeTasks["node1"]), 100)
	//add duplicate node
	s.AddNewNode("node1")
	// add new node
	s.AddNewNode("node2")
	s.Schedule()
	msgs := s.GetSchedulingMessages()
	require.Len(t, msgs, 50)
	require.Equal(t, len(s.Removing()), 50)
	require.Equal(t, len(s.Working()), 50)
	//still on the primary node
	require.Equal(t, len(s.nodeTasks["node1"]), 100)
	require.Equal(t, len(s.nodeTasks["node2"]), 0)

	// remove the node2
	s.RemoveNode("node2")
	require.Equal(t, len(s.Removing()), 0)
	// changed to working status
	require.Equal(t, len(s.Working()), 100)
	require.Equal(t, len(s.nodeTasks["node1"]), 100)
	s.RemoveNode("node2")
	require.Equal(t, len(s.Removing()), 0)
	require.Equal(t, len(s.Working()), 100)
}

func TestStoppedWhenMoving(t *testing.T) {
	s := NewController("test", 1, nil, nil, nil, 1000, 0)
	s.AddNewNode("node1")
	s.AddNewNode("node2")
	id := 1
	span := &heartbeatpb.TableSpan{TableID: int64(id)}
	dispatcherID := common.NewDispatcherID()
	stm := scheduler.NewStateMachine(dispatcherID, nil,
		NewReplicaSet(model.ChangeFeedID{}, dispatcherID, 1, span, 1))
	stm.State = scheduler.SchedulerStatusWorking
	stm.Primary = "node1"
	s.Working()[dispatcherID] = stm
	s.nodeTasks["node1"][dispatcherID] = stm

	stm.HandleMoveInferior("node2")
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusWorking, "node2", true)
	require.Equal(t, len(s.Removing()), 1)
	// changed to working status
	require.Equal(t, len(s.Working()), 0)
	require.Equal(t, len(s.nodeTasks["node1"]), 1)
	require.Equal(t, len(s.nodeTasks["node2"]), 0)

	s.HandleStatus("node1", []*heartbeatpb.TableSpanStatus{
		{
			ID:              dispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Stopped,
			CheckpointTs:    10,
		},
	})
	require.Equal(t, len(s.Commiting()), 1)
	require.Equal(t, len(s.Removing()), 0)
	require.Equal(t, len(s.nodeTasks["node1"]), 0)
	require.Equal(t, len(s.nodeTasks["node2"]), 1)
	require.Equal(t, uint64(10), stm.Inferior.(*ReplicaSet).status.CheckpointTs)
}

func TestFinishBootstrap(t *testing.T) {
	s := NewController("test", 1, nil, nil, nil, 1000, 0)
	s.AddNewNode("node1")
	span := &heartbeatpb.TableSpan{TableID: int64(1)}
	s.SetInitialTables([]commonEvent.Table{{TableID: 1, SchemaID: 1}})

	dispatcherID2 := common.NewDispatcherID()
	stm2 := scheduler.NewStateMachine(dispatcherID2, map[node.ID]any{
		"node1": &heartbeatpb.TableSpanStatus{
			ID:              dispatcherID2.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    10,
		},
	}, NewReplicaSet(model.ChangeFeedID{}, dispatcherID2, 1, span, 1))
	cached := utils.NewBtreeMap[*heartbeatpb.TableSpan, *scheduler.StateMachine[common.DispatcherID]](heartbeatpb.LessTableSpan)
	cached.ReplaceOrInsert(span, stm2)
	require.False(t, s.bootstrapped)
	s.FinishBootstrap(map[int64]utils.Map[*heartbeatpb.TableSpan, *scheduler.StateMachine[common.DispatcherID]]{
		1: cached,
	})
	require.True(t, s.bootstrapped)
	require.Equal(t, len(s.nodeTasks["node1"]), 1)
	require.Equal(t, len(s.Working()), 1)
	// ddl dispatcher
	_, ddlDispatcher := s.Absent()[s.ddlDispatcherID]
	require.True(t, ddlDispatcher)
	require.Equal(t, len(s.Commiting()), 0)
	require.Equal(t, len(s.Removing()), 0)
	require.Equal(t, stm2, s.Working()[dispatcherID2])
	require.Nil(t, s.initialTables)
	require.Panics(t, func() {
		s.FinishBootstrap(map[int64]utils.Map[*heartbeatpb.TableSpan, *scheduler.StateMachine[common.DispatcherID]]{})
	})
}

// 4 tasks and 2 servers, then add one server, no re-balance will be triggered
func TestBalanceUnEvenTask(t *testing.T) {
	s := NewController("test", 1, nil, nil, nil, 1000, 0)
	s.AddNewNode("node1")
	s.AddNewNode("node2")
	for i := 0; i < 4; i++ {
		s.AddNewTable(commonEvent.Table{
			SchemaID: 1,
			TableID:  int64(i),
		}, 1)
	}
	s.Schedule()
	require.Equal(t, len(s.Commiting()), 4)
	require.Equal(t, len(s.nodeTasks["node1"]), 2)
	require.Equal(t, len(s.nodeTasks["node2"]), 2)
	f := func(m map[common.DispatcherID]*scheduler.StateMachine[common.DispatcherID]) []*heartbeatpb.TableSpanStatus {
		var nodeIds []*heartbeatpb.TableSpanStatus
		for id, _ := range m {
			nodeIds = append(nodeIds, &heartbeatpb.TableSpanStatus{
				ID:              id.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Working,
				CheckpointTs:    10,
			})
		}
		return nodeIds
	}
	s.HandleStatus("node1", f(s.nodeTasks["node1"]))
	s.HandleStatus("node2", f(s.nodeTasks["node2"]))
	require.Equal(t, len(s.Working()), 4)
	require.Equal(t, len(s.Commiting()), 0)

	// add new node
	s.AddNewNode("node3")
	s.Schedule()
	require.Equal(t, len(s.Removing()), 0)
	require.Equal(t, len(s.Working()), 4)
	//still on the primary node
	require.Equal(t, len(s.nodeTasks["node1"]), 2)
	require.Equal(t, len(s.nodeTasks["node2"]), 2)
	require.Equal(t, len(s.nodeTasks["node3"]), 0)
}

func TestSplitTableWhenBootstrapFinished(t *testing.T) {
	pdAPI := &mockPdAPI{
		regions: make(map[int64][]pdutil.RegionInfo),
	}
	s := NewController("test", 1,
		pdAPI,
		nil, &config2.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: true,
			RegionThreshold:        0,
			WriteKeyThreshold:      1,
		}, 1000, 0)
	s.AddNewNode("node1")
	s.AddNewNode("node2")

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
	cached := utils.NewBtreeMap[*heartbeatpb.TableSpan, *scheduler.StateMachine[common.DispatcherID]](heartbeatpb.LessTableSpan)
	for _, span := range reportedSpans {
		dispatcherID1 := common.NewDispatcherID()
		stm1 := scheduler.NewStateMachine(dispatcherID1, map[node.ID]any{
			"node1": &heartbeatpb.TableSpanStatus{
				ID:              dispatcherID1.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Working,
				CheckpointTs:    10,
			},
		}, NewReplicaSet(model.ChangeFeedID{}, dispatcherID1, 1, span, 1))
		cached.ReplaceOrInsert(span, stm1)
	}

	ddlDispatcherID := common.NewDispatcherID()
	ddlStm := scheduler.NewStateMachine(ddlDispatcherID, map[node.ID]any{
		"node1": &heartbeatpb.TableSpanStatus{
			ID:              ddlDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    10,
		},
	}, NewReplicaSet(model.ChangeFeedID{}, ddlDispatcherID, heartbeatpb.DDLSpanSchemaID, heartbeatpb.DDLSpan, 1))
	ddlCache := utils.NewBtreeMap[*heartbeatpb.TableSpan, *scheduler.StateMachine[common.DispatcherID]](heartbeatpb.LessTableSpan)
	ddlCache.ReplaceOrInsert(heartbeatpb.DDLSpan, ddlStm)

	require.False(t, s.bootstrapped)
	s.FinishBootstrap(map[int64]utils.Map[*heartbeatpb.TableSpan, *scheduler.StateMachine[common.DispatcherID]]{
		0: ddlCache,
		1: cached,
	})
	require.Equal(t, ddlDispatcherID, s.ddlDispatcherID)
	//total 9 regions,
	// table 1: 2 holes will be inserted to absent
	// table 2: split to 4 spans, will be inserted to absent
	require.Len(t, s.Absent(), 6)
	// table 1 has two working span,  plus a working ddl span
	require.Len(t, s.Working(), 3)
	require.True(t, s.bootstrapped)
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
