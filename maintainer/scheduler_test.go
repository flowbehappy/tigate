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
	"testing"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/utils"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestSchedule(t *testing.T) {
	s := NewScheduler("test", 9, time.Minute)
	s.nodeTasks["node1"] = map[common.DispatcherID]*scheduler.StateMachine{}
	s.nodeTasks["node2"] = map[common.DispatcherID]*scheduler.StateMachine{}
	s.nodeTasks["node3"] = map[common.DispatcherID]*scheduler.StateMachine{}

	for i := 0; i < 1000; i++ {
		span := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{
			TableID: uint64(i),
		}}
		dispatcherID := common.NewDispatcherID()
		stm, _ := scheduler.NewStateMachine(dispatcherID, nil,
			NewReplicaSet(model.ChangeFeedID{}, dispatcherID, span, 1))
		s.AddNewTask(stm)
	}
	msgs, err := s.Schedule()
	require.NoError(t, err)
	require.Len(t, msgs, 9)
	require.Equal(t, len(s.committing), 9)
	require.Equal(t, len(s.absent), 991)
	require.Equal(t, len(s.nodeTasks["node1"]), 3)
	require.Equal(t, len(s.nodeTasks["node2"]), 3)
	require.Equal(t, len(s.nodeTasks["node3"]), 3)
}

func TestMoveTask(t *testing.T) {
	s := NewScheduler("test", 9, time.Minute)
	span := &common.TableSpan{
		TableSpan: &heartbeatpb.TableSpan{
			TableID: 1,
		},
	}
	dispatcherID := common.NewDispatcherID()
	stm, _ := scheduler.NewStateMachine(dispatcherID, nil, NewReplicaSet(model.DefaultChangeFeedID("test"), dispatcherID, span, 0))
	s.AddNewTask(stm)
	s.nodeTasks["a"] = map[common.DispatcherID]*scheduler.StateMachine{}
	s.nodeTasks["b"] = map[common.DispatcherID]*scheduler.StateMachine{}

	//absent->committing
	stm.State = scheduler.SchedulerStatusCommiting
	stm.Primary = "a"
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusAbsent, "", true)
	require.Equal(t, 0, len(s.absent))
	require.Equal(t, 1, len(s.nodeTasks["a"]))
	require.Equal(t, 1, len(s.committing))

	//committing -> working
	stm.State = scheduler.SchedulerStatusWorking
	stm.Primary = "a"
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusCommiting, "a", true)
	require.Equal(t, 0, len(s.absent))
	require.Equal(t, 1, len(s.nodeTasks["a"]))
	require.Equal(t, 0, len(s.committing))
	require.Equal(t, 1, len(s.working))

	//working -> removing
	_, _ = stm.HandleMoveInferior("b")
	stm.Primary = "a"
	stm.Secondary = "b"
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusWorking, "a", true)
	require.Equal(t, 0, len(s.absent))
	require.Equal(t, 1, len(s.nodeTasks["a"]))
	require.Equal(t, 1, len(s.removing))
	require.Equal(t, 0, len(s.working))

	// removing -> committing
	_, _ = stm.HandleInferiorStatus(ReplicaSetStatus{
		ID:           dispatcherID,
		State:        heartbeatpb.ComponentState_Stopped,
		CheckpointTs: 10,
	}, "a")
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusRemoving, "a", true)
	require.Equal(t, 0, len(s.absent))
	require.Equal(t, 0, len(s.nodeTasks["a"]))
	require.Equal(t, 1, len(s.nodeTasks["b"]))
	require.Equal(t, 1, len(s.committing))
	require.Equal(t, 0, len(s.working))

	//committing -> working
	_, _ = stm.HandleInferiorStatus(ReplicaSetStatus{
		ID:           dispatcherID,
		State:        heartbeatpb.ComponentState_Working,
		CheckpointTs: 10,
	}, "b")
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusCommiting, "a", true)
	require.Equal(t, 0, len(s.absent))
	require.Equal(t, 0, len(s.nodeTasks["a"]))
	require.Equal(t, 1, len(s.nodeTasks["b"]))
	require.Equal(t, 0, len(s.committing))
	require.Equal(t, 1, len(s.working))

	//working -> removing
	stm.HandleRemoveInferior()
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusWorking, "b", true)

	// removing -> removed
	stm.HandleInferiorStatus(ReplicaSetStatus{
		ID:           dispatcherID,
		State:        heartbeatpb.ComponentState_Stopped,
		CheckpointTs: 10,
	}, "b")
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusWorking, "b", true)
	require.Equal(t, 0, len(s.absent))
	require.Equal(t, 0, len(s.nodeTasks["a"]))
	require.Equal(t, 0, len(s.nodeTasks["b"]))
	require.Equal(t, 0, len(s.committing))
	require.Equal(t, 0, len(s.working))
}

func TestBalance(t *testing.T) {
	s := NewScheduler("test", 1000, 0)
	s.nodeTasks["node1"] = map[common.DispatcherID]*scheduler.StateMachine{}
	for i := 0; i < 100; i++ {
		span := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{
			TableID: uint64(i),
		}}
		dispatcherID := common.NewDispatcherID()
		stm, _ := scheduler.NewStateMachine(dispatcherID, nil,
			NewReplicaSet(model.ChangeFeedID{}, dispatcherID, span, 1))
		stm.State = scheduler.SchedulerStatusWorking
		stm.Primary = "node1"
		s.working[dispatcherID] = stm
		s.nodeTasks["node1"][dispatcherID] = stm
	}
	msgs, err := s.Schedule()
	require.NoError(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, len(s.working), 100)
	require.Equal(t, len(s.nodeTasks["node1"]), 100)
	//add duplicate node
	s.AddNewNode("node1")
	// add new node
	s.AddNewNode("node2")
	msgs, err = s.TryBalance()
	require.NoError(t, err)
	require.Len(t, msgs, 50)
	require.Equal(t, len(s.removing), 50)
	require.Equal(t, len(s.working), 50)
	//still on the primary node
	require.Equal(t, len(s.nodeTasks["node1"]), 100)
	require.Equal(t, len(s.nodeTasks["node2"]), 0)

	// remove the node2
	msgs = s.RemoveNode("node2")
	require.Len(t, msgs, 0)
	require.Equal(t, len(s.removing), 0)
	// changed to working status
	require.Equal(t, len(s.working), 100)
	require.Equal(t, len(s.nodeTasks["node1"]), 100)
	msgs = s.RemoveNode("node2")
	require.Len(t, msgs, 0)
}

func TestStoppedWhenMoving(t *testing.T) {
	s := NewScheduler("test", 1000, 0)
	s.AddNewNode("node1")
	s.AddNewNode("node2")
	id := 1
	span := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{
		TableID: uint64(id),
	}}
	dispatcherID := common.NewDispatcherID()
	stm, _ := scheduler.NewStateMachine(dispatcherID, nil,
		NewReplicaSet(model.ChangeFeedID{}, dispatcherID, span, 1))
	stm.State = scheduler.SchedulerStatusWorking
	stm.Primary = "node1"
	s.working[dispatcherID] = stm
	s.nodeTasks["node1"][dispatcherID] = stm

	_, _ = stm.HandleMoveInferior("node2")
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusWorking, "node2", true)
	require.Equal(t, len(s.removing), 1)
	// changed to working status
	require.Equal(t, len(s.working), 0)
	require.Equal(t, len(s.nodeTasks["node1"]), 1)
	require.Equal(t, len(s.nodeTasks["node2"]), 0)

	msgs, err := s.HandleStatus("node1", []*heartbeatpb.TableSpanStatus{
		{
			ID:              dispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Stopped,
			CheckpointTs:    10,
		},
	})
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, len(s.committing), 1)
	require.Equal(t, len(s.removing), 0)
	require.Equal(t, len(s.nodeTasks["node1"]), 0)
	require.Equal(t, len(s.nodeTasks["node2"]), 1)
	require.Equal(t, uint64(10), stm.Inferior.(*ReplicaSet).checkpointTs)
}

func TestFinishBootstrap(t *testing.T) {
	s := NewScheduler("test", 1000, 0)
	s.AddNewNode("node1")
	span := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{
		TableID: uint64(1),
	}}
	dispatcherID := common.NewDispatcherID()
	stm, _ := scheduler.NewStateMachine(dispatcherID, nil,
		NewReplicaSet(model.ChangeFeedID{}, dispatcherID, span, 1))
	s.AddTempTask(span, stm)

	dispatcherID2 := common.NewDispatcherID()
	stm2, err := scheduler.NewStateMachine(dispatcherID2, map[model.CaptureID]scheduler.InferiorStatus{
		"node1": ReplicaSetStatus{
			ID:           dispatcherID2,
			State:        heartbeatpb.ComponentState_Working,
			CheckpointTs: 10,
			DDLStatus:    nil,
		},
	}, NewReplicaSet(model.ChangeFeedID{}, dispatcherID2, span, 1))
	require.Nil(t, err)
	cached := utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine]()
	cached.ReplaceOrInsert(span, stm2)
	require.False(t, s.bootstrapped)
	s.FinishBootstrap(cached)
	require.True(t, s.bootstrapped)
	require.Equal(t, len(s.nodeTasks["node1"]), 1)
	require.Equal(t, len(s.working), 1)
	require.Equal(t, len(s.absent), 0)
	require.Equal(t, len(s.committing), 0)
	require.Equal(t, len(s.removing), 0)
	require.Equal(t, stm2, s.working[dispatcherID2])
	require.Nil(t, s.tempTasks)
	require.Panics(t, func() {
		s.FinishBootstrap(cached)
	})
}

// 4 tasks and 2 servers, then add one server, no re-balance will be triggered
func TestBalanceUnEvenTask(t *testing.T) {
	s := NewScheduler("test", 1000, 0)
	s.AddNewNode("node1")
	s.AddNewNode("node2")
	for i := 0; i < 4; i++ {
		span := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{
			TableID: uint64(i),
		}}
		dispatcherID := common.NewDispatcherID()
		stm, _ := scheduler.NewStateMachine(dispatcherID, nil,
			NewReplicaSet(model.ChangeFeedID{}, dispatcherID, span, 1))
		s.AddNewTask(stm)
	}
	msgs, err := s.Schedule()
	require.NoError(t, err)
	require.Len(t, msgs, 4)
	require.Equal(t, len(s.committing), 4)
	require.Equal(t, len(s.nodeTasks["node1"]), 2)
	require.Equal(t, len(s.nodeTasks["node2"]), 2)
	f := func(m map[common.DispatcherID]*scheduler.StateMachine) []*heartbeatpb.TableSpanStatus {
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
	require.Equal(t, len(s.working), 4)
	require.Equal(t, len(s.committing), 0)

	// add new node
	s.AddNewNode("node3")
	msgs, err = s.TryBalance()
	require.NoError(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, len(s.removing), 0)
	require.Equal(t, len(s.working), 4)
	//still on the primary node
	require.Equal(t, len(s.nodeTasks["node1"]), 2)
	require.Equal(t, len(s.nodeTasks["node2"]), 2)
	require.Equal(t, len(s.nodeTasks["node3"]), 0)
}
