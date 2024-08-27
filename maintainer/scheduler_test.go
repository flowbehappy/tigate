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
		s.absent[dispatcherID] = stm
	}
	msgs, err := s.Schedule()
	require.NoError(t, err)
	require.Len(t, msgs, 9)
	require.Equal(t, len(s.schedulingTask), 9)
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
	require.Equal(t, 1, len(s.schedulingTask))

	//committing -> working
	stm.State = scheduler.SchedulerStatusWorking
	stm.Primary = "a"
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusCommiting, "a", true)
	require.Equal(t, 0, len(s.absent))
	require.Equal(t, 1, len(s.nodeTasks["a"]))
	require.Equal(t, 0, len(s.schedulingTask))
	require.Equal(t, 1, len(s.working))

	//working -> removing
	stm.HandleMoveInferior("b")
	stm.Primary = "a"
	stm.Secondary = "b"
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusWorking, "a", true)
	require.Equal(t, 0, len(s.absent))
	require.Equal(t, 1, len(s.nodeTasks["a"]))
	require.Equal(t, 1, len(s.schedulingTask))
	require.Equal(t, 0, len(s.working))

	// removing -> committing
	stm.HandleInferiorStatus(ReplicaSetStatus{
		ID:           dispatcherID,
		State:        heartbeatpb.ComponentState_Stopped,
		CheckpointTs: 10,
	}, "a")
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusRemoving, "a", true)
	require.Equal(t, 0, len(s.absent))
	require.Equal(t, 0, len(s.nodeTasks["a"]))
	require.Equal(t, 1, len(s.nodeTasks["b"]))
	require.Equal(t, 1, len(s.schedulingTask))
	require.Equal(t, 0, len(s.working))

	//committing -> working
	stm.HandleInferiorStatus(ReplicaSetStatus{
		ID:           dispatcherID,
		State:        heartbeatpb.ComponentState_Working,
		CheckpointTs: 10,
	}, "b")
	s.tryMoveTask(dispatcherID, stm, scheduler.SchedulerStatusCommiting, "a", true)
	require.Equal(t, 0, len(s.absent))
	require.Equal(t, 0, len(s.nodeTasks["a"]))
	require.Equal(t, 1, len(s.nodeTasks["b"]))
	require.Equal(t, 0, len(s.schedulingTask))
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
	require.Equal(t, 0, len(s.schedulingTask))
	require.Equal(t, 0, len(s.working))
}
