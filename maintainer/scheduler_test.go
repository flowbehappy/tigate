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
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/utils"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSchedule(t *testing.T) {
	s := NewScheduler("test", 9, time.Minute)
	s.nodeTasks["node1"] = utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine]()
	s.nodeTasks["node2"] = utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine]()
	s.nodeTasks["node3"] = utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine]()

	for i := 0; i < 1000; i++ {
		span := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{
			TableID: uint64(i),
		}}
		stm, _ := scheduler.NewStateMachine(span, nil,
			NewReplicaSet(model.ChangeFeedID{}, span, 1))
		s.absent.ReplaceOrInsert(span, stm)
	}
	msgs, err := s.Schedule()
	require.NoError(t, err)
	require.Len(t, msgs, 9)
	require.Equal(t, s.schedulingTask.Len(), 9)
	require.Equal(t, s.absent.Len(), 991)
	require.Equal(t, s.nodeTasks["node1"].Len(), 3)
	require.Equal(t, s.nodeTasks["node2"].Len(), 3)
	require.Equal(t, s.nodeTasks["node3"].Len(), 3)
}

func TestMoveTask(t *testing.T) {
	s := NewScheduler("test", 9, time.Minute)
	span := &common.TableSpan{
		TableSpan: &heartbeatpb.TableSpan{
			TableID: 1,
		},
	}
	stm, _ := scheduler.NewStateMachine(span, nil, NewReplicaSet(model.DefaultChangeFeedID("test"), span, 0))
	s.AddNewTask(stm)
	s.nodeTasks["a"] = utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine]()
	s.nodeTasks["b"] = utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine]()

	//absent->committing
	stm.State = scheduler.SchedulerStatusCommiting
	stm.Primary = "a"
	s.tryMoveTask(span, stm, scheduler.SchedulerStatusAbsent, "", true)
	require.Equal(t, 0, s.absent.Len())
	require.Equal(t, 1, s.nodeTasks["a"].Len())
	require.Equal(t, 1, s.schedulingTask.Len())

	//committing -> working
	stm.State = scheduler.SchedulerStatusWorking
	stm.Primary = "a"
	s.tryMoveTask(span, stm, scheduler.SchedulerStatusCommiting, "a", true)
	require.Equal(t, 0, s.absent.Len())
	require.Equal(t, 1, s.nodeTasks["a"].Len())
	require.Equal(t, 0, s.schedulingTask.Len())
	require.Equal(t, 1, s.working.Len())

	//working -> removing
	stm.HandleMoveInferior("b")
	stm.Primary = "a"
	stm.Secondary = "b"
	s.tryMoveTask(span, stm, scheduler.SchedulerStatusWorking, "a", true)
	require.Equal(t, 0, s.absent.Len())
	require.Equal(t, 1, s.nodeTasks["a"].Len())
	require.Equal(t, 1, s.schedulingTask.Len())
	require.Equal(t, 0, s.working.Len())

	// removing -> committing
	stm.HandleInferiorStatus(ReplicaSetStatus{
		ID:           span,
		State:        heartbeatpb.ComponentState_Stopped,
		CheckpointTs: 10,
	}, "a")
	s.tryMoveTask(span, stm, scheduler.SchedulerStatusRemoving, "a", true)
	require.Equal(t, 0, s.absent.Len())
	require.Equal(t, 0, s.nodeTasks["a"].Len())
	require.Equal(t, 1, s.nodeTasks["b"].Len())
	require.Equal(t, 1, s.schedulingTask.Len())
	require.Equal(t, 0, s.working.Len())

	//committing -> working
	stm.HandleInferiorStatus(ReplicaSetStatus{
		ID:           span,
		State:        heartbeatpb.ComponentState_Working,
		CheckpointTs: 10,
	}, "b")
	s.tryMoveTask(span, stm, scheduler.SchedulerStatusCommiting, "a", true)
	require.Equal(t, 0, s.absent.Len())
	require.Equal(t, 0, s.nodeTasks["a"].Len())
	require.Equal(t, 1, s.nodeTasks["b"].Len())
	require.Equal(t, 0, s.schedulingTask.Len())
	require.Equal(t, 1, s.working.Len())

	//working -> removing
	stm.HandleRemoveInferior()
	s.tryMoveTask(span, stm, scheduler.SchedulerStatusWorking, "b", true)

	// removing -> removed
	stm.HandleInferiorStatus(ReplicaSetStatus{
		ID:           span,
		State:        heartbeatpb.ComponentState_Stopped,
		CheckpointTs: 10,
	}, "b")
	s.tryMoveTask(span, stm, scheduler.SchedulerStatusWorking, "b", true)
	require.Equal(t, 0, s.absent.Len())
	require.Equal(t, 0, s.nodeTasks["a"].Len())
	require.Equal(t, 0, s.nodeTasks["b"].Len())
	require.Equal(t, 0, s.schedulingTask.Len())
	require.Equal(t, 0, s.working.Len())
}
