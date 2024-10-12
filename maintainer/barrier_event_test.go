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
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/stretchr/testify/require"
)

func TestScheduleEvent(t *testing.T) {
	controller := NewController("test", 1, nil, nil, nil, 1000, 0)
	controller.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 1}, 1)
	event := NewBlockEvent("test", controller, &heartbeatpb.State{
		IsBlocked: true,
		BlockTs:   10,
		NeedDroppedTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_All,
			SchemaID:      1,
		},
		NeedAddedTables: []*heartbeatpb.Table{{2, 1}, {3, 1}},
	})
	event.scheduleBlockEvent()
	//drop table will be executed first
	require.Len(t, controller.Absent(), 2)

	event = NewBlockEvent("test", controller, &heartbeatpb.State{
		IsBlocked: true,
		BlockTs:   10,
		NeedDroppedTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_DB,
			SchemaID:      1,
		},
		NeedAddedTables: []*heartbeatpb.Table{{4, 1}},
	})
	event.scheduleBlockEvent()
	//drop table will be executed first, then add the new table
	require.Len(t, controller.Absent(), 1)

	event = NewBlockEvent("test", controller, &heartbeatpb.State{
		IsBlocked: true,
		BlockTs:   10,
		NeedDroppedTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			TableIDs:      []int64{4},
		},
		NeedAddedTables: []*heartbeatpb.Table{{5, 1}},
	})
	event.scheduleBlockEvent()
	//drop table will be executed first, then add the new table
	require.Len(t, controller.Absent(), 1)
}

func TestResendAction(t *testing.T) {
	controller := NewController("test", 1, nil, nil, nil, 1000, 0)
	controller.AddNewNode("node1")
	controller.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 1}, 1)
	controller.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 2}, 1)
	var dispatcherIDs []common.DispatcherID
	for key, stm := range controller.Absent() {
		stm.Primary = "node1"
		stm.State = scheduler.SchedulerStatusWorking
		controller.tryMoveTask(key, stm, scheduler.SchedulerStatusAbsent, "", true)
		dispatcherIDs = append(dispatcherIDs, key)
	}
	event := NewBlockEvent("test", controller, &heartbeatpb.State{
		IsBlocked: true,
		BlockTs:   10,
		BlockTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_All,
		},
	})
	// time is not reached
	event.lastResendTime = time.Now()
	event.selected = true
	msgs := event.resend()
	require.Len(t, msgs, 0)

	// time is not reached
	event.lastResendTime = time.Time{}
	event.selected = false
	msgs = event.resend()
	require.Len(t, msgs, 0)

	// resend write action
	event.selected = true
	event.writerDispatcherAdvanced = false
	event.writerDispatcher = dispatcherIDs[0]
	msgs = event.resend()
	require.Len(t, msgs, 1)

	event = NewBlockEvent("test", controller, &heartbeatpb.State{
		IsBlocked: true,
		BlockTs:   10,
		BlockTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_DB,
			SchemaID:      1,
		},
	})
	event.selected = true
	event.writerDispatcherAdvanced = true
	msgs = event.resend()
	require.Len(t, msgs, 1)
	resp := msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.Equal(t, resp.DispatcherStatuses[0].Action.Action, heartbeatpb.Action_Pass)
	require.Equal(t, resp.DispatcherStatuses[0].InfluencedDispatchers.InfluenceType, heartbeatpb.InfluenceType_DB)
	require.Equal(t, resp.DispatcherStatuses[0].Action.CommitTs, uint64(10))

	event = NewBlockEvent("test", controller, &heartbeatpb.State{
		IsBlocked: true,
		BlockTs:   10,
		BlockTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_All,
			SchemaID:      1,
		},
	})
	event.selected = true
	event.writerDispatcherAdvanced = true
	msgs = event.resend()
	require.Len(t, msgs, 1)
	resp = msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.Equal(t, resp.DispatcherStatuses[0].Action.Action, heartbeatpb.Action_Pass)
	require.Equal(t, resp.DispatcherStatuses[0].InfluencedDispatchers.InfluenceType, heartbeatpb.InfluenceType_All)
	require.Equal(t, resp.DispatcherStatuses[0].Action.CommitTs, uint64(10))

	event = NewBlockEvent("test", controller, &heartbeatpb.State{
		IsBlocked: true,
		BlockTs:   10,
		BlockTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			TableIDs:      []int64{1, 2},
			SchemaID:      1,
		},
	})
	event.selected = true
	event.writerDispatcherAdvanced = true
	msgs = event.resend()
	require.Len(t, msgs, 1)
	resp = msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.Equal(t, resp.DispatcherStatuses[0].InfluencedDispatchers.InfluenceType, heartbeatpb.InfluenceType_Normal)
	require.Len(t, resp.DispatcherStatuses[0].InfluencedDispatchers.DispatcherIDs, 2)
	require.Equal(t, resp.DispatcherStatuses[0].Action.Action, heartbeatpb.Action_Pass)
	require.Equal(t, resp.DispatcherStatuses[0].Action.CommitTs, uint64(10))
}

func TestUpdateSchemaID(t *testing.T) {
	controller := NewController("test", 1, nil, nil, nil, 1000, 0)
	controller.AddNewNode("node1")
	controller.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 1}, 1)
	require.Len(t, controller.Absent(), 1)
	require.Len(t, controller.GetTasksBySchemaID(1), 1)
	event := NewBlockEvent("test", controller, &heartbeatpb.State{
		IsBlocked: true,
		BlockTs:   10,
		BlockTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_All,
		},
		UpdatedSchemas: []*heartbeatpb.SchemaIDChange{
			{
				TableID:     1,
				OldSchemaID: 1,
				NewSchemaID: 2,
			},
		}},
	)
	event.scheduleBlockEvent()
	require.Len(t, controller.Absent(), 1)
	// check the schema id and map is updated
	require.Len(t, controller.GetTasksBySchemaID(1), 0)
	require.Len(t, controller.GetTasksBySchemaID(2), 1)
	require.Equal(t, controller.GetTasksByTableIDs(1)[0].Inferior.(*ReplicaSet).SchemaID, int64(2))
}
