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
	"github.com/stretchr/testify/require"
)

func TestScheduleEvent(t *testing.T) {
	sche := NewScheduler("test", 1, nil, nil, nil, 1000, 0)
	sche.AddNewTable(common.Table{1, 1}, 1)
	event := NewBlockEvent("test", sche, &heartbeatpb.State{
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
	require.Len(t, sche.Absent(), 2)

	event = NewBlockEvent("test", sche, &heartbeatpb.State{
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
	require.Len(t, sche.Absent(), 1)

	event = NewBlockEvent("test", sche, &heartbeatpb.State{
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
	require.Len(t, sche.Absent(), 1)
}

func TestResendAction(t *testing.T) {
	sche := NewScheduler("test", 1, nil, nil, nil, 1000, 0)
	sche.AddNewNode("node1")
	sche.AddNewTable(common.Table{1, 1}, 1)
	sche.AddNewTable(common.Table{1, 2}, 1)
	var dispatcherIDs []common.DispatcherID
	for key, stm := range sche.Absent() {
		stm.Primary = "node1"
		stm.State = scheduler.SchedulerStatusWorking
		sche.tryMoveTask(key, stm, scheduler.SchedulerStatusAbsent, "", true)
		dispatcherIDs = append(dispatcherIDs, key)
	}
	event := NewBlockEvent("test", sche, &heartbeatpb.State{
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

	event = NewBlockEvent("test", sche, &heartbeatpb.State{
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

	event = NewBlockEvent("test", sche, &heartbeatpb.State{
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

	event = NewBlockEvent("test", sche, &heartbeatpb.State{
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
