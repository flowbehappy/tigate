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
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

func TestScheduleEvent(t *testing.T) {
	setNodeManagerAndMessageCenter()
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test")
	tsoClient := &replica.MockTsoClient{}
	ddlSpan := replica.NewWorkingReplicaSet(cfID, tableTriggerEventDispatcherID,
		tsoClient, heartbeatpb.DDLSpanSchemaID,
		heartbeatpb.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "test1")
	controller := NewController(cfID, 1, nil, tsoClient, nil, nil, nil, ddlSpan, 1000, 0)
	controller.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 1}, 1)
	event := NewBlockEvent(cfID, controller, &heartbeatpb.State{
		IsBlocked:         true,
		BlockTs:           10,
		NeedDroppedTables: &heartbeatpb.InfluencedTables{InfluenceType: heartbeatpb.InfluenceType_All},
		NeedAddedTables:   []*heartbeatpb.Table{{2, 1}, {3, 1}},
	}, true)
	event.scheduleBlockEvent()
	//drop table will be executed first
	require.Equal(t, 2, controller.replicationDB.GetAbsentSize())

	event = NewBlockEvent(cfID, controller, &heartbeatpb.State{
		IsBlocked: true,
		BlockTs:   10,
		NeedDroppedTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_DB,
			SchemaID:      1,
		},
		NeedAddedTables: []*heartbeatpb.Table{{4, 1}},
	}, false)
	event.scheduleBlockEvent()
	//drop table will be executed first, then add the new table
	require.Equal(t, 1, controller.replicationDB.GetAbsentSize())

	event = NewBlockEvent(cfID, controller, &heartbeatpb.State{
		IsBlocked: true,
		BlockTs:   10,
		NeedDroppedTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			TableIDs:      []int64{4},
		},
		NeedAddedTables: []*heartbeatpb.Table{{5, 1}},
	}, false)
	event.scheduleBlockEvent()
	//drop table will be executed first, then add the new table
	require.Equal(t, 1, controller.replicationDB.GetAbsentSize())
}

func TestResendAction(t *testing.T) {
	nodeManager := setNodeManagerAndMessageCenter()
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test")
	tsoClient := &replica.MockTsoClient{}
	ddlSpan := replica.NewWorkingReplicaSet(cfID, tableTriggerEventDispatcherID,
		tsoClient, heartbeatpb.DDLSpanSchemaID,
		heartbeatpb.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	controller := NewController(cfID, 1, nil, tsoClient, nil, nil, nil, ddlSpan, 1000, 0)
	controller.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 1}, 1)
	controller.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 2}, 1)
	var dispatcherIDs []common.DispatcherID
	absents := controller.replicationDB.GetAbsent(make([]*replica.SpanReplication, 0), 100)
	for _, stm := range absents {
		controller.replicationDB.BindSpanToNode("", "node1", stm)
		controller.replicationDB.MarkSpanReplicating(stm)
		dispatcherIDs = append(dispatcherIDs, stm.ID)
	}
	event := NewBlockEvent(cfID, controller, &heartbeatpb.State{
		IsBlocked: true,
		BlockTs:   10,
		BlockTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_All,
		},
	}, false)
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

	event = NewBlockEvent(cfID, controller, &heartbeatpb.State{
		IsBlocked: true,
		BlockTs:   10,
		BlockTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_DB,
			SchemaID:      1,
		},
	}, false)
	event.selected = true
	event.writerDispatcherAdvanced = true
	msgs = event.resend()
	require.Len(t, msgs, 1)
	resp := msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.Equal(t, resp.DispatcherStatuses[0].Action.Action, heartbeatpb.Action_Pass)
	require.Equal(t, resp.DispatcherStatuses[0].InfluencedDispatchers.InfluenceType, heartbeatpb.InfluenceType_DB)
	require.Equal(t, resp.DispatcherStatuses[0].Action.CommitTs, uint64(10))

	event = NewBlockEvent(cfID, controller, &heartbeatpb.State{
		IsBlocked: true,
		BlockTs:   10,
		BlockTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_All,
			SchemaID:      1,
		},
	}, false)
	event.selected = true
	event.writerDispatcherAdvanced = true
	msgs = event.resend()
	require.Len(t, msgs, 1)
	resp = msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.Equal(t, resp.DispatcherStatuses[0].Action.Action, heartbeatpb.Action_Pass)
	require.Equal(t, resp.DispatcherStatuses[0].InfluencedDispatchers.InfluenceType, heartbeatpb.InfluenceType_All)
	require.Equal(t, resp.DispatcherStatuses[0].Action.CommitTs, uint64(10))

	event = NewBlockEvent(cfID, controller, &heartbeatpb.State{
		IsBlocked: true,
		BlockTs:   10,
		BlockTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			TableIDs:      []int64{1, 2},
			SchemaID:      1,
		},
	}, false)
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
	setNodeManagerAndMessageCenter()
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test")
	tsoClient := &replica.MockTsoClient{}
	ddlSpan := replica.NewWorkingReplicaSet(cfID, tableTriggerEventDispatcherID,
		tsoClient, heartbeatpb.DDLSpanSchemaID,
		heartbeatpb.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	controller := NewController(cfID, 1, nil, tsoClient, nil, nil, nil, ddlSpan, 1000, 0)
	controller.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 1}, 1)
	require.Equal(t, 1, controller.replicationDB.GetAbsentSize())
	require.Len(t, controller.GetTasksBySchemaID(1), 1)
	event := NewBlockEvent(cfID, controller, &heartbeatpb.State{
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
		}}, true,
	)
	event.scheduleBlockEvent()
	require.Equal(t, 1, controller.replicationDB.GetAbsentSize())
	// check the schema id and map is updated
	require.Len(t, controller.GetTasksBySchemaID(1), 0)
	require.Len(t, controller.GetTasksBySchemaID(2), 1)
	require.Equal(t, controller.GetTasksByTableIDs(1)[0].GetSchemaID(), int64(2))
}

func setNodeManagerAndMessageCenter() *watcher.NodeManager {
	n := node.NewInfo("", "")
	appcontext.SetService(appcontext.MessageCenter, messaging.NewMessageCenter(context.Background(),
		n.ID, 100, config.NewDefaultMessageCenterConfig()))
	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	return nodeManager
}
