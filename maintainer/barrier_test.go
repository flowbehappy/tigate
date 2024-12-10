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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestOneBlockEvent(t *testing.T) {
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
	controller := NewController(cfID, 1, nil, tsoClient,
		nil, nil, nil, ddlSpan, 1000, 0)
	startTs := uint64(10)
	controller.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 1}, startTs)
	stm := controller.GetTasksByTableIDs(1)[0]
	controller.replicationDB.BindSpanToNode("", "node1", stm)
	controller.replicationDB.MarkSpanReplicating(stm)

	barrier := NewBarrier(controller, false)
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: controller.ddlDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_All,
					},
					IsSyncPoint: true,
				},
			},
			{
				ID: stm.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_All,
					},
					IsSyncPoint: true,
				},
			},
		},
	})
	require.NotNil(t, msg)
	key := eventKey{
		blockTs:     10,
		isSyncPoint: true,
	}
	resp := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	event := barrier.blockedTs[key]
	require.Equal(t, uint64(10), event.commitTs)
	require.True(t, event.writerDispatcher == controller.ddlDispatcherID)
	require.True(t, event.selected)
	require.False(t, event.writerDispatcherAdvanced)
	require.Len(t, resp.DispatcherStatuses, 2)
	require.Equal(t, resp.DispatcherStatuses[0].Ack.CommitTs, uint64(10))
	require.Equal(t, resp.DispatcherStatuses[1].Action.CommitTs, uint64(10))
	require.Equal(t, resp.DispatcherStatuses[1].Action.Action, heartbeatpb.Action_Write)
	require.True(t, resp.DispatcherStatuses[1].Action.IsSyncPoint)

	// test resend action and syncpoint is set
	msgs := event.resend()
	require.Len(t, msgs, 1)
	require.True(t, msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse).DispatcherStatuses[0].Action.Action == heartbeatpb.Action_Write)
	require.True(t, msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse).DispatcherStatuses[0].Action.IsSyncPoint)

	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: controller.ddlDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					BlockTs:     10,
					IsBlocked:   true,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
			{
				ID: stm.ID.ToPB(),
				State: &heartbeatpb.State{
					BlockTs:     10,
					IsBlocked:   true,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
		},
	})
	require.NotNil(t, msg)
	resp = msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Equal(t, resp.DispatcherStatuses[0].Ack.CommitTs, uint64(10))
	require.Len(t, barrier.blockedTs, 0)

	//send event done again
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: controller.ddlDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					BlockTs:     10,
					IsBlocked:   true,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
			{
				ID: stm.ID.ToPB(),
				State: &heartbeatpb.State{
					BlockTs:     10,
					IsBlocked:   true,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
		},
	})
	require.Len(t, barrier.blockedTs, 0)
	// no event if found, no message will be sent
	require.NotNil(t, msg)
	require.Equal(t, resp.DispatcherStatuses[0].Ack.CommitTs, uint64(10))
}

func TestNormalBlock(t *testing.T) {
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
	var blockedDispatcherIDS []*heartbeatpb.DispatcherID
	for id := 1; id < 4; id++ {
		controller.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: int64(id)}, 10)
		stm := controller.GetTasksByTableIDs(int64(id))[0]
		blockedDispatcherIDS = append(blockedDispatcherIDS, stm.ID.ToPB())
		controller.replicationDB.BindSpanToNode("", "node1", stm)
		controller.replicationDB.MarkSpanReplicating(stm)
	}

	// the last one is the writer
	var selectDispatcherID = common.NewDispatcherIDFromPB(blockedDispatcherIDS[2])
	selectedRep := controller.GetTask(selectDispatcherID)
	controller.replicationDB.BindSpanToNode("node1", "node2", selectedRep)
	dropID := selectedRep.Span.TableID

	newSpan := &heartbeatpb.Table{TableID: 10, SchemaID: 1}
	barrier := NewBarrier(controller, false)

	// first node block request
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: blockedDispatcherIDS[0],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{1, 2, 3},
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{dropID},
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
				},
			},
			{
				ID: blockedDispatcherIDS[1],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{1, 2, 3},
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{dropID},
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
				},
			},
		},
	})
	require.NotNil(t, msg)
	resp := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)
	require.Len(t, resp.DispatcherStatuses[0].InfluencedDispatchers.DispatcherIDs, 2)

	// other node block request
	msg = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: selectDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{1, 2, 3},
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{dropID},
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
				},
			},
		},
	})
	require.NotNil(t, msg)
	key := eventKey{
		blockTs:     10,
		isSyncPoint: false,
	}
	event := barrier.blockedTs[key]
	require.Equal(t, uint64(10), event.commitTs)
	require.True(t, event.writerDispatcher == selectDispatcherID)
	// all dispatcher reported, the reported status is reset
	require.False(t, event.rangeChecker.IsFullyCovered())

	// repeated status
	barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: blockedDispatcherIDS[0],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{1, 2, 3},
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{dropID},
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
				},
			},
			{
				ID: blockedDispatcherIDS[1],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{1, 2, 3},
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{dropID},
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
				},
			},
		},
	})
	require.Equal(t, uint64(10), event.commitTs)
	require.True(t, event.writerDispatcher == selectDispatcherID)

	// selected node write done
	msg = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: blockedDispatcherIDS[2],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					Stage:     heartbeatpb.BlockStage_DONE,
				},
			},
		},
	})
	require.Len(t, barrier.blockedTs, 1)
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: blockedDispatcherIDS[0],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					Stage:     heartbeatpb.BlockStage_DONE,
				},
			},
			{
				ID: blockedDispatcherIDS[1],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					Stage:     heartbeatpb.BlockStage_DONE,
				},
			},
		},
	})
	require.Len(t, barrier.blockedTs, 0)
}

func TestNormalBlockWithTableTrigger(t *testing.T) {
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
	var blockedDispatcherIDS []*heartbeatpb.DispatcherID
	for id := 1; id < 3; id++ {
		controller.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: int64(id)}, 10)
		stm := controller.GetTasksByTableIDs(int64(id))[0]
		blockedDispatcherIDS = append(blockedDispatcherIDS, stm.ID.ToPB())
		controller.replicationDB.BindSpanToNode("", "node1", stm)
		controller.replicationDB.MarkSpanReplicating(stm)
	}

	newSpan := &heartbeatpb.Table{TableID: 10, SchemaID: 1}
	barrier := NewBarrier(controller, false)

	// first node block request
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: blockedDispatcherIDS[0],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{0, 1, 2},
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{2},
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
				},
			},
		},
	})
	require.NotNil(t, msg)
	resp := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)
	require.Len(t, resp.DispatcherStatuses[0].InfluencedDispatchers.DispatcherIDs, 1)
	require.False(t, barrier.blockedTs[eventKey{blockTs: 10, isSyncPoint: false}].tableTriggerDispatcherRelated)

	// table trigger  block request
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: tableTriggerEventDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{0, 1, 2},
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{2},
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
				},
			},
			{
				ID: blockedDispatcherIDS[1],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{0, 2, 3},
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{2},
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
				},
			},
		},
	})
	require.NotNil(t, msg)
	key := eventKey{
		blockTs:     10,
		isSyncPoint: false,
	}
	event := barrier.blockedTs[key]
	require.Equal(t, uint64(10), event.commitTs)
	require.True(t, event.writerDispatcher == tableTriggerEventDispatcherID)
	// all dispatcher reported, the reported status is reset
	require.False(t, event.rangeChecker.IsFullyCovered())
	require.True(t, event.tableTriggerDispatcherRelated)

	// table trigger write done
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: tableTriggerEventDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					Stage:     heartbeatpb.BlockStage_DONE,
				},
			},
		},
	})
	require.Len(t, barrier.blockedTs, 1)
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: blockedDispatcherIDS[0],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					Stage:     heartbeatpb.BlockStage_DONE,
				},
			},
			{
				ID: blockedDispatcherIDS[1],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					Stage:     heartbeatpb.BlockStage_DONE,
				},
			},
		},
	})
	require.Len(t, barrier.blockedTs, 0)
}

func TestSchemaBlock(t *testing.T) {
	nm := setNodeManagerAndMessageCenter()
	nmap := nm.GetAliveNodes()
	for key, _ := range nmap {
		delete(nmap, key)
	}
	nmap["node1"] = &node.Info{ID: "node1"}
	nmap["node2"] = &node.Info{ID: "node2"}
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
	controller.AddNewTable(commonEvent.Table{SchemaID: 2, TableID: 3}, 1)
	var dispatcherIDs []*heartbeatpb.DispatcherID
	var dropTables = []int64{1, 2}
	absents := controller.replicationDB.GetAbsent(make([]*replica.SpanReplication, 0), 100)
	for _, stm := range absents {
		if stm.GetSchemaID() == 1 {
			dispatcherIDs = append(dispatcherIDs, stm.ID.ToPB())
		}
		controller.replicationDB.BindSpanToNode("", "node1", stm)
		controller.replicationDB.MarkSpanReplicating(stm)
	}

	newTable := &heartbeatpb.Table{TableID: 10, SchemaID: 2}
	barrier := NewBarrier(controller, true)

	// first dispatcher  block request
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: controller.ddlDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_DB,
						SchemaID:      1,
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      dropTables,
					},
					NeedAddedTables: []*heartbeatpb.Table{newTable},
				},
			},
			{
				ID: dispatcherIDs[0],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_DB,
						SchemaID:      1,
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      dropTables,
					},
					NeedAddedTables: []*heartbeatpb.Table{newTable},
				},
			},
		},
	})
	require.NotNil(t, msg)
	resp := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)

	// second dispatcher  block request
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcherIDs[1],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_DB,
						SchemaID:      1,
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      dropTables,
					},
					NeedAddedTables: []*heartbeatpb.Table{newTable},
				},
			},
		},
	})
	require.NotNil(t, msg)
	resp = msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 2)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)
	require.True(t, resp.DispatcherStatuses[1].Action.CommitTs == 10)
	require.True(t, resp.DispatcherStatuses[1].Action.Action == heartbeatpb.Action_Write)
	key := eventKey{blockTs: 10}
	event := barrier.blockedTs[key]
	require.Equal(t, uint64(10), event.commitTs)
	//the ddl dispatcher will be the writer
	require.Equal(t, event.writerDispatcher, controller.ddlDispatcherID)

	// repeated status
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcherIDs[1],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_DB,
						SchemaID:      1,
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      dropTables,
					},
					NeedAddedTables: []*heartbeatpb.Table{newTable},
				},
			},
		},
	})
	// ack and write message
	resp = msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)
	event = barrier.blockedTs[key]
	require.Equal(t, uint64(10), event.commitTs)
	//the ddl dispatcher will be the writer
	require.Equal(t, event.writerDispatcher, controller.ddlDispatcherID)

	// selected node write done
	msg = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: controller.ddlDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					Stage:     heartbeatpb.BlockStage_DONE,
				},
			},
		},
	})
	// 1 pass action message to one node
	msgs := barrier.Resend()
	require.Len(t, msgs, 1)
	msg = msgs[0]
	require.Equal(t, messaging.TypeHeartBeatResponse, msg.Type)
	require.Equal(t, msg.Message[0].(*heartbeatpb.HeartBeatResponse).DispatcherStatuses[0].Action.Action,
		heartbeatpb.Action_Pass)
	require.Len(t, barrier.blockedTs, 1)
	// other dispatcher advanced checkpoint ts
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcherIDs[0],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					Stage:     heartbeatpb.BlockStage_DONE,
				},
			},
			{
				ID: dispatcherIDs[1],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					Stage:     heartbeatpb.BlockStage_DONE,
				},
			},
		},
	})
	require.Len(t, barrier.blockedTs, 0)

	require.Equal(t, 1, controller.replicationDB.GetAbsentSize())
	require.Equal(t, 2, controller.operatorController.OperatorSize())
	// two dispatcher and moved to operator queue, operator will be removed after ack
	require.Equal(t, 1, controller.replicationDB.GetReplicatingSize())
	for _, task := range controller.replicationDB.GetReplicating() {
		op := controller.operatorController.GetOperator(task.ID)
		if op != nil {
			op.PostFinish()
		}
	}
	require.Equal(t, 1, controller.replicationDB.GetReplicatingSize())
}

func TestSyncPointBlock(t *testing.T) {
	nm := setNodeManagerAndMessageCenter()
	nmap := nm.GetAliveNodes()
	for key, _ := range nmap {
		delete(nmap, key)
	}
	nmap["node1"] = &node.Info{ID: "node1"}
	nmap["node2"] = &node.Info{ID: "node2"}
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
	controller.AddNewTable(commonEvent.Table{SchemaID: 2, TableID: 3}, 1)
	var dispatcherIDs []*heartbeatpb.DispatcherID
	var dropTables = []int64{1, 2, 3}
	absents := controller.replicationDB.GetAbsent(make([]*replica.SpanReplication, 0), 10000)
	for _, stm := range absents {
		dispatcherIDs = append(dispatcherIDs, stm.ID.ToPB())
		controller.replicationDB.BindSpanToNode("", "node1", stm)
		controller.replicationDB.MarkSpanReplicating(stm)
	}
	var selectDispatcherID = common.NewDispatcherIDFromPB(dispatcherIDs[2])
	selectedRep := controller.GetTask(selectDispatcherID)
	controller.replicationDB.BindSpanToNode("node1", "node2", selectedRep)

	newSpan := &heartbeatpb.Table{TableID: 10, SchemaID: 2}
	barrier := NewBarrier(controller, true)
	// first dispatcher  block request
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: controller.ddlDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_All,
						SchemaID:      1,
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      dropTables,
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
					IsSyncPoint:     true,
				},
			},
			{
				ID: dispatcherIDs[0],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_All,
						SchemaID:      1,
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      dropTables,
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
					IsSyncPoint:     true,
				},
			},
			{
				ID: dispatcherIDs[1],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_All,
						SchemaID:      1,
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      dropTables,
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
					IsSyncPoint:     true,
				},
			},
		},
	})
	// 3 ack messages, including the ddl dispatcher
	resp := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.Len(t, resp.DispatcherStatuses[0].InfluencedDispatchers.DispatcherIDs, 3)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)

	// second dispatcher  block request
	msg = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcherIDs[2],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_All,
						SchemaID:      1,
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      dropTables,
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
					IsSyncPoint:     true,
				},
			},
		},
	})
	// ack and write message
	resp = msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 2)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)
	require.True(t, resp.DispatcherStatuses[1].Action.CommitTs == 10)
	require.True(t, resp.DispatcherStatuses[1].Action.Action == heartbeatpb.Action_Write)
	key := eventKey{blockTs: 10, isSyncPoint: true}
	event := barrier.blockedTs[key]
	require.Equal(t, uint64(10), event.commitTs)
	//the last one will be the writer
	require.Equal(t, event.writerDispatcher, controller.ddlDispatcherID)

	// selected node write done
	_ = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: controller.ddlDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     10,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
		},
	})
	msgs := barrier.Resend()
	// 2 pass action messages to one node
	require.Len(t, msgs, 2)
	require.Len(t, barrier.blockedTs, 1)
	// other dispatcher advanced checkpoint ts
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcherIDs[0],
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     10,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
			{
				ID: dispatcherIDs[1],
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     10,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
			{
				ID: dispatcherIDs[2],
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     10,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
		},
	})
	require.Len(t, barrier.blockedTs, 0)
}

func TestNonBlocked(t *testing.T) {
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
	barrier := NewBarrier(controller, false)

	var blockedDispatcherIDS []*heartbeatpb.DispatcherID
	for id := 1; id < 4; id++ {
		blockedDispatcherIDS = append(blockedDispatcherIDS, common.NewDispatcherID().ToPB())
	}
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: blockedDispatcherIDS[0],
				State: &heartbeatpb.State{
					IsBlocked: false,
					BlockTs:   10,
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						TableIDs:      []int64{1, 2, 3},
						InfluenceType: heartbeatpb.InfluenceType_Normal,
					},
					NeedAddedTables: []*heartbeatpb.Table{
						{TableID: 1, SchemaID: 1}, {TableID: 2, SchemaID: 2},
					},
				},
			},
		},
	})
	// 1 ack  message
	require.NotNil(t, msg)
	resp := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.Equal(t, uint64(10), resp.DispatcherStatuses[0].Ack.CommitTs)
	require.True(t, heartbeatpb.InfluenceType_Normal == resp.DispatcherStatuses[0].InfluencedDispatchers.InfluenceType)
	require.Equal(t, resp.DispatcherStatuses[0].InfluencedDispatchers.DispatcherIDs[0], blockedDispatcherIDS[0])
	require.Len(t, barrier.blockedTs, 0)
	require.Equal(t, 2, barrier.controller.replicationDB.GetAbsentSize(), 2)
}

func TestUpdateCheckpointTs(t *testing.T) {
	setNodeManagerAndMessageCenter()
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test")

	tsoClient := &replica.MockTsoClient{Phy: 2, Logic: 3}
	ddlSpan := replica.NewWorkingReplicaSet(cfID, tableTriggerEventDispatcherID,
		tsoClient, heartbeatpb.DDLSpanSchemaID,
		heartbeatpb.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	controller := NewController(cfID, 1, nil, tsoClient,
		nil, nil, nil, ddlSpan, 1000, 0)

	barrier := NewBarrier(controller, false)
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: controller.ddlDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{0},
					},
					IsSyncPoint: false,
				},
			},
		},
	})
	require.NotNil(t, msg)
	key := eventKey{
		blockTs:     10,
		isSyncPoint: false,
	}
	resp := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	event := barrier.blockedTs[key]
	require.Equal(t, uint64(10), event.commitTs)
	require.True(t, event.writerDispatcher == controller.ddlDispatcherID)
	require.True(t, event.selected)
	require.False(t, event.writerDispatcherAdvanced)
	require.Len(t, resp.DispatcherStatuses, 2)
	require.Equal(t, resp.DispatcherStatuses[0].Ack.CommitTs, uint64(10))
	require.Equal(t, resp.DispatcherStatuses[1].Action.CommitTs, uint64(10))
	require.Equal(t, resp.DispatcherStatuses[1].Action.Action, heartbeatpb.Action_Write)
	require.False(t, resp.DispatcherStatuses[1].Action.IsSyncPoint)
	// the checkpoint ts is updated
	msg, err := ddlSpan.NewAddDispatcherMessage("node1")
	require.Nil(t, err)
	require.Equal(t, uint64(9), msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest).Config.StartTs)
	require.NotEqual(t, uint64(0), msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest).Config.StartTs)
}

func TestHandleBlockBootstrapResponse(t *testing.T) {
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
	var dispatcherIDs []*heartbeatpb.DispatcherID
	for id := 1; id < 4; id++ {
		controller.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: int64(id)}, 2)
		stm := controller.GetTasksByTableIDs(int64(id))[0]
		dispatcherIDs = append(dispatcherIDs, stm.ID.ToPB())
		controller.replicationDB.BindSpanToNode("", "node1", stm)
		controller.replicationDB.MarkSpanReplicating(stm)
	}

	// two waiting dispatcher
	barrier := NewBarrier(controller, false)
	barrier.HandleBootstrapResponse(map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{
		"nod1": {
			ChangefeedID: cfID.ToPB(),
			Spans: []*heartbeatpb.BootstrapTableSpan{
				{
					ID: dispatcherIDs[0],
					BlockState: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   6,
						BlockTables: &heartbeatpb.InfluencedTables{
							InfluenceType: heartbeatpb.InfluenceType_Normal,
							TableIDs:      []int64{1, 2},
						},
						Stage: heartbeatpb.BlockStage_WAITING,
					},
				},
				{
					ID: dispatcherIDs[1],
					BlockState: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   6,
						BlockTables: &heartbeatpb.InfluencedTables{
							InfluenceType: heartbeatpb.InfluenceType_Normal,
							TableIDs:      []int64{1, 2},
						},
						Stage: heartbeatpb.BlockStage_WAITING,
					},
				},
			},
		},
	})
	event := barrier.blockedTs[getEventKey(6, false)]
	require.NotNil(t, event)
	require.False(t, event.selected)
	require.False(t, event.writerDispatcherAdvanced)
	require.True(t, event.allDispatcherReported())

	// one waiting dispatcher, and one writing
	barrier = NewBarrier(controller, false)
	barrier.HandleBootstrapResponse(map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{
		"nod1": {
			ChangefeedID: cfID.ToPB(),
			Spans: []*heartbeatpb.BootstrapTableSpan{
				{
					ID: dispatcherIDs[0],
					BlockState: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   6,
						BlockTables: &heartbeatpb.InfluencedTables{
							InfluenceType: heartbeatpb.InfluenceType_Normal,
							TableIDs:      []int64{1, 2},
						},
						Stage: heartbeatpb.BlockStage_WAITING,
					},
				},
				{
					ID: dispatcherIDs[1],
					BlockState: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   6,
						BlockTables: &heartbeatpb.InfluencedTables{
							InfluenceType: heartbeatpb.InfluenceType_Normal,
							TableIDs:      []int64{1, 2},
						},
						Stage: heartbeatpb.BlockStage_WRITING,
					},
				},
			},
		},
	})
	event = barrier.blockedTs[getEventKey(6, false)]
	require.NotNil(t, event)
	require.True(t, event.selected)
	require.False(t, event.writerDispatcherAdvanced)

	// two done dispatchers
	barrier = NewBarrier(controller, false)
	barrier.HandleBootstrapResponse(map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{
		"nod1": {
			ChangefeedID: cfID.ToPB(),
			Spans: []*heartbeatpb.BootstrapTableSpan{
				{
					ID: dispatcherIDs[0],
					BlockState: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   6,
						BlockTables: &heartbeatpb.InfluencedTables{
							InfluenceType: heartbeatpb.InfluenceType_Normal,
							TableIDs:      []int64{1, 2},
						},
						Stage: heartbeatpb.BlockStage_DONE,
					},
				},
				{
					ID: dispatcherIDs[1],
					BlockState: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   6,
						BlockTables: &heartbeatpb.InfluencedTables{
							InfluenceType: heartbeatpb.InfluenceType_Normal,
							TableIDs:      []int64{1, 2},
						},
						Stage: heartbeatpb.BlockStage_DONE,
					},
				},
			},
		},
	})
	event = barrier.blockedTs[getEventKey(6, false)]
	require.NotNil(t, event)
	require.True(t, event.selected)
	require.True(t, event.writerDispatcherAdvanced)

	// nil, none stage
	barrier = NewBarrier(controller, false)
	barrier.HandleBootstrapResponse(map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{
		"nod1": {
			ChangefeedID: cfID.ToPB(),
			Spans: []*heartbeatpb.BootstrapTableSpan{
				{
					ID: dispatcherIDs[0],
					BlockState: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   6,
						BlockTables: &heartbeatpb.InfluencedTables{
							InfluenceType: heartbeatpb.InfluenceType_Normal,
							TableIDs:      []int64{1, 2},
						},
						Stage: heartbeatpb.BlockStage_NONE,
					},
				},
				{
					ID: dispatcherIDs[1],
				},
			},
		},
	})
	event = barrier.blockedTs[getEventKey(6, false)]
	require.Nil(t, event)
}

func TestSyncPointBlockPerf(t *testing.T) {
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
	barrier := NewBarrier(controller, true)
	for id := 1; id < 1000; id++ {
		controller.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: int64(id)}, 1)
	}
	var dispatcherIDs []*heartbeatpb.DispatcherID
	absent := controller.replicationDB.GetAbsent(make([]*replica.SpanReplication, 0), 10000)
	for _, stm := range absent {
		controller.replicationDB.BindSpanToNode("", "node1", stm)
		controller.replicationDB.MarkSpanReplicating(stm)
		dispatcherIDs = append(dispatcherIDs, stm.ID.ToPB())
	}
	var blockStatus []*heartbeatpb.TableSpanBlockStatus
	for _, id := range dispatcherIDs {
		blockStatus = append(blockStatus, &heartbeatpb.TableSpanBlockStatus{
			ID: id,
			State: &heartbeatpb.State{
				IsBlocked: true,
				BlockTs:   10,
				BlockTables: &heartbeatpb.InfluencedTables{
					InfluenceType: heartbeatpb.InfluenceType_All,
					SchemaID:      1,
				},
				IsSyncPoint: true,
			},
		})
	}

	//f, _ := os.OpenFile("cpu.profile", os.O_CREATE|os.O_RDWR, 0644)
	//defer f.Close()
	//pprof.StartCPUProfile(f)
	//defer pprof.StopCPUProfile()
	now := time.Now()
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID:  cfID.ToPB(),
		BlockStatuses: blockStatus,
	})
	require.NotNil(t, msg)
	log.Info("duration", zap.Duration("duration", time.Since(now)))

	now = time.Now()
	var passStatus []*heartbeatpb.TableSpanBlockStatus
	for _, id := range dispatcherIDs {
		passStatus = append(passStatus, &heartbeatpb.TableSpanBlockStatus{
			ID: id,
			State: &heartbeatpb.State{
				IsBlocked:   true,
				BlockTs:     10,
				IsSyncPoint: true,
				Stage:       heartbeatpb.BlockStage_DONE,
			},
		})
	}
	barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID:  cfID.ToPB(),
		BlockStatuses: passStatus,
	})
	require.NotNil(t, msg)
	log.Info("duration", zap.Duration("duration", time.Since(now)))
}
