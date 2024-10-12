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
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestOneBlockEvent(t *testing.T) {
	sche := NewController("test", 1, nil, nil, nil, 1000, 0)
	sche.AddNewNode("node1")
	sche.AddNewTable(commonEvent.Table{1, 1}, 0)
	stm := sche.GetTasksByTableIDs(1)[0]
	stm.Primary = "node1"
	stm.State = scheduler.SchedulerStatusWorking
	sche.tryMoveTask(stm.ID, stm, scheduler.SchedulerStatusAbsent, "", true)
	barrier := NewBarrier(sche)
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: "test",
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
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
	require.True(t, event.writerDispatcher == stm.ID)
	require.True(t, event.selected)
	require.False(t, event.writerDispatcherAdvanced)
	require.Len(t, event.reportedDispatchers, 1)
	require.Equal(t, resp.DispatcherStatuses[0].Ack.CommitTs, uint64(10))
	require.Equal(t, resp.DispatcherStatuses[0].Action.CommitTs, uint64(10))
	require.Equal(t, resp.DispatcherStatuses[0].Action.Action, heartbeatpb.Action_Write)
	require.True(t, resp.DispatcherStatuses[0].Action.IsSyncPoint)

	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: "test",
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: stm.ID.ToPB(),
				State: &heartbeatpb.State{
					BlockTs:     10,
					IsBlocked:   true,
					EventDone:   true,
					IsSyncPoint: true,
				},
			},
		},
	})
	require.Len(t, barrier.blockedTs, 0)
}

func TestNormalBlock(t *testing.T) {
	sche := NewController("test", 1, nil, nil, nil, 1000, 0)
	sche.AddNewNode("node1")
	sche.AddNewNode("node2")
	var blockedDispatcherIDS []*heartbeatpb.DispatcherID
	for id := 1; id < 4; id++ {
		sche.AddNewTable(commonEvent.Table{1, int64(id)}, 0)
		stm := sche.GetTasksByTableIDs(int64(id))[0]
		blockedDispatcherIDS = append(blockedDispatcherIDS, stm.ID.ToPB())
		stm.Primary = "node1"
		stm.State = scheduler.SchedulerStatusWorking
		sche.tryMoveTask(stm.ID, stm, scheduler.SchedulerStatusAbsent, "", true)
	}

	// the last one is the writer
	var selectDispatcherID = common.NewDispatcherIDFromPB(blockedDispatcherIDS[2])
	sche.nodeTasks["node2"][selectDispatcherID] = sche.nodeTasks["node1"][selectDispatcherID]
	dropID := sche.nodeTasks["node2"][selectDispatcherID].Inferior.(*ReplicaSet).Span.TableID
	delete(sche.nodeTasks["node1"], selectDispatcherID)

	newSpan := &heartbeatpb.Table{TableID: 10, SchemaID: 1}
	barrier := NewBarrier(sche)

	// first node block request
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: "test",
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
	require.Len(t, resp.DispatcherStatuses, 2)

	// other node block request
	msg = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: "test",
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
	require.Len(t, event.reportedDispatchers, 3)

	// repeated status
	barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: "test",
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
	require.Len(t, event.reportedDispatchers, 3)

	// selected node write done
	msg = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: "test",
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: blockedDispatcherIDS[2],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					EventDone: true,
				},
			},
		},
	})
	require.Len(t, barrier.blockedTs, 1)
	require.Len(t, event.reportedDispatchers, 2)
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: "test",
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: blockedDispatcherIDS[0],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					EventDone: true,
				},
			},
			{
				ID: blockedDispatcherIDS[1],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					EventDone: true,
				},
			},
		},
	})
	require.Len(t, barrier.blockedTs, 0)
}

func TestSchemaBlock(t *testing.T) {
	sche := NewController("test", 1, nil, nil, nil, 1000, 0)
	sche.AddNewNode("node1")
	sche.AddNewNode("node2")
	sche.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 1}, 1)
	sche.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 2}, 1)
	sche.AddNewTable(commonEvent.Table{SchemaID: 2, TableID: 3}, 1)
	var dispatcherIDs []*heartbeatpb.DispatcherID
	var dropTables = []int64{1, 2}
	for key, stm := range sche.Absent() {
		if stm.Inferior.(*ReplicaSet).SchemaID == 1 {
			dispatcherIDs = append(dispatcherIDs, key.ToPB())
		}
		stm.Primary = "node1"
		stm.State = scheduler.SchedulerStatusWorking
		sche.tryMoveTask(key, stm, scheduler.SchedulerStatusAbsent, "", true)
	}

	newSpan := &heartbeatpb.Table{TableID: 10, SchemaID: 2}
	barrier := NewBarrier(sche)

	// first dispatcher  block request
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: "test",
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
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
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
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
		ChangefeedID: "test",
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
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
				},
			},
		},
	})
	require.NotNil(t, msg)
	resp = msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)
	require.True(t, resp.DispatcherStatuses[0].Action.CommitTs == 10)
	require.True(t, resp.DispatcherStatuses[0].Action.Action == heartbeatpb.Action_Write)
	key := eventKey{blockTs: 10}
	event := barrier.blockedTs[key]
	require.Equal(t, uint64(10), event.commitTs)
	//the last one will be the writer
	require.Equal(t, event.writerDispatcher.ToPB(), dispatcherIDs[1])

	// repeated status
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: "test",
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
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
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
	//the last one will be the writer
	require.Equal(t, event.writerDispatcher.ToPB(), dispatcherIDs[1])

	// selected node write done
	msg = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: "test",
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcherIDs[1],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					EventDone: true,
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
	// the writer already advanced
	require.Len(t, event.reportedDispatchers, 1)
	require.Equal(t, 1, len(sche.Absent()))
	require.Equal(t, 0, len(sche.Commiting()))
	require.Equal(t, 2, len(sche.Removing()))
	require.Equal(t, 1, len(sche.Working()))
	// other dispatcher advanced checkpoint ts
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: "test",
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcherIDs[0],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					EventDone: true,
				},
			},
		},
	})
	require.Len(t, barrier.blockedTs, 0)
}

func TestSyncPointBlock(t *testing.T) {
	sche := NewController("test", 1, nil, nil, nil, 1000, 0)
	sche.AddNewNode("node1")
	sche.AddNewNode("node2")
	sche.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 1}, 1)
	sche.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 2}, 1)
	sche.AddNewTable(commonEvent.Table{SchemaID: 2, TableID: 3}, 1)
	var dispatcherIDs []*heartbeatpb.DispatcherID
	var dropTables = []int64{1, 2, 3}
	for key, stm := range sche.Absent() {
		dispatcherIDs = append(dispatcherIDs, key.ToPB())
		stm.Primary = "node1"
		stm.State = scheduler.SchedulerStatusWorking
		sche.tryMoveTask(key, stm, scheduler.SchedulerStatusAbsent, "", true)
	}
	var selectDispatcherID = common.NewDispatcherIDFromPB(dispatcherIDs[2])
	sche.nodeTasks["node2"][selectDispatcherID] = sche.nodeTasks["node1"][selectDispatcherID]
	delete(sche.nodeTasks["node1"], selectDispatcherID)

	newSpan := &heartbeatpb.Table{TableID: 10, SchemaID: 2}
	barrier := NewBarrier(sche)
	// first dispatcher  block request
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: "test",
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
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
	// 2 ack message2
	resp := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 2)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)

	// second dispatcher  block request
	msg = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: "test",
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
	require.Len(t, resp.DispatcherStatuses, 1)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)
	require.True(t, resp.DispatcherStatuses[0].Action.CommitTs == 10)
	require.True(t, resp.DispatcherStatuses[0].Action.Action == heartbeatpb.Action_Write)
	key := eventKey{blockTs: 10, isSyncPoint: true}
	event := barrier.blockedTs[key]
	require.Equal(t, uint64(10), event.commitTs)
	//the last one will be the writer
	require.Equal(t, event.writerDispatcher.ToPB(), dispatcherIDs[2])

	// selected node write done
	_ = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: "test",
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcherIDs[2],
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     10,
					EventDone:   true,
					IsSyncPoint: true,
				},
			},
		},
	})
	msgs := barrier.Resend()
	// 2 pass action messages to one node
	require.Len(t, msgs, 2)
	require.Len(t, barrier.blockedTs, 1)
	// the writer already advanced
	require.Len(t, event.reportedDispatchers, 2)
	// other dispatcher advanced checkpoint ts
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: "test",
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcherIDs[0],
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     10,
					EventDone:   true,
					IsSyncPoint: true,
				},
			},
			{
				ID: dispatcherIDs[1],
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     10,
					EventDone:   true,
					IsSyncPoint: true,
				},
			},
		},
	})
	require.Len(t, barrier.blockedTs, 0)
}

func TestNonBlocked(t *testing.T) {
	sche := NewController("test", 1, nil, nil, nil, 1000, 0)
	sche.AddNewNode("node1")
	barrier := NewBarrier(sche)

	var blockedDispatcherIDS []*heartbeatpb.DispatcherID
	for id := 1; id < 4; id++ {
		blockedDispatcherIDS = append(blockedDispatcherIDS, common.NewDispatcherID().ToPB())
	}
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: "test",
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
	require.Len(t, barrier.controller.Absent(), 2)
}

func TestSyncPointBlockPerf(t *testing.T) {
	sche := NewController("test", 1, nil, nil, nil, 1000, 0)
	sche.AddNewNode("node1")
	barrier := NewBarrier(sche)
	for id := 1; id < 1000; id++ {
		sche.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: int64(id)}, 1)
	}
	var dispatcherIDs []*heartbeatpb.DispatcherID
	for key, stm := range sche.Absent() {
		stm.Primary = "node1"
		stm.State = scheduler.SchedulerStatusWorking
		sche.tryMoveTask(key, stm, scheduler.SchedulerStatusAbsent, "", true)
		dispatcherIDs = append(dispatcherIDs, key.ToPB())
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
		ChangefeedID:  "test",
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
				EventDone:   true,
			},
		})
	}
	barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID:  "test",
		BlockStatuses: passStatus,
	})
	require.NotNil(t, msg)
	log.Info("duration", zap.Duration("duration", time.Since(now)))
}
