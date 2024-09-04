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

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
)

func TestNormalBlock(t *testing.T) {
	sche := NewScheduler("test", 1, nil, nil, nil, 1000, 0)
	sche.AddNewNode("node1")
	sche.AddNewNode("node2")
	var blockedDispatcherIDS []*heartbeatpb.DispatcherID
	for id := 1; id < 4; id++ {
		span := spanz.TableIDToComparableSpan(int64(id))
		tableSpan := &heartbeatpb.TableSpan{
			TableID:  uint64(id),
			StartKey: span.StartKey,
			EndKey:   span.EndKey,
		}
		dispatcherID := common.NewDispatcherID()
		blockedDispatcherIDS = append(blockedDispatcherIDS, dispatcherID.ToPB())
		replicaSet := NewReplicaSet(model.DefaultChangeFeedID("test"), dispatcherID, 1, tableSpan, 0)
		stm := scheduler.NewStateMachine(dispatcherID, nil, replicaSet)
		stm.State = scheduler.SchedulerStatusWorking
		sche.Working()[dispatcherID] = stm
		stm.Primary = "node1"
		sche.nodeTasks["node1"][dispatcherID] = stm
	}

	var selectDispatcherID = common.NewDispatcherIDFromPB(blockedDispatcherIDS[2])
	sche.nodeTasks["node2"][selectDispatcherID] = sche.nodeTasks["node1"][selectDispatcherID]
	dropID := int64(sche.nodeTasks["node2"][selectDispatcherID].Inferior.(*ReplicaSet).Span.TableID)
	delete(sche.nodeTasks["node1"], selectDispatcherID)

	newSpan := &heartbeatpb.Table{TableID: 10, SchemaID: 1}
	barrier := NewBarrier(sche)

	// first node block request
	msgs, err := barrier.HandleStatus("node1", &heartbeatpb.HeartBeatRequest{
		ChangefeedID: "test",
		Statuses: []*heartbeatpb.TableSpanStatus{
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
				CheckpointTs: 9,
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
				CheckpointTs: 9,
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	resp := msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 2)

	// other node block request
	msgs, err = barrier.HandleStatus("node2", &heartbeatpb.HeartBeatRequest{
		ChangefeedID: "test",
		Statuses: []*heartbeatpb.TableSpanStatus{
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
				CheckpointTs: 9,
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, barrier.blockedDispatcher[selectDispatcherID], barrier.blockedTs[10])
	event := barrier.blockedTs[10]
	require.Equal(t, uint64(10), event.commitTs)
	require.True(t, event.writerDispatcher == selectDispatcherID)
	require.True(t, event.advancedDispatchers[selectDispatcherID])
	require.True(t, event.allDispatcherReported())

	// repeated status
	_, _ = barrier.HandleStatus("node1", &heartbeatpb.HeartBeatRequest{
		ChangefeedID: "test",
		Statuses: []*heartbeatpb.TableSpanStatus{
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
				CheckpointTs: 9,
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
				CheckpointTs: 9,
			},
		},
	})
	require.Equal(t, uint64(10), event.commitTs)
	require.True(t, event.writerDispatcher == selectDispatcherID)
	require.True(t, event.advancedDispatchers[selectDispatcherID])
	require.True(t, event.allDispatcherReported())

	// selected node write done
	msgs, err = barrier.HandleStatus("node2", &heartbeatpb.HeartBeatRequest{
		ChangefeedID: "test",
		Statuses: []*heartbeatpb.TableSpanStatus{
			{
				ID:           blockedDispatcherIDS[2],
				CheckpointTs: 11,
			},
		},
	})
	require.NoError(t, err)
	//two schedule messages and 1 pass action message to one node
	require.Len(t, msgs, 3)
	require.Len(t, barrier.blockedTs, 1)
	require.Len(t, barrier.blockedDispatcher, 2)
	msgs, err = barrier.HandleStatus("node1", &heartbeatpb.HeartBeatRequest{
		ChangefeedID: "test",
		Statuses: []*heartbeatpb.TableSpanStatus{
			{
				ID:           blockedDispatcherIDS[0],
				CheckpointTs: 19,
			},
			{
				ID:           blockedDispatcherIDS[1],
				CheckpointTs: 13,
			},
		},
	})
	require.Len(t, barrier.blockedTs, 0)
	require.Len(t, barrier.blockedDispatcher, 0)
	require.Len(t, msgs, 0)
}

func TestSchemaBlock(t *testing.T) {
	sche := NewScheduler("test", 1, nil, nil, nil, 1000, 0)
	sche.AddNewNode("node1")
	sche.AddNewNode("node2")
	sche.AddNewTable(common.Table{SchemaID: 1, TableID: 1})
	sche.AddNewTable(common.Table{SchemaID: 1, TableID: 2})
	sche.AddNewTable(common.Table{SchemaID: 2, TableID: 3})
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
	msgs, err := barrier.HandleStatus("node1", &heartbeatpb.HeartBeatRequest{
		ChangefeedID: "test",
		Statuses: []*heartbeatpb.TableSpanStatus{
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
				CheckpointTs: 9,
			},
		},
	})
	require.NoError(t, err)
	// one ack message
	require.Len(t, msgs, 1)
	resp := msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)

	// second dispatcher  block request
	msgs, err = barrier.HandleStatus("node1", &heartbeatpb.HeartBeatRequest{
		ChangefeedID: "test",
		Statuses: []*heartbeatpb.TableSpanStatus{
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
				CheckpointTs: 9,
			},
		},
	})
	require.NoError(t, err)
	// ack and write message
	require.Len(t, msgs, 1)
	resp = msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)
	require.True(t, resp.DispatcherStatuses[0].Action.CommitTs == 10)
	require.True(t, resp.DispatcherStatuses[0].Action.Action == heartbeatpb.Action_Write)
	event := barrier.blockedTs[10]
	require.Equal(t, uint64(10), event.commitTs)
	//the last one will be the writer
	require.Equal(t, event.writerDispatcher.ToPB(), dispatcherIDs[1])
	require.True(t, event.allDispatcherReported())

	// repeated status
	msgs, err = barrier.HandleStatus("node1", &heartbeatpb.HeartBeatRequest{
		ChangefeedID: "test",
		Statuses: []*heartbeatpb.TableSpanStatus{
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
				CheckpointTs: 9,
			},
		},
	})
	require.NoError(t, err)
	// ack and write message
	require.Len(t, msgs, 1)
	resp = msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)
	event = barrier.blockedTs[10]
	require.Equal(t, uint64(10), event.commitTs)
	//the last one will be the writer
	require.Equal(t, event.writerDispatcher.ToPB(), dispatcherIDs[1])
	require.True(t, event.allDispatcherReported())

	// selected node write done
	msgs, err = barrier.HandleStatus("node2", &heartbeatpb.HeartBeatRequest{
		ChangefeedID: "test",
		Statuses: []*heartbeatpb.TableSpanStatus{
			{
				ID:           dispatcherIDs[1],
				CheckpointTs: 11,
			},
		},
	})
	require.NoError(t, err)
	//3 schedule messages and 1 pass action message to one node
	require.Len(t, msgs, 4)
	for _, msg := range msgs {
		if msg.Type == messaging.TypeHeartBeatResponse {
			require.Equal(t, msg.Message[0].(*heartbeatpb.HeartBeatResponse).DispatcherStatuses[0].Action.Action,
				heartbeatpb.Action_Pass)
		}
	}
	require.Len(t, barrier.blockedTs, 1)
	// the writer already advanced
	require.Len(t, barrier.blockedDispatcher, 1)
	require.Equal(t, 0, len(sche.Absent()))
	require.Equal(t, 1, len(sche.Commiting()))
	require.Equal(t, 2, len(sche.Removing()))
	require.Equal(t, 1, len(sche.Working()))
	// other dispatcher advanced checkpoint ts
	msgs, err = barrier.HandleStatus("node1", &heartbeatpb.HeartBeatRequest{
		ChangefeedID: "test",
		Statuses: []*heartbeatpb.TableSpanStatus{
			{
				ID:           dispatcherIDs[0],
				CheckpointTs: 19,
			},
		},
	})
	require.Len(t, barrier.blockedTs, 0)
	require.Len(t, barrier.blockedDispatcher, 0)
	require.Len(t, msgs, 0)
}

func TestSyncPointBlock(t *testing.T) {
	sche := NewScheduler("test", 1, nil, nil, nil, 1000, 0)
	sche.AddNewNode("node1")
	sche.AddNewNode("node2")
	sche.AddNewTable(common.Table{SchemaID: 1, TableID: 1})
	sche.AddNewTable(common.Table{SchemaID: 1, TableID: 2})
	sche.AddNewTable(common.Table{SchemaID: 2, TableID: 3})
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
	msgs, err := barrier.HandleStatus("node1", &heartbeatpb.HeartBeatRequest{
		ChangefeedID: "test",
		Statuses: []*heartbeatpb.TableSpanStatus{
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
				},
				CheckpointTs: 9,
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
				},
				CheckpointTs: 9,
			},
		},
	})
	require.NoError(t, err)
	// 2 ack message2
	require.Len(t, msgs, 1)
	resp := msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 2)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)

	// second dispatcher  block request
	msgs, err = barrier.HandleStatus("node2", &heartbeatpb.HeartBeatRequest{
		ChangefeedID: "test",
		Statuses: []*heartbeatpb.TableSpanStatus{
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
				},
				CheckpointTs: 9,
			},
		},
	})
	require.NoError(t, err)
	// ack and write message
	require.Len(t, msgs, 1)
	resp = msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)
	require.True(t, resp.DispatcherStatuses[0].Action.CommitTs == 10)
	require.True(t, resp.DispatcherStatuses[0].Action.Action == heartbeatpb.Action_Write)
	event := barrier.blockedTs[10]
	require.Equal(t, uint64(10), event.commitTs)
	//the last one will be the writer
	require.Equal(t, event.writerDispatcher.ToPB(), dispatcherIDs[2])
	require.True(t, event.allDispatcherReported())

	// selected node write done
	msgs, err = barrier.HandleStatus("node2", &heartbeatpb.HeartBeatRequest{
		ChangefeedID: "test",
		Statuses: []*heartbeatpb.TableSpanStatus{
			{
				ID:           dispatcherIDs[2],
				CheckpointTs: 11,
			},
		},
	})
	require.NoError(t, err)
	//4 schedule messages and 2 pass action message to one node
	require.Len(t, msgs, 6)
	require.Len(t, barrier.blockedTs, 1)
	// the writer already advanced
	require.Len(t, barrier.blockedDispatcher, 2)
	// other dispatcher advanced checkpoint ts
	msgs, err = barrier.HandleStatus("node1", &heartbeatpb.HeartBeatRequest{
		ChangefeedID: "test",
		Statuses: []*heartbeatpb.TableSpanStatus{
			{
				ID:           dispatcherIDs[0],
				CheckpointTs: 19,
			},
			{
				ID:           dispatcherIDs[1],
				CheckpointTs: 19,
			},
		},
	})
	require.Len(t, barrier.blockedTs, 0)
	require.Len(t, barrier.blockedDispatcher, 0)
	require.Len(t, msgs, 0)
}

func TestNonBlocked(t *testing.T) {
	sche := NewScheduler("test", 1, nil, nil, nil, 1000, 0)
	sche.AddNewNode("node1")
	barrier := NewBarrier(sche)

	var blockedDispatcherIDS []*heartbeatpb.DispatcherID
	for id := 1; id < 4; id++ {
		blockedDispatcherIDS = append(blockedDispatcherIDS, common.NewDispatcherID().ToPB())
	}
	msgs, err := barrier.HandleStatus("node1", &heartbeatpb.HeartBeatRequest{
		ChangefeedID: "test",
		Statuses: []*heartbeatpb.TableSpanStatus{
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
				CheckpointTs: 9,
			},
		},
	})
	require.NoError(t, err)
	// 1 ack and two scheduling messages
	require.Len(t, msgs, 3)
	require.Len(t, barrier.blockedTs, 0)
	require.Len(t, barrier.blockedDispatcher, 0)
	require.Len(t, barrier.scheduler.Commiting(), 2)
}
