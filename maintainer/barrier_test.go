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

//func TestBlock(t *testing.T) {
//	sche := NewScheduler("test", 1, nil, nil, nil, 1000, 0)
//	sche.AddNewNode("node1")
//	sche.AddNewNode("node2")
//	var blockedDispatcherIDS []*heartbeatpb.DispatcherID
//	for id := 0; id < 3; id++ {
//		span := spanz.TableIDToComparableSpan(int64(id))
//		tableSpan := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{
//			TableID:  uint64(id),
//			StartKey: span.StartKey,
//			EndKey:   span.EndKey,
//		}}
//		dispatcherID := common.NewDispatcherID()
//		blockedDispatcherIDS = append(blockedDispatcherIDS, dispatcherID.ToPB())
//		replicaSet := NewReplicaSet(model.DefaultChangeFeedID("test"), dispatcherID, tableSpan, 0)
//		stm, _ := scheduler.NewStateMachine(dispatcherID, nil, replicaSet)
//		sche.working[dispatcherID] = stm
//		stm.Primary = "node1"
//		sche.nodeTasks["node1"][dispatcherID] = stm
//	}
//
//	var selectDispatcherID = common.NewDispatcherIDFromPB(blockedDispatcherIDS[2])
//	sche.nodeTasks["node2"][selectDispatcherID] = sche.nodeTasks["node1"][selectDispatcherID]
//	delete(sche.nodeTasks["node1"], selectDispatcherID)
//
//	newSpan := &heartbeatpb.TableSpan{TableID: 10}
//	barrier := NewBarrier(sche)
//
//	// first node block request
//	msgs, err := barrier.HandleStatus("node1", &heartbeatpb.HeartBeatRequest{
//		ChangefeedID: "test",
//		Statuses: []*heartbeatpb.TableSpanStatus{
//			{
//				ID: blockedDispatcherIDS[0],
//				State: &heartbeatpb.State{
//					IsBlocked:                true,
//					BlockTs:                  10,
//					BlockDispatcherIDs:       blockedDispatcherIDS,
//					NeedDroppedDispatcherIDs: []*heartbeatpb.DispatcherID{selectDispatcherID.ToPB()},
//					NeedAddedTableSpan:       []*heartbeatpb.TableSpan{newSpan},
//				},
//				CheckpointTs: 9,
//			},
//			{
//				ID: blockedDispatcherIDS[1],
//				State: &heartbeatpb.State{
//					IsBlocked:                true,
//					BlockTs:                  10,
//					BlockDispatcherIDs:       blockedDispatcherIDS,
//					NeedDroppedDispatcherIDs: []*heartbeatpb.DispatcherID{selectDispatcherID.ToPB()},
//					NeedAddedTableSpan:       []*heartbeatpb.TableSpan{newSpan},
//				},
//				CheckpointTs: 9,
//			},
//		},
//	})
//	require.NoError(t, err)
//	require.Len(t, msgs, 1)
//	resp := msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
//	require.Len(t, resp.DispatcherStatuses, 2)
//
//	// other node block request
//	msgs, err = barrier.HandleStatus("node2", &heartbeatpb.HeartBeatRequest{
//		ChangefeedID: "test",
//		Statuses: []*heartbeatpb.TableSpanStatus{
//			{
//				ID: selectDispatcherID.ToPB(),
//				State: &heartbeatpb.State{
//					IsBlocked:                true,
//					BlockTs:                  10,
//					BlockDispatcherIDs:       blockedDispatcherIDS,
//					NeedDroppedDispatcherIDs: []*heartbeatpb.DispatcherID{selectDispatcherID.ToPB()},
//					NeedAddedTableSpan:       []*heartbeatpb.TableSpan{newSpan},
//				},
//				CheckpointTs: 9,
//			},
//		},
//	})
//	require.NoError(t, err)
//	require.Len(t, msgs, 1)
//	require.Equal(t, barrier.blockedDispatcher[selectDispatcherID], barrier.blockedTs[10])
//	event := barrier.blockedTs[10]
//	require.Equal(t, uint64(10), event.commitTs)
//	require.True(t, event.selectedDispatcher == selectDispatcherID)
//	require.True(t, event.blockedDispatcherMap[selectDispatcherID])
//	require.True(t, event.allDispatcherReported())
//
//	// repeated status
//	_, _ = barrier.HandleStatus("node1", &heartbeatpb.HeartBeatRequest{
//		ChangefeedID: "test",
//		Statuses: []*heartbeatpb.TableSpanStatus{
//			{
//				ID: blockedDispatcherIDS[0],
//				State: &heartbeatpb.State{
//					IsBlocked:                true,
//					BlockTs:                  10,
//					BlockDispatcherIDs:       blockedDispatcherIDS,
//					NeedDroppedDispatcherIDs: []*heartbeatpb.DispatcherID{selectDispatcherID.ToPB()},
//					NeedAddedTableSpan:       []*heartbeatpb.TableSpan{newSpan},
//				},
//				CheckpointTs: 9,
//			},
//			{
//				ID: blockedDispatcherIDS[1],
//				State: &heartbeatpb.State{
//					IsBlocked:                true,
//					BlockTs:                  10,
//					BlockDispatcherIDs:       blockedDispatcherIDS,
//					NeedDroppedDispatcherIDs: []*heartbeatpb.DispatcherID{selectDispatcherID.ToPB()},
//					NeedAddedTableSpan:       []*heartbeatpb.TableSpan{newSpan},
//				},
//				CheckpointTs: 9,
//			},
//		},
//	})
//	require.Equal(t, uint64(10), event.commitTs)
//	require.True(t, event.selectedDispatcher == selectDispatcherID)
//	require.True(t, event.blockedDispatcherMap[selectDispatcherID])
//	require.True(t, event.allDispatcherReported())
//
//	// selected node write done
//	msgs, err = barrier.HandleStatus("node2", &heartbeatpb.HeartBeatRequest{
//		ChangefeedID: "test",
//		Statuses: []*heartbeatpb.TableSpanStatus{
//			{
//				ID:           blockedDispatcherIDS[2],
//				CheckpointTs: 11,
//			},
//		},
//	})
//	require.NoError(t, err)
//	//two schedule messages and 2 pass action message
//	require.Len(t, msgs, 4)
//	require.Len(t, barrier.blockedTs, 1)
//	require.Len(t, barrier.blockedDispatcher, 2)
//	msgs, err = barrier.HandleStatus("node1", &heartbeatpb.HeartBeatRequest{
//		ChangefeedID: "test",
//		Statuses: []*heartbeatpb.TableSpanStatus{
//			{
//				ID:           blockedDispatcherIDS[0],
//				CheckpointTs: 19,
//			},
//			{
//				ID:           blockedDispatcherIDS[1],
//				CheckpointTs: 13,
//			},
//		},
//	})
//	require.Len(t, barrier.blockedTs, 0)
//	require.Len(t, barrier.blockedDispatcher, 0)
//	require.Len(t, msgs, 0)
//}
//
//func TestNonBlocked(t *testing.T) {
//	sche := NewScheduler("test", 1, nil, nil, nil, 1000, 0)
//	sche.AddNewNode("node1")
//	barrier := NewBarrier(sche)
//
//	var blockedDispatcherIDS []*heartbeatpb.DispatcherID
//	for id := 0; id < 3; id++ {
//		blockedDispatcherIDS = append(blockedDispatcherIDS, common.NewDispatcherID().ToPB())
//	}
//	msgs, err := barrier.HandleStatus("node1", &heartbeatpb.HeartBeatRequest{
//		ChangefeedID: "test",
//		Statuses: []*heartbeatpb.TableSpanStatus{
//			{
//				ID: blockedDispatcherIDS[0],
//				State: &heartbeatpb.State{
//					IsBlocked:          false,
//					BlockTs:            10,
//					BlockDispatcherIDs: blockedDispatcherIDS,
//					NeedAddedTableSpan: []*heartbeatpb.TableSpan{
//						{TableID: 1}, {TableID: 2},
//					},
//				},
//				CheckpointTs: 9,
//			},
//		},
//	})
//	require.NoError(t, err)
//	// 1 ack and two scheduling messages
//	require.Len(t, msgs, 3)
//	require.Len(t, barrier.blockedTs, 0)
//	require.Len(t, barrier.blockedDispatcher, 0)
//	require.Len(t, barrier.scheduler.committing, 2)
//}
