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

package logcoordinator

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/pingcap/ticdc/pkg/node"

	"github.com/stretchr/testify/assert"
)

func TestGetCandidateNodes(t *testing.T) {
	// Initialize logCoordinator
	coordinator := &logCoordinator{}

	nodeID1 := node.ID("node-1")
	nodeID2 := node.ID("node-2")
	nodeID3 := node.ID("node-3")

	tableID1 := int64(100)
	tableID2 := int64(101)

	span1 := &heartbeatpb.TableSpan{
		TableID: tableID1,
	}
	span2 := &heartbeatpb.TableSpan{
		TableID: tableID2,
	}

	coordinator.eventStoreStates.m = map[node.ID]*eventStoreState{
		nodeID1: &eventStoreState{
			subscriptionStates: map[int64]subscriptionStates{
				tableID1: {
					{
						subID:        1,
						span:         span1,
						checkpointTs: 100,
						resolvedTs:   200,
					},
				},
			},
		},
		nodeID2: &eventStoreState{
			subscriptionStates: map[int64]subscriptionStates{
				tableID1: {
					{
						subID:        1,
						span:         span1,
						checkpointTs: 90,
						resolvedTs:   180,
					},
					{
						subID:        2,
						span:         span1,
						checkpointTs: 100,
						resolvedTs:   220,
					},
					{
						subID:        3,
						span:         span1,
						checkpointTs: 80,
						resolvedTs:   160,
					},
				},
				tableID2: {
					{
						subID:        4,
						span:         span2,
						checkpointTs: 90,
						resolvedTs:   190,
					},
					{
						subID:        5,
						span:         span2,
						checkpointTs: 90,
						resolvedTs:   240,
					},
				},
			},
		},
		nodeID3: &eventStoreState{
			subscriptionStates: map[int64]subscriptionStates{
				tableID2: {
					{
						subID:        1,
						span:         span2,
						checkpointTs: 100,
						resolvedTs:   290,
					},
					{
						subID:        2,
						span:         span2,
						checkpointTs: 100,
						resolvedTs:   230,
					},
				},
			},
		},
	}

	// test span1
	{
		nodes := coordinator.getCandidateNodes(nodeID1, span1, uint64(100))
		assert.Equal(t, []node.ID{nodeID2}, nodes)
	}
	{
		nodes := coordinator.getCandidateNodes(nodeID3, span1, uint64(100))
		assert.Equal(t, []node.ID{nodeID2, nodeID1}, nodes)
	}

	// test span2
	{
		nodes := coordinator.getCandidateNodes(nodeID1, span2, uint64(100))
		assert.Equal(t, []node.ID{nodeID3, nodeID2}, nodes)
	}
	{
		nodes := coordinator.getCandidateNodes(nodeID3, span2, uint64(100))
		assert.Equal(t, []node.ID{nodeID2}, nodes)
	}

	// update node1 and test
	{
		state := &logservicepb.EventStoreState{
			Subscriptions: map[int64]*logservicepb.SubscriptionStates{
				tableID1: {
					Subscriptions: []*logservicepb.SubscriptionState{
						{
							SubID:        1,
							Span:         span1,
							CheckpointTs: 100,
							ResolvedTs:   300,
						},
					},
				},
			},
		}
		coordinator.updateEventStoreState(nodeID1, state)
		nodes := coordinator.getCandidateNodes(nodeID3, span1, uint64(100))
		assert.Equal(t, []node.ID{nodeID1, nodeID2}, nodes)
	}

	// update node2 and test
	{
		state := &logservicepb.EventStoreState{
			Subscriptions: map[int64]*logservicepb.SubscriptionStates{
				tableID1: {
					Subscriptions: []*logservicepb.SubscriptionState{
						{
							SubID:        1,
							Span:         span1,
							CheckpointTs: 100,
							ResolvedTs:   230,
						},
						{
							SubID:        2,
							Span:         span1,
							CheckpointTs: 100,
							ResolvedTs:   310,
						},
					},
				},
			},
		}
		coordinator.updateEventStoreState(nodeID2, state)
		nodes := coordinator.getCandidateNodes(nodeID3, span1, uint64(100))
		assert.Equal(t, []node.ID{nodeID2, nodeID1}, nodes)
	}
}
