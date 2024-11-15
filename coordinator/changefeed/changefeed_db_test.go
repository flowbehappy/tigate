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

package changefeed

import (
	"math"
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestAddAbsentChangefeed(t *testing.T) {
	db := NewChangefeedDB()
	cf := &Changefeed{ID: common.NewChangeFeedIDWithName("test")}

	db.AddAbsentChangefeed(cf)

	require.Contains(t, db.absent, cf.ID)
	require.Contains(t, db.changefeeds, cf.ID)
}

func TestAddStoppedChangefeed(t *testing.T) {
	db := NewChangefeedDB()
	cf := &Changefeed{ID: common.NewChangeFeedIDWithName("test")}

	db.AddStoppedChangefeed(cf)

	require.Contains(t, db.stopped, cf.ID)
	require.Contains(t, db.changefeeds, cf.ID)
}

func TestAddReplicatingMaintainer(t *testing.T) {
	db := NewChangefeedDB()
	cf := &Changefeed{ID: common.NewChangeFeedIDWithName("test")}
	nodeID := node.ID("node-1")

	db.AddReplicatingMaintainer(cf, nodeID)

	require.Contains(t, db.replicating, cf.ID)
	require.Contains(t, db.changefeeds, cf.ID)
	require.Equal(t, nodeID, cf.GetNodeID())
}

func TestStopByChangefeedID(t *testing.T) {
	db := NewChangefeedDB()
	cf := &Changefeed{ID: common.NewChangeFeedIDWithName("test")}
	db.AddReplicatingMaintainer(cf, node.ID("node-1"))

	nodeID := db.StopByChangefeedID(cf.ID, false)

	require.Contains(t, db.stopped, cf.ID)
	require.Contains(t, db.changefeeds, cf.ID)
	require.Equal(t, node.ID("node-1"), nodeID)

	require.Equal(t, "", db.StopByChangefeedID(common.NewChangeFeedIDWithName("a"), false).String())
}

func TestResume(t *testing.T) {
	db := NewChangefeedDB()
	cf := &Changefeed{ID: common.NewChangeFeedIDWithName("test")}
	db.AddStoppedChangefeed(cf)
	cf.backoff = NewBackoff(cf.ID, 0, 0)

	db.Resume(cf.ID, true)

	require.Contains(t, db.absent, cf.ID)
	require.NotContains(t, db.stopped, cf.ID)
}

func TestRemoveChangefeed(t *testing.T) {
	db := NewChangefeedDB()
	cf := &Changefeed{ID: common.NewChangeFeedIDWithName("test")}
	db.AddAbsentChangefeed(cf)

	db.StopByChangefeedID(cf.ID, false)
	require.NotContains(t, db.absent, cf.ID)
	require.Contains(t, db.changefeeds, cf.ID)

	cf2 := &Changefeed{ID: common.NewChangeFeedIDWithName("test2")}
	db.AddReplicatingMaintainer(cf2, "node1")
	require.Equal(t, node.ID("node1"), db.StopByChangefeedID(cf2.ID, true))
	require.NotContains(t, db.absent, cf2.ID)
	require.NotContains(t, db.changefeeds, cf2.ID)
	require.Equal(t, "", cf2.nodeID.String())
}

func TestGetByID(t *testing.T) {
	db := NewChangefeedDB()
	cf := &Changefeed{ID: common.NewChangeFeedIDWithName("test")}
	db.AddStoppedChangefeed(cf)

	result := db.GetByID(cf.ID)
	require.Equal(t, cf, result)
}

func TestChangefeedDBGetAllChangefeeds(t *testing.T) {
	db := NewChangefeedDB()
	cf1 := &Changefeed{ID: common.NewChangeFeedIDWithName("test1")}
	cf2 := &Changefeed{ID: common.NewChangeFeedIDWithName("test2")}
	db.AddAbsentChangefeed(cf1)
	db.AddAbsentChangefeed(cf2)

	result := db.GetAllChangefeeds()

	require.Contains(t, result, cf1)
	require.Contains(t, result, cf2)
}

func TestGetWaitingSchedulingChangefeeds(t *testing.T) {
	db := NewChangefeedDB()
	cf1 := &Changefeed{ID: common.NewChangeFeedIDWithName("test1")}
	cf2 := &Changefeed{ID: common.NewChangeFeedIDWithName("test2")}
	cf1.backoff = NewBackoff(cf1.ID, 0, 0)
	cf1.backoff.failed.Store(true)
	cf2.backoff = NewBackoff(cf2.ID, 0, 0)
	db.AddAbsentChangefeed(cf1)
	db.AddReplicatingMaintainer(cf2, "node1")
	cf3 := &Changefeed{ID: common.NewChangeFeedIDWithName("test3")}
	cf3.backoff = NewBackoff(cf3.ID, 0, 0)
	db.AddAbsentChangefeed(cf3)

	result, nMap := db.GetWaitingSchedulingChangefeeds(nil, 3)
	require.Equal(t, 1, nMap["node1"])
	require.NotContains(t, result, cf1)
	require.NotContains(t, result, cf2)
	require.Contains(t, result, cf3)

	result, nMap = db.GetWaitingSchedulingChangefeeds(nil, 1)
	require.Equal(t, 1, nMap["node1"])
	require.Len(t, result, 1)
}

func TestGetAllStoppedChangefeeds(t *testing.T) {
	db := NewChangefeedDB()
	cf1 := &Changefeed{ID: common.NewChangeFeedIDWithName("test1")}
	cf2 := &Changefeed{ID: common.NewChangeFeedIDWithName("test2")}
	db.AddStoppedChangefeed(cf1)
	db.AddStoppedChangefeed(cf2)

	require.Equal(t, 2, db.GetStoppedSize())
}

func TestGetAllReplicatingMaintainers(t *testing.T) {
	db := NewChangefeedDB()
	cf1 := &Changefeed{ID: common.NewChangeFeedIDWithName("test1")}
	cf2 := &Changefeed{ID: common.NewChangeFeedIDWithName("test2")}
	nodeID1 := node.ID("node-1")
	nodeID2 := node.ID("node-2")
	db.AddReplicatingMaintainer(cf1, nodeID1)
	db.AddReplicatingMaintainer(cf2, nodeID2)

	result := db.GetReplicating()
	require.Contains(t, result, cf1)
	require.Contains(t, result, cf2)
}

func TestGetSize(t *testing.T) {
	db := NewChangefeedDB()
	cf1 := &Changefeed{ID: common.NewChangeFeedIDWithName("test1")}
	cf2 := &Changefeed{ID: common.NewChangeFeedIDWithName("test2")}
	db.AddReplicatingMaintainer(cf1, "node-1")
	db.AddAbsentChangefeed(cf2)
	db.AddStoppedChangefeed(&Changefeed{ID: common.NewChangeFeedIDWithName("test2")})
	require.Equal(t, 1, db.GetReplicatingSize())
	require.Equal(t, 1, db.GetStoppedSize())
	require.Equal(t, 1, db.GetAbsentSize())
	require.Equal(t, 3, db.GetSize())
	db.BindChangefeedToNode("", "node-1", cf2)
	require.Equal(t, 0, db.GetAbsentSize())
	require.Equal(t, 1, len(db.scheduling))
	require.Contains(t, db.GetByNodeID("node-1"), cf1)
	require.Contains(t, db.GetByNodeID("node-1"), cf2)

	db.BindChangefeedToNode("node-1", "node-2", cf2)
	require.NotContains(t, db.GetByNodeID("node-1"), cf2)
	require.Contains(t, db.GetByNodeID("node-2"), cf2)

	db.BindChangefeedToNode("node-2", "node-3", cf2)
	require.NotContains(t, db.GetByNodeID("node-2"), cf2)
	require.Contains(t, db.GetByNodeID("node-3"), cf2)

	sizeMap := db.GetTaskSizePerNode()
	require.Equal(t, 1, sizeMap["node-1"])
	require.Equal(t, 1, sizeMap["node-3"])
	_, ok := sizeMap["node-2"]
	require.False(t, ok)
}

func TestReplaceStoppedChangefeed(t *testing.T) {
	db := NewChangefeedDB()
	cfID := common.NewChangeFeedIDWithName("test")
	cf := &Changefeed{
		ID: cfID,
		info: atomic.NewPointer(&config.ChangeFeedInfo{ChangefeedID: cfID,
			Config:  config.GetDefaultReplicaConfig(),
			SinkURI: "mysql://127.0.0.1:3306"}),
		status: atomic.NewPointer(&heartbeatpb.MaintainerStatus{}),
	}
	db.AddStoppedChangefeed(cf)
	require.Contains(t, db.stopped, cf.ID)

	cf2 := &config.ChangeFeedInfo{ChangefeedID: cfID,
		Config:  config.GetDefaultReplicaConfig(),
		SinkURI: "kafka://127.0.0.1:9092"}
	db.ReplaceStoppedChangefeed(cf2)
	require.Contains(t, db.stopped, cf.ID)

	cf3 := db.GetByID(cf.ID)
	require.Equal(t, true, cf3.isMQSink)

	cf4ID := common.NewChangeFeedIDWithName("test4")
	cf4 := &config.ChangeFeedInfo{ChangefeedID: cf4ID,
		SinkURI: "kafka://127.0.0.1:9092"}
	db.ReplaceStoppedChangefeed(cf4)

	require.NotContains(t, db.changefeeds, cf4ID)
}

func TestScheduleChangefeed(t *testing.T) {
	db := NewChangefeedDB()
	cfID := common.NewChangeFeedIDWithName("test")
	cf := NewChangefeed(cfID, &config.ChangeFeedInfo{ChangefeedID: cfID,
		Config:  config.GetDefaultReplicaConfig(),
		SinkURI: "mysql://127.0.0.1:3306"},
		10)
	db.AddAbsentChangefeed(cf)
	db.MarkMaintainerScheduling(cf)
	db.BindChangefeedToNode("", "node-1", cf)
	cf.backoff.isRestarting.Store(true)
	db.MarkMaintainerReplicating(cf)
	require.Contains(t, db.replicating, cf.ID)
	require.False(t, cf.backoff.isRestarting.Load())

	cf2 := db.GetByChangefeedDisplayName(cf.ID.DisplayName)
	require.Equal(t, cf, cf2)

	db.MarkMaintainerAbsent(cf2)
	require.Contains(t, db.absent, cf.ID)
	require.NotContains(t, db.replicating, cf.ID)
	require.NotContains(t, db.scheduling, cf.ID)
	require.Equal(t, "", cf.nodeID.String())
	require.NotContains(t, db.GetByNodeID("node-1"), cf)
}

func TestCalculateGCSafepoint(t *testing.T) {
	db := NewChangefeedDB()
	require.True(t, math.MaxUint64 == db.CalculateGCSafepoint())

	cfID := common.NewChangeFeedIDWithName("test")
	cf1 := NewChangefeed(cfID,
		&config.ChangeFeedInfo{ChangefeedID: cfID,
			Config: config.GetDefaultReplicaConfig(),
			State:  model.StateStopped,
		}, 11)
	db.AddStoppedChangefeed(cf1)
	require.Equal(t, uint64(11), db.CalculateGCSafepoint())

	cf2ID := common.NewChangeFeedIDWithName("test")
	cf2 := NewChangefeed(cf2ID,
		&config.ChangeFeedInfo{ChangefeedID: cf2ID,
			Config: config.GetDefaultReplicaConfig(),
			State:  model.StateFinished,
		}, 9)
	db.AddStoppedChangefeed(cf2)
	require.Equal(t, uint64(11), db.CalculateGCSafepoint())

	cf3ID := common.NewChangeFeedIDWithName("test")
	cf3 := NewChangefeed(cf3ID,
		&config.ChangeFeedInfo{ChangefeedID: cf3ID,
			Config: config.GetDefaultReplicaConfig(),
			State:  model.StateNormal,
		}, 10)
	db.AddStoppedChangefeed(cf3)
	require.Equal(t, uint64(10), db.CalculateGCSafepoint())

	cf4ID := common.NewChangeFeedIDWithName("test")
	cf4 := NewChangefeed(cf4ID,
		&config.ChangeFeedInfo{ChangefeedID: cf4ID,
			Config: config.GetDefaultReplicaConfig(),
			State:  model.StateFailed,
			Error: &model.RunningError{
				Code: string(cerror.ErrGCTTLExceeded.ID()),
			},
		}, 7)
	db.AddStoppedChangefeed(cf4)
	require.Equal(t, uint64(10), db.CalculateGCSafepoint())

	cf5ID := common.NewChangeFeedIDWithName("test")
	cf5 := NewChangefeed(cf5ID,
		&config.ChangeFeedInfo{ChangefeedID: cf5ID,
			Config: config.GetDefaultReplicaConfig(),
			State:  model.StateFailed,
			Error:  &model.RunningError{},
		}, 7)
	db.AddStoppedChangefeed(cf5)
	require.Equal(t, uint64(7), db.CalculateGCSafepoint())
}
