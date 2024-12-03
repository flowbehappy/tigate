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

package replica

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/heartbeatpb"
	replica_mock "github.com/pingcap/ticdc/maintainer/replica/mock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestUpdateStatus(t *testing.T) {
	replicaSet := NewReplicaSet(common.NewChangeFeedIDWithName("test"), common.NewDispatcherID(), nil, 1, getTableSpanByID(4), 10)
	replicaSet.UpdateStatus(&heartbeatpb.TableSpanStatus{CheckpointTs: 9})
	require.Equal(t, uint64(10), replicaSet.status.Load().CheckpointTs)
	replicaSet.UpdateStatus(&heartbeatpb.TableSpanStatus{CheckpointTs: 11})
	require.Equal(t, uint64(11), replicaSet.status.Load().CheckpointTs)
}

func TestNewRemoveDispatcherMessage(t *testing.T) {
	replicaSet := NewReplicaSet(common.NewChangeFeedIDWithName("test"), common.NewDispatcherID(), nil, 1, getTableSpanByID(4), 10)
	msg := replicaSet.NewRemoveDispatcherMessage("node1")
	req := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
	require.Equal(t, heartbeatpb.ScheduleAction_Remove, req.ScheduleAction)
	require.Equal(t, replicaSet.ID.ToPB(), req.Config.DispatcherID)
	require.Equal(t, "node1", msg.To.String())
}

func TestSpanReplication_NewAddDispatcherMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	tsoClient := replica_mock.NewMockTSOClient(ctrl)
	replicaSet := NewReplicaSet(common.NewChangeFeedIDWithName("test"), common.NewDispatcherID(), tsoClient, 1, getTableSpanByID(4), 10)

	tsoClient.EXPECT().GetTS(gomock.Any()).Return(int64(10), int64(1), nil).Times(1)
	msg, err := replicaSet.NewAddDispatcherMessage("node1")
	require.Nil(t, err)
	require.Equal(t, "node1", msg.To.String())
	req := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
	require.Equal(t, heartbeatpb.ScheduleAction_Create, req.ScheduleAction)
	require.Equal(t, oracle.ComposeTS(10, 1), req.Config.CurrentPdTs)
	require.Equal(t, replicaSet.ID.ToPB(), req.Config.DispatcherID)
	require.Equal(t, replicaSet.schemaID, req.Config.SchemaID)

	tsoClient.EXPECT().GetTS(gomock.Any()).Return(int64(1), int64(1), errors.New("error")).AnyTimes()
	_, err = replicaSet.NewAddDispatcherMessage("node1")
	require.NotNil(t, err)
}
