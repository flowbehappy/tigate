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
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/utils"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSchedule(t *testing.T) {
	s := NewScheduler(9)
	s.nodeTasks["node1"] = utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine]()
	s.nodeTasks["node2"] = utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine]()
	s.nodeTasks["node3"] = utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine]()

	for i := 0; i < 1000; i++ {
		span := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{
			TableID: uint64(i),
		}}
		stm, _ := scheduler.NewStateMachine(span, nil,
			NewReplicaSet(model.ChangeFeedID{}, span, 1))
		s.absent.ReplaceOrInsert(span, stm)
	}
	msgs, err := s.Schedule()
	require.NoError(t, err)
	require.Len(t, msgs, 9)
	require.Equal(t, s.schedulingTask.Len(), 9)
	require.Equal(t, s.absent.Len(), 991)
	require.Equal(t, s.nodeTasks["node1"].Len(), 3)
	require.Equal(t, s.nodeTasks["node2"].Len(), 3)
	require.Equal(t, s.nodeTasks["node3"].Len(), 3)
}
