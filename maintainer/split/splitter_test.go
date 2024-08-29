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

package split

import (
	"testing"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/utils"
	"github.com/stretchr/testify/require"
)

func TestMapFindHole(t *testing.T) {
	cases := []struct {
		spans        []*common.TableSpan
		rang         *common.TableSpan
		expectedHole []*common.TableSpan
	}{
		{ // 0. all found.
			spans: []*common.TableSpan{
				{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")}},
				{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_1"), EndKey: []byte("t1_2")}},
				{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_2"), EndKey: []byte("t2_0")}},
			},
			rang: &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")}},
		},
		{ // 1. on hole in the middle.
			spans: []*common.TableSpan{
				{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")}},
				{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_3"), EndKey: []byte("t1_4")}},
				{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_4"), EndKey: []byte("t2_0")}},
			},
			rang: &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")}},
			expectedHole: []*common.TableSpan{
				{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_1"), EndKey: []byte("t1_3")}},
			},
		},
		{ // 2. two holes in the middle.
			spans: []*common.TableSpan{
				{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")}},
				{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_2"), EndKey: []byte("t1_3")}},
				{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_4"), EndKey: []byte("t2_0")}},
			},
			rang: &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")}},
			expectedHole: []*common.TableSpan{
				{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_1"), EndKey: []byte("t1_2")}},
				{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_3"), EndKey: []byte("t1_4")}},
			},
		},
		{ // 3. all missing.
			spans: []*common.TableSpan{},
			rang:  &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")}},
			expectedHole: []*common.TableSpan{
				{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")}},
			},
		},
		{ // 4. start not found
			spans: []*common.TableSpan{
				{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_4"), EndKey: []byte("t2_0")}},
			},
			rang: &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")}},
			expectedHole: []*common.TableSpan{
				{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t1_4")}},
			},
		},
		{ // 5. end not found
			spans: []*common.TableSpan{
				{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")}},
			},
			rang: &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")}},
			expectedHole: []*common.TableSpan{
				{TableSpan: &heartbeatpb.TableSpan{StartKey: []byte("t1_1"), EndKey: []byte("t2_0")}},
			},
		},
	}

	for i, cs := range cases {
		m := utils.NewBtreeMap[*common.TableSpan, *scheduler.StateMachine]()
		for _, span := range cs.spans {
			m.ReplaceOrInsert(span, &scheduler.StateMachine{})
		}
		holes := FindHoles(m, cs.rang)
		require.Equalf(t, cs.expectedHole, holes, "case %d, %#v", i, cs)
	}
}
