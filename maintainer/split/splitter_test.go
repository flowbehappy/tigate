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
	"github.com/flowbehappy/tigate/maintainer/replica"
	"github.com/flowbehappy/tigate/utils"
	"github.com/stretchr/testify/require"
)

func TestMapFindHole(t *testing.T) {
	cases := []struct {
		spans        []*heartbeatpb.TableSpan
		rang         *heartbeatpb.TableSpan
		expectedHole []*heartbeatpb.TableSpan
	}{
		{ // 0. all found.
			spans: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")},
				{StartKey: []byte("t1_1"), EndKey: []byte("t1_2")},
				{StartKey: []byte("t1_2"), EndKey: []byte("t2_0")},
			},
			rang: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")},
		},
		{ // 1. on hole in the middle.
			spans: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")},
				{StartKey: []byte("t1_3"), EndKey: []byte("t1_4")},
				{StartKey: []byte("t1_4"), EndKey: []byte("t2_0")},
			},
			rang: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")},
			expectedHole: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_1"), EndKey: []byte("t1_3")},
			},
		},
		{ // 2. two holes in the middle.
			spans: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")},
				{StartKey: []byte("t1_2"), EndKey: []byte("t1_3")},
				{StartKey: []byte("t1_4"), EndKey: []byte("t2_0")},
			},
			rang: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")},
			expectedHole: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_1"), EndKey: []byte("t1_2")},
				{StartKey: []byte("t1_3"), EndKey: []byte("t1_4")},
			},
		},
		{ // 3. all missing.
			spans: []*heartbeatpb.TableSpan{},
			rang:  &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")},
			expectedHole: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")},
			},
		},
		{ // 4. start not found
			spans: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_4"), EndKey: []byte("t2_0")},
			},
			rang: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")},
			expectedHole: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_4")},
			},
		},
		{ // 5. end not found
			spans: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")},
			},
			rang: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")},
			expectedHole: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_1"), EndKey: []byte("t2_0")},
			},
		},
	}

	for i, cs := range cases {
		m := utils.NewBtreeMap[*heartbeatpb.TableSpan, *replica.ReplicaSet](heartbeatpb.LessTableSpan)
		for _, span := range cs.spans {
			m.ReplaceOrInsert(span, &replica.ReplicaSet{})
		}
		holes := FindHoles(m, cs.rang)
		require.Equalf(t, cs.expectedHole, holes, "case %d, %#v", i, cs)
	}
}
