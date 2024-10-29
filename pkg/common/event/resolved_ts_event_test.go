// Copyright 2020 PingCAP, Inc.
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

package event

import (
	"testing"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestResolvedEvent(t *testing.T) {
	did := common.NewDispatcherID()
	e := ResolvedEvent{
		DispatcherID: did,
		ResolvedTs:   123,
	}
	data, err := e.Marshal()
	require.NoError(t, err)
	require.Len(t, data, 1+8+16)

	var e2 ResolvedEvent
	err = e2.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, e, e2)
}

func TestBatchResolvedTs(t *testing.T) {
	did := common.NewDispatcherID()
	did2 := common.NewDispatcherID()
	e := ResolvedEvent{
		DispatcherID: did,
		ResolvedTs:   123,
	}
	e2 := ResolvedEvent{
		DispatcherID: did2,
		ResolvedTs:   456,
	}

	b := BatchResolvedEvent{
		Events: []ResolvedEvent{e, e2},
	}
	data, err := b.Marshal()
	require.NoError(t, err)
	require.Len(t, data, int(e.GetSize())*2)

	var b2 BatchResolvedEvent
	err = b2.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, b, b2)
}
