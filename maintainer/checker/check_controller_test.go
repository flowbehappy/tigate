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

package checker

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestControllerExecute(t *testing.T) {
	ctl := NewController(common.NewChangeFeedIDWithName("test"), nil, nil, nil, nil)
	require.Equal(t, 2, len(ctl.checkers))
	ctl.maxTimePerRound = time.Hour
	ctl.Execute()
	require.Equal(t, 0, ctl.checkedIndex)
	// only check the first checker
	ctl.maxTimePerRound = -1
	ctl.Execute()
	require.Equal(t, 1, ctl.checkedIndex)
	ctl.Execute()
	require.Equal(t, 0, ctl.checkedIndex)
}
