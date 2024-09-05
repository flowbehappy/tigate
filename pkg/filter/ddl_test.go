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

package filter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSingleTableDDL(t *testing.T) {
	for d := range singleTableDDLs {
		_, ok := ddlWhiteListMap[d]
		require.True(t, ok, "DDL %s is not in the white list", d)
	}
	for d := range multiTableDDLs {
		_, ok := ddlWhiteListMap[d]
		require.True(t, ok, "DDL %s is not in the white list", d)
	}
	for d := range globalTableDDLs {
		_, ok := ddlWhiteListMap[d]
		require.True(t, ok, "DDL %s is in the white list", d)
	}
	require.Equal(t, len(singleTableDDLs)+len(multiTableDDLs)+len(globalTableDDLs), len(ddlWhiteListMap))
}
