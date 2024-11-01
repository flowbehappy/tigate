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

package partition

import (
	"strconv"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
)

// TsDispatcher is a partition dispatcher which dispatch events based on ts.
type TsPartitionGenerator struct{}

// NewTsDispatcher creates a TsDispatcher.
func newTsPartitionGenerator() *TsPartitionGenerator {
	return &TsPartitionGenerator{}
}

func (t *TsPartitionGenerator) GeneratePartitionIndexAndKey(
	row *commonEvent.RowChange,
	partitionNum int32,
	tableInfo *common.TableInfo,
	commitTs uint64,
) (int32, string, error) {
	return int32(commitTs % uint64(partitionNum)), strconv.FormatUint(commitTs, 10), nil
}
