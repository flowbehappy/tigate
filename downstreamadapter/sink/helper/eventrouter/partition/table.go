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
	"sync"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tiflow/pkg/hash"
)

// TablePartitionGenerator is a partition generator which dispatches events based on the schema and table name.
type TablePartitionGenerator struct {
	hasher *hash.PositionInertia
	lock   sync.Mutex
}

// NewTableDispatcher creates a TableDispatcher.
func newTablePartitionGenerator() *TablePartitionGenerator {
	return &TablePartitionGenerator{
		hasher: hash.NewPositionInertia(),
	}
}

// GeneratePartitionIndexAndKey returns the target partition to which a row changed event should be dispatched.
func (t *TablePartitionGenerator) GeneratePartitionIndexAndKey(
	row *commonEvent.RowChange,
	partitionNum int32,
	tableInfo *common.TableInfo,
	commitTs uint64,
) (int32, string, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.hasher.Reset()
	// distribute partition by table
	t.hasher.Write([]byte(tableInfo.GetSchemaName()), []byte(tableInfo.GetTableName()))
	return int32(t.hasher.Sum32() % uint32(partitionNum)), tableInfo.TableName.String(), nil
}
