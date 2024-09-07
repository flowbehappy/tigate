// Copyright 2023 PingCAP, Inc.
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
	"sync"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/hash"
	"go.uber.org/zap"
)

// ColumnsDispatcher is a partition dispatcher
// which dispatches events based on the given columns.
type ColumnsPartitionGenerator struct {
	hasher *hash.PositionInertia
	lock   sync.Mutex

	Columns []string
}

// NewColumnsDispatcher creates a ColumnsDispatcher.
func newColumnsPartitionGenerator(columns []string) *ColumnsPartitionGenerator {
	return &ColumnsPartitionGenerator{
		hasher:  hash.NewPositionInertia(),
		Columns: columns,
	}
}

func (r *ColumnsPartitionGenerator) GeneratePartitionIndexAndKey(row *common.RowDelta, partitionNum int32, tableInfo *common.TableInfo, commitTs uint64) (int32, string, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.hasher.Reset()

	r.hasher.Write([]byte(tableInfo.GetSchemaName()), []byte(tableInfo.GetTableName()))

	rowData := row.Row
	if rowData.IsEmpty() {
		rowData = row.PreRow
	}

	offsets, ok := tableInfo.OffsetsByNames(r.Columns)
	if !ok {
		log.Error("columns not found when dispatch event",
			zap.Any("tableName", tableInfo.GetTableName()),
			zap.Strings("columns", r.Columns))
		return 0, "", errors.ErrDispatcherFailed.GenWithStack(
			"columns not found when dispatch event, table: %v, columns: %v", tableInfo.GetTableName(), r.Columns)
	}

	for idx := 0; idx < len(r.Columns); idx++ {
		colInfo := tableInfo.Columns[offsets[idx]]
		value, err := common.FormatColVal(&rowData, colInfo, idx)
		if err != nil {
			// FIXME:
			log.Panic("FormatColVal failed", zap.Error(err))
		}
		if value == nil {
			continue
		}
		r.hasher.Write([]byte(r.Columns[idx]), []byte(model.ColumnValueString(value)))
	}

	sum32 := r.hasher.Sum32()
	return int32(sum32 % uint32(partitionNum)), strconv.FormatInt(int64(sum32), 10), nil
}
