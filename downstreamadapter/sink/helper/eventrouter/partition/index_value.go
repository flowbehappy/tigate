// Copyright 2022 PingCAP, Inc.
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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/hash"
	"go.uber.org/zap"
)

type IndexValuePartitionGenerator struct {
	hasher *hash.PositionInertia
	lock   sync.Mutex

	IndexName string
}

func newIndexValuePartitionGenerator(indexName string) *IndexValuePartitionGenerator {
	return &IndexValuePartitionGenerator{
		hasher:    hash.NewPositionInertia(),
		IndexName: indexName,
	}
}

func (r *IndexValuePartitionGenerator) GeneratePartitionIndexAndKey(row *commonEvent.RowChange,
	partitionNum int32,
	tableInfo *common.TableInfo,
	commitTs uint64,
) (int32, string, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.hasher.Reset()
	r.hasher.Write([]byte(tableInfo.GetSchemaName()), []byte(tableInfo.GetTableName()))

	rowData := row.Row
	if rowData.IsEmpty() {
		rowData = row.PreRow
	}

	// the most normal case, index-name is not set, use the handle key columns.
	if r.IndexName == "" {
		for idx, col := range tableInfo.Columns {
			if col == nil {
				continue
			}
			if tableInfo.GetColumnFlags()[col.ID].IsHandleKey() {
				value, err := common.FormatColVal(&rowData, col, idx)
				if err != nil {
					// FIXME:
					log.Panic("FormatColVal failed", zap.Error(err))
				}
				r.hasher.Write([]byte(col.Name.O), []byte(model.ColumnValueString(value)))
			}
		}
	} else {
		names, offsets, ok := tableInfo.IndexByName(r.IndexName)
		if !ok {
			log.Error("index not found when dispatch event",
				zap.Any("tableName", tableInfo.GetTableName()),
				zap.String("indexName", r.IndexName))
			return 0, "", errors.ErrDispatcherFailed.GenWithStack(
				"index not found when dispatch event, table: %v, index: %s", tableInfo.GetTableName(), r.IndexName)
		}
		for idx := 0; idx < len(names); idx++ {
			colInfo := tableInfo.Columns[offsets[idx]]
			value, err := common.FormatColVal(&rowData, colInfo, idx)
			if err != nil {
				// FIXME:
				log.Panic("FormatColVal failed", zap.Error(err))
			}
			if value == nil {
				continue
			}
			r.hasher.Write([]byte(names[idx]), []byte(model.ColumnValueString(value)))
		}
	}

	sum32 := r.hasher.Sum32()
	return int32(sum32 % uint32(partitionNum)), strconv.FormatInt(int64(sum32), 10), nil
}
