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
	"strings"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/sink"
)

type PartitionGenerator interface {
	// GeneratePartitionIndexAndKey returns an index of partitions or a partition key for event.
	// Concurrency Note: This method is thread-safe.
	GeneratePartitionIndexAndKey(row *common.RowChangedEvent, partitionNum int32) (int32, string, error)
}

func GetPartitionGenerator(rule string, scheme string, indexName string, columns []string) PartitionGenerator {
	switch strings.ToLower(rule) {
	case "default":
	case "table":
		return newTablePartitionGenerator()
	case "ts":
		return newTsPartitionGenerator()
	case "index-value":
		return newIndexValuePartitionGenerator(indexName)
	case "rowid":
		log.Warn("rowid is deprecated, index-value is used as the partition dispatcher.")
		return newIndexValuePartitionGenerator(indexName)
	case "columns":
		return newColumnsPartitionGenerator(columns)
	default:
	}

	if sink.IsPulsarScheme(scheme) {
		return newKeyPartitionGenerator(rule)
	}

	log.Warn("the partition dispatch rule is not default/ts/table/index-value/columns," +
		" use the default rule instead.")
	return newTablePartitionGenerator()
}
