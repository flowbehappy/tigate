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

package common

import (
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// TEvent is a transaction event.
type TEvent struct {
	DispatcherID    DispatcherID `json:"dispatcher_id"`
	PhysicalTableID uint64       `json:"physical_table_id"`
	StartTs         uint64       `msg:"start-ts"`
	CommitTs        uint64       `msg:"commit-ts"`

	Rows    chunk.Chunk `json:"rows"`
	PreRows chunk.Chunk `json:"pre_rows"`
}

type ResolvedEvent struct {
	DispatcherID DispatcherID `json:"dispatcher_id"`
	ResolvedTs   Ts           `json:"resolved_ts"`
}

type DDLEvent struct {
	DispatcherID DispatcherID `json:"dispatcher_id"`
	// commitTS of the rawKV
	CommitTS Ts         `json:"commit_ts"`
	Job      *model.Job `json:"ddl_job"`
}
